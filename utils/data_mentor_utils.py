import os
import time
import requests
import json
from typing import Optional, Tuple
from logging_config import logger
from models import FileDailyID
from openai import OpenAI

# ——————————————————————————————————————————
#  CONFIG
# ——————————————————————————————————————————
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("Tenés que definir OPENAI_API_KEY en tus env vars")

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {OPENAI_API_KEY}",
    "OpenAI-Beta": "assistants=v2"
}

ASSISTANT_ID = os.environ.get("OPENAI_ASSISTANT_ID", "asst_Gy0OKzAqKGqXiU25q9Z89Ifs")

# Inicializa el cliente de OpenAI
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

def query_assistant_mentor(prompt: str, thread_id: Optional[str] = None) -> Tuple[str, str]:
    logger.info('Entró al util query_assistant_mentor')
    
    # 1. Obtener el ID del archivo de conocimiento diario más reciente
    daily_file_record = FileDailyID.query.first()
    if not daily_file_record:
        logger.error("No se encontró el ID del archivo de conocimiento diario en la base de datos.")
        raise RuntimeError("No se encontró la base de conocimiento diaria. Por favor, asegúrese de ejecutar la ruta de actualización de archivos.")
    
    current_knowledge_file_id = daily_file_record.current_file_id
    logger.info(f"Usando archivo de conocimiento con ID recuperado de DB: {current_knowledge_file_id}")

    # --- NUEVA VERIFICACIÓN: Estado del procesamiento del archivo ---
    try:
        file_status_check_limit = 10 # Intentar verificar el estado del archivo unas cuantas veces
        file_check_count = 0
        file_is_processed = False
        
        while file_check_count < file_status_check_limit:
            file_obj = client.files.retrieve(current_knowledge_file_id)
            logger.info(f"Estado de procesamiento del archivo {current_knowledge_file_id}: {file_obj.status}")
            
            if file_obj.status == "processed":
                file_is_processed = True
                break
            elif file_obj.status == "failed":
                logger.error(f"El archivo {current_knowledge_file_id} falló su procesamiento en OpenAI. Detalles: {file_obj.error}")
                raise RuntimeError(f"El archivo de conocimiento ({current_knowledge_file_id}) falló su procesamiento en OpenAI. Por favor, revise el archivo o contacte a soporte si persiste.")
            
            file_check_count += 1
            time.sleep(2) # Esperar un poco antes de volver a consultar el estado

        if not file_is_processed:
            logger.warning(f"El archivo {current_knowledge_file_id} aún no está 'processed' después de {file_status_check_limit} intentos. Estado actual: {file_obj.status}")
            raise RuntimeError("La base de conocimiento aún se está procesando. Por favor, inténtelo de nuevo en unos minutos.")

    except Exception as e:
        logger.error(f"Error al verificar el estado del archivo {current_knowledge_file_id}: {e}", exc_info=True)
        raise RuntimeError(f"Error al verificar la disponibilidad de la base de conocimiento: {str(e)}")


    # Configuración de los adjuntos para el mensaje
    attachments = [
        {
            "file_id": current_knowledge_file_id,
            "tools": [{"type": "file_search"}]
        }
    ]

    current_thread_id = thread_id

    try:
        if not current_thread_id:
            logger.info('thread_id vino SIN contenido (charla nueva). Creando un nuevo hilo...')
            logger.info(f"DEBUG: Creando nuevo Thread. Adjuntando file_id: {current_knowledge_file_id}")
            
            thread = client.beta.threads.create(
                messages=[
                    {
                        "role": "user",
                        "content": prompt,
                        "attachments": attachments
                    }
                ]
            )
            current_thread_id = thread.id
            logger.info(f"Nuevo Thread creado con ID: {current_thread_id}")
        else:
            logger.info(f"thread_id vino con contenido. Continuar hilo existente: {current_thread_id}...")
            logger.info(f"DEBUG: Añadiendo mensaje a Thread existente. Adjuntando file_id: {current_knowledge_file_id}")
            
            client.beta.threads.messages.create(
                thread_id=current_thread_id,
                role="user",
                content=prompt,
                attachments=attachments
            )
            logger.info(f"Mensaje y archivo adjunto al Thread: {current_thread_id}")

        logger.info(f"Creando o continuando run para el Thread: {current_thread_id} con Assistant ID: {ASSISTANT_ID}")
        
        run = client.beta.threads.runs.create(
            thread_id=current_thread_id,
            assistant_id=ASSISTANT_ID,
        )
        run_id = run.id
        logger.info(f"Run creado con ID: {run_id}. Estado inicial: {run.status}")

        while run.status in ["queued", "in_progress", "cancelling"]:
            time.sleep(1)
            run = client.beta.threads.runs.retrieve(thread_id=current_thread_id, run_id=run_id)
            logger.info(f"Estado del Run: {run.status}")

        if run.status != "completed":
            error_message = f"El asistente no pudo completar la solicitud. Estado: '{run.status}'."
            if run.last_error:
                error_message += f" Código de error: {run.last_error.code}. Mensaje: {run.last_error.message}"
                logger.error(f"Detalles del error del Run: Código={run.last_error.code}, Mensaje='{run.last_error.message}'")
            else:
                logger.error("El Run falló, pero no se encontraron detalles adicionales en 'last_error'.")
            
            raise RuntimeError(error_message)

        messages_page = client.beta.threads.messages.list(
            thread_id=current_thread_id,
            order="desc",
            limit="1"
        )
        
        assistant_response = ""
        for msg in messages_page.data:
            if msg.role == "assistant":
                for content_block in msg.content:
                    if content_block.type == "text":
                        assistant_response += content_block.text.value
                break

        if not assistant_response:
            logger.warning(f"El asistente no devolvió un mensaje de texto. Thread ID: {current_thread_id}")
            assistant_response = "El asistente no pudo generar una respuesta de texto."

        return assistant_response, current_thread_id

    except Exception as e:
        logger.error(f"Error en query_assistant_mentor: {e}", exc_info=True)
        raise