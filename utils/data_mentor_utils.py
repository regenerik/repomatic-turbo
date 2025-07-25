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
            # --- LOG DE VERIFICACIÓN 1: Antes de crear el nuevo hilo con adjunto ---
            logger.info(f"DEBUG: Creando nuevo Thread. Adjuntando file_id: {current_knowledge_file_id}")
            
            # Crear un nuevo hilo y añadir el primer mensaje con el archivo adjunto
            thread = client.beta.threads.create(
                messages=[
                    {
                        "role": "user",
                        "content": prompt,
                        "attachments": attachments # Adjuntamos el archivo aquí
                    }
                ]
            )
            current_thread_id = thread.id
            logger.info(f"Nuevo Thread creado con ID: {current_thread_id}")
        else:
            logger.info(f"thread_id vino con contenido. Continuar hilo existente: {current_thread_id}...")
            # --- LOG DE VERIFICACIÓN 2: Antes de añadir el mensaje al hilo existente con adjunto ---
            logger.info(f"DEBUG: Añadiendo mensaje a Thread existente. Adjuntando file_id: {current_knowledge_file_id}")
            
            # Añadir el mensaje al hilo existente con el archivo adjunto
            client.beta.threads.messages.create(
                thread_id=current_thread_id,
                role="user",
                content=prompt,
                attachments=attachments # Adjuntamos el archivo aquí también
            )
            logger.info(f"Mensaje y archivo adjunto al Thread: {current_thread_id}")

        logger.info(f"Creando o continuando run para el Thread: {current_thread_id} con Assistant ID: {ASSISTANT_ID}")
        
        # Crear y ejecutar el run para el asistente
        run = client.beta.threads.runs.create(
            thread_id=current_thread_id,
            assistant_id=ASSISTANT_ID,
            # Puedes añadir additional_instructions aquí si son dinámicas
            # "additional_instructions": "Responde siempre con un nuevo mensaje."
        )
        run_id = run.id
        logger.info(f"Run creado con ID: {run_id}. Estado inicial: {run.status}")

        # Polling: esperar a que el run se complete
        while run.status in ["queued", "in_progress", "cancelling"]:
            time.sleep(1) # Esperar 1 segundo antes de volver a consultar
            run = client.beta.threads.runs.retrieve(thread_id=current_thread_id, run_id=run_id)
            logger.info(f"Estado del Run: {run.status}")

        if run.status != "completed":
            logger.error(f"El run terminó con estado inesperado: '{run.status}'. Último Run ID: {run_id}")
            raise RuntimeError(f"El asistente no pudo completar la solicitud. Estado: '{run.status}'.")

        # Recuperar los mensajes del thread
        messages_page = client.beta.threads.messages.list(
            thread_id=current_thread_id,
            order="desc", # Queremos los más recientes primero
            limit="1"     # Solo necesitamos el último mensaje del asistente
        )
        
        assistant_response = ""
        # Itera sobre los mensajes para encontrar el último del asistente
        for msg in messages_page.data:
            if msg.role == "assistant":
                for content_block in msg.content:
                    if content_block.type == "text":
                        assistant_response += content_block.text.value
                break # Una vez que encontramos el último mensaje del asistente, salimos

        if not assistant_response:
            logger.warning(f"El asistente no devolvió un mensaje de texto. Thread ID: {current_thread_id}")
            assistant_response = "El asistente no pudo generar una respuesta de texto."

        return assistant_response, current_thread_id

    except Exception as e:
        logger.error(f"Error en query_assistant_mentor: {e}", exc_info=True)
        raise # Re-lanza la excepción para que la ruta la capture y retorne 500