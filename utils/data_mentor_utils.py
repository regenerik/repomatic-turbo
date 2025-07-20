import os
import time
import requests
import json
from typing import Optional, Tuple
from logging_config import logger

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



def query_assistant_mentor(prompt: str, thread_id: Optional[str] = None) -> Tuple[str, str]:
    """
    Envía un prompt al asistente con ID ASSISTANT_ID utilizando la API de OpenAI.
    - Si NO hay thread_id => se crea un nuevo hilo (POST /v1/threads/runs).
    - Si SÍ hay thread_id => se continúa el hilo existente (POST /v1/threads/{thread_id}/runs).
    
    Espera a que el run se complete y devuelve (respuesta_del_asistente, thread_id).
    """
    logger.info('Entró al util query_assistant_mentor')
    full_prompt =  prompt
    if prompt:
        logger.info('Prompt vino con contenido...')
    if thread_id:
        logger.info('thread_id vino con contenido...')
        # Continuar hilo existente
        create_run_url = f"https://api.openai.com/v1/threads/{thread_id}/runs"
        payload = {
            "assistant_id": ASSISTANT_ID,
            "additional_messages": [
                {"role": "user", "content": full_prompt}
            ],
            "additional_instructions": "Responde siempre con un nuevo mensaje."
        }
    else:
        logger.info('thread_id vino SIN con contenido ( charla nueva )...')
        # Crear hilo nuevo
        create_run_url = "https://api.openai.com/v1/threads/runs"
        payload = {
            "assistant_id": ASSISTANT_ID,
            "thread": {
                "messages": [
                    {"role": "user", "content": full_prompt}
                ]
            }
        }

    logger.info('Creando o continuando charla...')
    # 1. Crear (o continuar) el run
    response = requests.post(create_run_url, headers=HEADERS, json=payload)
    response.raise_for_status()
    run_data = response.json()

    # Si es nuevo hilo, el thread_id viene de la respuesta
    # Si es hilo existente, ya lo teníamos, pero la API lo reenvía igual
    new_thread_id = run_data.get("thread_id") or thread_id
    run_id = run_data["id"]

    # 2. Polling: esperar a que el run se complete
    run_status = run_data["status"]
    while run_status not in ["completed", "failed", "cancelled"]:
        time.sleep(1)
        get_run_url = f"https://api.openai.com/v1/threads/{new_thread_id}/runs/{run_id}"
        run_response = requests.get(get_run_url, headers=HEADERS)
        run_response.raise_for_status()
        run_data = run_response.json()
        run_status = run_data["status"]

    if run_status != "completed":
        raise RuntimeError(f"El run terminó con estado '{run_status}'.")

    # 3. Recuperar los mensajes del thread
    messages_url = f"https://api.openai.com/v1/threads/{new_thread_id}/messages"
    messages_response = requests.get(messages_url, headers=HEADERS)
    messages_response.raise_for_status()
    messages_data = messages_response.json()

    # Agregar un logger o print para ver todo el thread
    import json
    # print("Mensajes del thread:", json.dumps(messages_data, indent=2))

    # 4. Filtrar los mensajes del asistente y elegir el más reciente
    assistant_messages = [
        msg for msg in messages_data.get("data", []) if msg.get("role") == "assistant"
    ]
    if assistant_messages:
        last_assistant_msg = max(assistant_messages, key=lambda m: m.get("created_at", 0))
        assistant_message = ""
        for part in last_assistant_msg.get("content", []):
            if part.get("type") == "text":
                assistant_message += part.get("text", {}).get("value", "")
    else:
        assistant_message = ""

    return assistant_message, new_thread_id






# # ——————————————————————————————————————————
# #  TUS FUNCIONES LOCALES
# # ——————————————————————————————————————————
# def obtener_horas_por_curso() -> dict:
#     logger.info("Entré a obtener_horas_por_curso()")
#     result = {
#         "Python Básico": 120,
#         "React Avanzado": 80,
#         "Flask Deploy": 24
#     }
#     logger.info(f"Resultado de obtener_horas_por_curso: {result}")
#     return result


# FUNCTION_MAP = {
#     "obtener_horas_por_curso": obtener_horas_por_curso,
# }


# # ——————————————————————————————————————————
# #  UTIL PRINCIPAL
# # ——————————————————————————————————————————
# def query_assistant_mentor(
#     prompt: str,
#     thread_id: Optional[str] = None
# ) -> Tuple[str, str]:
#     logger.info(f"query_assistant_mentor arrancó con prompt={prompt!r}, thread_id={thread_id!r}")

#     # 1) Crear o continuar el run
#     if thread_id:
#         url_run = f"https://api.openai.com/v1/threads/{thread_id}/runs"
#         payload = {
#             "assistant_id": ASSISTANT_ID,
#             "additional_messages": [{"role": "user", "content": prompt}],
#             "additional_instructions": "Responde siempre con un nuevo mensaje."
#         }
#         logger.info(f"Continuando run existente: {url_run}")
#     else:
#         url_run = "https://api.openai.com/v1/threads/runs"
#         payload = {
#             "assistant_id": ASSISTANT_ID,
#             "thread": {"messages": [{"role": "user", "content": prompt}]}
#         }
#         logger.info(f"Iniciando run nuevo: {url_run}")

#     r = requests.post(url_run, headers=HEADERS, json=payload)
#     r.raise_for_status()
#     run_data = r.json()
#     new_thread_id = run_data.get("thread_id") or thread_id
#     run_id = run_data["id"]
#     status = run_data["status"]
#     logger.info(f"Run enviado: thread_id={new_thread_id}, run_id={run_id}, estado inicial={status}")

#     # 2) Poll hasta que termine o requiera acción
#     while status not in ("completed", "failed", "cancelled", "requires_action"):
#         time.sleep(1)
#         logger.info("Polling…")
#         check = requests.get(
#             f"https://api.openai.com/v1/threads/{new_thread_id}/runs/{run_id}",
#             headers=HEADERS
#         )
#         check.raise_for_status()
#         run_data = check.json()
#         status = run_data["status"]
#         logger.info(f"Estado tras polling: {status}")

#     if status in ("failed", "cancelled"):
#         logger.error(f"Run terminó mal: {status}")
#         raise RuntimeError(f"Run terminó con estado {status}")

#     logger.info(f"Polling salió con estado: {status}")

#     # 3) Si pide función, la ejecutamos vía submit_tool_outputs
#     if status == "requires_action":
#         logger.info("El run requiere acción (function_call). Extrayendo required_action…")
#         ra = run_data.get("required_action", {}) \
#                      .get("submit_tool_outputs", {}) \
#                      .get("tool_calls", [])
#         if not ra:
#             logger.error("No encontré tool_calls en required_action")
#         else:
#             call = ra[0]
#             call_id = call["id"]
#             fn_name = call["function"]["name"]
#             fn_args = json.loads(call["function"]["arguments"] or "{}")
#             logger.info(f"Tool call detectada: {fn_name} con args {fn_args}")

#             # 3.1) Ejecutamos la función local
#             result = FUNCTION_MAP.get(fn_name, lambda **k: {"error": f"No encontré {fn_name}"})(**fn_args)
#             logger.info(f"Resultado de la función local: {result}")

#             # 3.2) Enviamos el resultado al run con submit_tool_outputs
#             tool_payload = {
#                 "tool_outputs": [
#                     {
#                         "tool_call_id": call_id,
#                         "output": json.dumps(result)
#                     }
#                 ]
#             }
#             post_tool = requests.post(
#                 f"https://api.openai.com/v1/threads/{new_thread_id}/runs/{run_id}/tool_outputs",
#                 headers=HEADERS,
#                 json=tool_payload
#             )
#             post_tool.raise_for_status()
#             logger.info("Output de función enviado con submit_tool_outputs")

#             # 3.3) Poll hasta que el run finalmente complete
#             status2 = run_data["status"]
#             while status2 not in ("completed", "failed", "cancelled"):
#                 time.sleep(1)
#                 logger.info("Polling post submit_tool_outputs…")
#                 c2 = requests.get(
#                     f"https://api.openai.com/v1/threads/{new_thread_id}/runs/{run_id}",
#                     headers=HEADERS
#                 )
#                 c2.raise_for_status()
#                 run_data = c2.json()
#                 status2 = run_data["status"]
#                 logger.info(f"Estado tras polling 2: {status2}")

#             if status2 != "completed":
#                 logger.error(f"Segundo run terminó mal: {status2}")
#                 raise RuntimeError(f"Segundo run terminó con {status2}")
#             logger.info("Run completado luego de submit_tool_outputs")

#     # 4) Obtener los mensajes finales del thread
#     logger.info("Recuperando mensajes finales del thread…")
#     final = requests.get(
#         f"https://api.openai.com/v1/threads/{new_thread_id}/messages",
#         headers=HEADERS
#     )
#     final.raise_for_status()
#     data = final.json().get("data", [])
#     logger.info(f"Mensajes finales recibidos: {len(data)}")

#     # 5) Concatenar solo los bloques de texto
#     text = ""
#     for m in data:
#         if m.get("role") == "assistant":
#             for part in m.get("content", []):
#                 if part.get("type") == "text":
#                     text += part["text"]["value"]

#     logger.info(f"Texto final: {text!r}")
#     return text, new_thread_id

