import json
import os
import re
import time
from io import BytesIO
from datetime import datetime

from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession

from database import db
from models import DriveFileDownload
from logging_config import logger


# ============================================================
# CONFIGURACION
# ============================================================

DEFAULT_DRIVE_URL = os.getenv(
    "GOOGLE_DRIVE_FILE_URL",
    (
        "https://docs.google.com/spreadsheets/d/"
        "1uzK9UQOIW5JQD-4VR-paWrKTvpZM2ao_IGEGK0QSg0o/"
        "edit?gid=1374924463#gid=1374924463"
    )
)

DEFAULT_FILE_NAME = os.getenv(
    "GOOGLE_DRIVE_FILE_NAME",
    "Rating_YPF.xlsx"
)

GOOGLE_SERVICE_ACCOUNT_JSON_ENV = "GOOGLE_SERVICE_ACCOUNT_JSON"

DRIVE_READONLY_SCOPE = (
    "https://www.googleapis.com/auth/drive.readonly"
)

XLSX_MIME_TYPE = (
    "application/vnd.openxmlformats-officedocument."
    "spreadsheetml.sheet"
)


# ============================================================
# OBTENER ID DE GOOGLE DRIVE
# ============================================================

def extract_google_file_id(url):
    """
    Extrae el ID de una URL de Google Drive / Google Sheets.

    Ejemplo:

    https://docs.google.com/spreadsheets/d/ABC123/edit

    devuelve:

    ABC123
    """

    match = re.search(
        r"/d/([a-zA-Z0-9_-]+)",
        url
    )

    if not match:
        raise ValueError(
            "No se pudo obtener el ID del archivo "
            "desde la URL de Google Drive"
        )

    return match.group(1)


# ============================================================
# CARGAR CREDENCIALES DESDE VARIABLE DE ENTORNO
# ============================================================

def build_google_credentials():
    """
    Lee el JSON completo de la Service Account desde:

    GOOGLE_SERVICE_ACCOUNT_JSON

    No requiere guardar ningun archivo .json en el servidor.
    """

    raw_credentials = os.getenv(
        GOOGLE_SERVICE_ACCOUNT_JSON_ENV
    )

    if not raw_credentials:
        raise RuntimeError(
            "Falta la variable de entorno "
            "GOOGLE_SERVICE_ACCOUNT_JSON"
        )

    try:
        service_account_info = json.loads(
            raw_credentials
        )

    except json.JSONDecodeError as e:
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON no contiene "
            "un JSON valido. "
            f"Detalle: {str(e)}"
        ) from e

    if service_account_info.get("type") != "service_account":
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON no parece "
            "ser una credencial de Service Account"
        )

    required_fields = (
        "project_id",
        "private_key",
        "client_email",
        "token_uri"
    )

    missing_fields = [
        field
        for field in required_fields
        if not service_account_info.get(field)
    ]

    if missing_fields:
        raise RuntimeError(
            "Faltan campos obligatorios en "
            "GOOGLE_SERVICE_ACCOUNT_JSON: "
            + ", ".join(missing_fields)
        )

    try:
        credentials = (
            service_account.Credentials
            .from_service_account_info(
                service_account_info,
                scopes=[DRIVE_READONLY_SCOPE]
            )
        )

    except Exception as e:
        raise RuntimeError(
            "No se pudieron construir las credenciales "
            f"de Google: {str(e)}"
        ) from e

    return credentials


# ============================================================
# CONSTRUIR URL DE DESCARGA LRO
# ============================================================

def build_drive_download_url(drive_file_id):
    """
    Usa el endpoint Drive API v3 files.download.

    A diferencia de files.export, inicia una operacion
    de larga duracion (Long Running Operation).
    """

    return (
        "https://www.googleapis.com/drive/v3/files/"
        f"{drive_file_id}/download"
    )


# ============================================================
# CONSTRUIR URL PARA CONSULTAR OPERACION
# ============================================================

def build_operation_url(operation_name):
    """
    Construye la URL de operations.get.

    Google normalmente devuelve operation_name con formato:

    operations/XXXXXXXX

    El endpoint REST espera solamente el nombre de la operacion
    despues de /operations/.
    """

    operation_id = operation_name.split("/")[-1]

    return (
        "https://www.googleapis.com/drive/v3/operations/"
        f"{operation_id}"
    )


# ============================================================
# DESCARGAR GOOGLE SHEET PRIVADO COMO XLSX
# ============================================================

def download_google_sheet_as_xlsx(drive_url):
    """
    Descarga una Google Sheet privada como XLSX usando
    Drive API files.download.

    Flujo:

    1. Obtiene file_id desde la URL.
    2. Autentica con Service Account.
    3. Inicia files.download.
    4. Google devuelve una Long Running Operation.
    5. Consultamos operations.get hasta finalizar.
    6. Obtenemos downloadUri.
    7. Descargamos el XLSX.
    8. Retornamos el binario para guardarlo en la DB.
    """

    drive_file_id = extract_google_file_id(
        drive_url
    )

    credentials = build_google_credentials()

    session = AuthorizedSession(
        credentials
    )

    download_url = build_drive_download_url(
        drive_file_id
    )

    logger.info(
        "Iniciando descarga Google Sheet privado. "
        "drive_file_id=%s service_account=%s",
        drive_file_id,
        credentials.service_account_email
    )

    # ========================================================
    # 1. INICIAR LONG RUNNING OPERATION
    # ========================================================

    response = session.post(
        download_url,
        params={
            "mimeType": XLSX_MIME_TYPE
        },
        headers={
            "Accept": "application/json",
            "Content-Length": "0"
        },
        timeout=(15, 60)
    )

    if response.status_code not in (200, 201):

        try:
            error_detail = response.text[:2000]

        except Exception:
            error_detail = "<sin detalle>"

        raise RuntimeError(
            "Google Drive API rechazo el inicio "
            "de la descarga. "
            f"HTTP {response.status_code}. "
            f"Detalle: {error_detail}"
        )

    try:
        operation_data = response.json()

    except Exception as e:
        raise RuntimeError(
            "Google Drive no devolvio una operacion "
            "JSON valida"
        ) from e

    operation_name = operation_data.get("name")

    if not operation_name:
        raise RuntimeError(
            "Google Drive no devolvio el nombre "
            "de la operacion. "
            f"Respuesta: {operation_data}"
        )

    logger.info(
        "Operacion Drive creada. "
        "drive_file_id=%s operation=%s",
        drive_file_id,
        operation_name
    )

    # ========================================================
    # 2. CONSULTAR OPERACION HASTA QUE TERMINE
    # ========================================================

    operation_url = build_operation_url(
        operation_name
    )

    max_attempts = 150
    wait_seconds = 2

    final_operation = None

    for attempt in range(
        1,
        max_attempts + 1
    ):

        # La respuesta inicial puede venir ya terminada.
        if operation_data.get("done"):
            final_operation = operation_data
            break

        logger.info(
            "Esperando operacion Drive. "
            "job_attempt=%s/%s operation=%s",
            attempt,
            max_attempts,
            operation_name
        )

        time.sleep(
            wait_seconds
        )

        operation_response = session.get(
            operation_url,
            timeout=(15, 60)
        )

        if operation_response.status_code != 200:

            try:
                error_detail = (
                    operation_response.text[:2000]
                )

            except Exception:
                error_detail = "<sin detalle>"

            raise RuntimeError(
                "Google Drive API rechazo la consulta "
                "del estado de la operacion. "
                f"HTTP {operation_response.status_code}. "
                f"Detalle: {error_detail}"
            )

        try:
            operation_data = (
                operation_response.json()
            )

        except Exception as e:
            raise RuntimeError(
                "Google Drive devolvio una respuesta "
                "invalida al consultar la operacion"
            ) from e

        if operation_data.get("done"):
            final_operation = operation_data
            break

    if not final_operation:
        raise TimeoutError(
            "Google Drive tardo demasiado en preparar "
            "la descarga del archivo"
        )

    # ========================================================
    # 3. VALIDAR RESULTADO DE LA OPERACION
    # ========================================================

    if final_operation.get("error"):

        raise RuntimeError(
            "Google Drive fallo al preparar "
            "la descarga. "
            f"Detalle: {final_operation['error']}"
        )

    operation_result = final_operation.get(
        "response",
        {}
    )

    download_uri = operation_result.get(
        "downloadUri"
    )

    if not download_uri:
        raise RuntimeError(
            "Google Drive termino la operacion pero "
            "no devolvio downloadUri. "
            f"Respuesta: {final_operation}"
        )

    logger.info(
        "Archivo preparado por Google Drive. "
        "drive_file_id=%s",
        drive_file_id
    )

    # ========================================================
    # 4. DESCARGAR XLSX FINAL
    # ========================================================

    file_response = session.get(
        download_uri,
        stream=True,
        allow_redirects=True,
        timeout=(15, 300)
    )

    if file_response.status_code != 200:

        try:
            error_detail = (
                file_response.text[:2000]
            )

        except Exception:
            error_detail = "<sin detalle>"

        raise RuntimeError(
            "No se pudo descargar el XLSX generado. "
            f"HTTP {file_response.status_code}. "
            f"Detalle: {error_detail}"
        )

    content_type = file_response.headers.get(
        "Content-Type",
        ""
    ).lower()

    if "text/html" in content_type:
        raise RuntimeError(
            "Google devolvio HTML en lugar del XLSX. "
            "Revisar permisos del archivo y "
            "la Service Account."
        )

    buffer = BytesIO()

    for chunk in file_response.iter_content(
        chunk_size=1024 * 1024
    ):
        if chunk:
            buffer.write(chunk)

    binary_data = buffer.getvalue()

    if not binary_data:
        raise RuntimeError(
            "Google Drive devolvio un archivo vacio"
        )

    # XLSX internamente es un ZIP.
    if not binary_data.startswith(b"PK"):
        raise RuntimeError(
            "La respuesta de Google no parece "
            "ser un XLSX valido"
        )

    logger.info(
        "Google Sheet descargado correctamente. "
        "drive_file_id=%s size=%s bytes",
        drive_file_id,
        len(binary_data)
    )

    return {
        "drive_file_id": drive_file_id,
        "data": binary_data,
        "size_bytes": len(binary_data)
    }


# ============================================================
# PROCESO COMPLETO DEL JOB
# ============================================================

def process_drive_download(job_id):
    """
    Ejecuta todo el proceso:

    queued
       ↓
    running
       ↓
    autentica Service Account
       ↓
    inicia files.download
       ↓
    espera Long Running Operation
       ↓
    descarga Google Sheet privado como XLSX
       ↓
    guarda binario
       ↓
    completed
    """

    job = DriveFileDownload.query.filter_by(
        job_id=job_id
    ).first()

    if not job:
        raise RuntimeError(
            f"No existe el job {job_id}"
        )

    job.status = "running"
    job.started_at = datetime.utcnow()
    job.error = None

    db.session.commit()

    try:

        result = download_google_sheet_as_xlsx(
            job.source_url
        )

        job.drive_file_id = result[
            "drive_file_id"
        ]

        job.data = result[
            "data"
        ]

        job.size_bytes = result[
            "size_bytes"
        ]

        job.status = "completed"
        job.finished_at = datetime.utcnow()
        job.error = None

        db.session.commit()

        logger.info(
            "Job Drive completado. "
            "job_id=%s size=%s",
            job_id,
            result["size_bytes"]
        )

        return {
            "job_id": job_id,
            "status": "completed",
            "size_bytes": result[
                "size_bytes"
            ]
        }

    except Exception as e:

        db.session.rollback()

        # Recuperamos nuevamente el registro porque
        # hicimos rollback.
        job = DriveFileDownload.query.filter_by(
            job_id=job_id
        ).first()

        if job:
            job.status = "failed"
            job.error = str(e)
            job.finished_at = datetime.utcnow()

            db.session.commit()

        logger.error(
            "Error descargando archivo de Drive. "
            "job_id=%s error=%s",
            job_id,
            str(e),
            exc_info=True
        )

        raise