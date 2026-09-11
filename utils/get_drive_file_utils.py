import re
import requests
from io import BytesIO
from datetime import datetime

from database import db
from models import DriveFileDownload
from logging_config import logger


# ============================================================
# CONFIGURACION DEL ARCHIVO
# ============================================================

DEFAULT_DRIVE_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1uzK9UQOIW5JQD-4VR-paWrKTvpZM2ao_IGEGK0QSg0o/"
    "edit?gid=1374924463#gid=1374924463"
)

DEFAULT_FILE_NAME = "Rating_YPF.xlsx"


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

    match = re.search(r"/d/([a-zA-Z0-9_-]+)", url)

    if not match:
        raise ValueError(
            "No se pudo obtener el ID del archivo desde la URL de Google Drive"
        )

    return match.group(1)


# ============================================================
# CONSTRUIR URL DE EXPORTACION
# ============================================================

def build_xlsx_export_url(drive_file_id):
    """
    Como el archivo es un Google Sheet nativo,
    le pedimos a Google que lo exporte como XLSX.
    """

    return (
        f"https://docs.google.com/spreadsheets/d/"
        f"{drive_file_id}/export?format=xlsx"
    )


# ============================================================
# DESCARGAR GOOGLE SHEET COMO XLSX
# ============================================================

def download_google_sheet_as_xlsx(drive_url):
    drive_file_id = extract_google_file_id(drive_url)

    export_url = build_xlsx_export_url(drive_file_id)

    logger.info(
        "Descargando Google Sheet. drive_file_id=%s",
        drive_file_id
    )

    response = requests.get(
        export_url,
        stream=True,
        timeout=(15, 300),
        allow_redirects=True
    )

    response.raise_for_status()

    # --------------------------------------------------------
    # Google a veces devuelve una pagina HTML cuando el archivo
    # necesita login/permisos.
    # --------------------------------------------------------

    content_type = response.headers.get(
        "Content-Type",
        ""
    ).lower()

    if "text/html" in content_type:
        raise RuntimeError(
            "Google devolvio HTML en lugar del Excel. "
            "Probablemente el archivo no permite acceso publico "
            "al servidor."
        )

    # --------------------------------------------------------
    # Lo construimos por chunks para no depender de response.content
    # directamente.
    # --------------------------------------------------------

    buffer = BytesIO()

    for chunk in response.iter_content(
        chunk_size=1024 * 1024
    ):
        if chunk:
            buffer.write(chunk)

    binary_data = buffer.getvalue()

    if not binary_data:
        raise RuntimeError(
            "Google Drive devolvio un archivo vacio"
        )

    logger.info(
        "Google Sheet descargado correctamente. size=%s bytes",
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
    descarga Google
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

        job.drive_file_id = result["drive_file_id"]
        job.data = result["data"]
        job.size_bytes = result["size_bytes"]

        job.status = "completed"
        job.finished_at = datetime.utcnow()
        job.error = None

        db.session.commit()

        logger.info(
            "Job Drive completado. job_id=%s size=%s",
            job_id,
            result["size_bytes"]
        )

        return {
            "job_id": job_id,
            "status": "completed",
            "size_bytes": result["size_bytes"]
        }

    except Exception as e:

        db.session.rollback()

        # Recuperamos nuevamente el registro porque hicimos rollback
        job = DriveFileDownload.query.filter_by(
            job_id=job_id
        ).first()

        if job:
            job.status = "failed"
            job.error = str(e)
            job.finished_at = datetime.utcnow()

            db.session.commit()

        logger.error(
            "Error descargando archivo de Drive. job_id=%s error=%s",
            job_id,
            str(e),
            exc_info=True
        )

        raise