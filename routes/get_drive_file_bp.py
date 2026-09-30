from flask import (
    Blueprint,
    jsonify,
    current_app,
    request,
    send_file
)

from io import BytesIO
from datetime import datetime
import uuid

from database import db
from models import DriveFileDownload
from logging_config import logger

from utils.get_drive_file_utils import (
    process_drive_download,
    extract_google_file_id,
    download_history_as_xlsx,
    DEFAULT_DRIVE_URL,
    DEFAULT_FILE_NAME,
    DEFAULT_HISTORY_FILE_NAME
)


get_drive_file_bp = Blueprint(
    "get_drive_file_bp",
    __name__
)


# ============================================================
# INICIAR RECUPERACION
# ============================================================

@get_drive_file_bp.route(
    "/recuperar_drive_file",
    methods=["GET"]
)
def recuperar_drive_file():

    from extensions import executor

    try:

        job_id = str(uuid.uuid4())

        drive_file_id = extract_google_file_id(
            DEFAULT_DRIVE_URL
        )

        new_job = DriveFileDownload(
            job_id=job_id,
            drive_file_id=drive_file_id,
            source_url=DEFAULT_DRIVE_URL,
            file_name=DEFAULT_FILE_NAME,
            status="queued",
            created_at=datetime.utcnow()
        )

        db.session.add(new_job)
        db.session.commit()

        app = current_app._get_current_object()

        executor.submit(
            run_drive_download,
            app,
            job_id
        )

        logger.info(
            "Descarga Drive iniciada. job_id=%s",
            job_id
        )

        return jsonify({
            "message": (
                "La recuperacion del archivo desde "
                "Google Drive ha comenzado"
            ),
            "job_id": job_id,
            "status": "queued",

            "status_url":
                f"/estado_drive_file/{job_id}",

            "download_url":
                f"/descargar_drive_file?job_id={job_id}",

            "historical_download_url":
                "/descargar_drive_file_historico"
        }), 202

    except Exception as e:

        db.session.rollback()

        logger.error(
            "Error iniciando descarga Drive: %s",
            str(e),
            exc_info=True
        )

        return jsonify({
            "message": (
                "No se pudo iniciar la recuperacion "
                "del archivo"
            ),
            "error": str(e)
        }), 500


# ============================================================
# FUNCION QUE EJECUTA EL EXECUTOR
# ============================================================

def run_drive_download(app, job_id):

    try:

        with app.app_context():

            logger.info(
                "Background Drive iniciado. job_id=%s",
                job_id
            )

            process_drive_download(
                job_id
            )

    except Exception as e:

        logger.error(
            "Background Drive fallo. "
            "job_id=%s error=%s",
            job_id,
            str(e),
            exc_info=True
        )


# ============================================================
# CONSULTAR ESTADO
# ============================================================

@get_drive_file_bp.route(
    "/estado_drive_file/<job_id>",
    methods=["GET"]
)
def estado_drive_file(job_id):

    job = DriveFileDownload.query.filter_by(
        job_id=job_id
    ).first()

    if not job:
        return jsonify({
            "message": "No existe ese job",
            "job_id": job_id
        }), 404

    return jsonify(
        job.serialize()
    ), 200


# ============================================================
# DESCARGAR ARCHIVO DIARIO
# ============================================================

@get_drive_file_bp.route(
    "/descargar_drive_file",
    methods=["GET"]
)
def descargar_drive_file():

    try:

        job_id = request.args.get(
            "job_id"
        )

        if job_id:

            job = DriveFileDownload.query.filter_by(
                job_id=job_id
            ).first()

        else:

            job = DriveFileDownload.query.order_by(
                DriveFileDownload.id.desc()
            ).first()

        if not job:

            return jsonify({
                "message": (
                    "No existe ninguna recuperacion "
                    "del archivo"
                )
            }), 404

        if job.status in (
            "queued",
            "running"
        ):

            return jsonify({
                "message": (
                    "El archivo todavia no esta "
                    "disponible"
                ),
                "job_id": job.job_id,
                "status": job.status
            }), 409

        if job.status == "failed":

            return jsonify({
                "message": (
                    "La recuperacion del archivo fallo"
                ),
                "job_id": job.job_id,
                "status": job.status,
                "error": job.error
            }), 409

        if not job.data:

            return jsonify({
                "message": (
                    "El proceso termino pero "
                    "el archivo no existe"
                ),
                "job_id": job.job_id
            }), 500

        buffer = BytesIO(
            job.data
        )

        buffer.seek(0)

        logger.info(
            "Entregando archivo diario Drive. "
            "job_id=%s size=%s",
            job.job_id,
            job.size_bytes
        )

        return send_file(
            buffer,
            download_name=(
                job.file_name
                or "drive_file.xlsx"
            ),
            as_attachment=True,
            mimetype=(
                "application/vnd.openxmlformats-officedocument."
                "spreadsheetml.sheet"
            )
        )

    except Exception as e:

        logger.error(
            "Error entregando archivo Drive: %s",
            str(e),
            exc_info=True
        )

        return jsonify({
            "message": "Error descargando el archivo",
            "error": str(e)
        }), 500


# ============================================================
# DESCARGAR ARCHIVO HISTORICO
# ============================================================

@get_drive_file_bp.route(
    "/descargar_drive_file_historico",
    methods=["GET"]
)
def descargar_drive_file_historico():

    try:

        result = download_history_as_xlsx()

        buffer = BytesIO(
            result["data"]
        )

        buffer.seek(0)

        logger.info(
            "Entregando archivo historico Drive. "
            "drive_file_id=%s size=%s",
            result["drive_file_id"],
            result["size_bytes"]
        )

        return send_file(
            buffer,
            download_name=(
                DEFAULT_HISTORY_FILE_NAME
            ),
            as_attachment=True,
            mimetype=(
                "application/vnd.openxmlformats-officedocument."
                "spreadsheetml.sheet"
            )
        )

    except Exception as e:

        logger.error(
            "Error entregando historico Drive: %s",
            str(e),
            exc_info=True
        )

        return jsonify({
            "message": (
                "Error descargando el archivo historico"
            ),
            "error": str(e)
        }), 500
