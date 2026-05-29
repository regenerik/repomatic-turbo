from flask import Blueprint, send_file, request, jsonify, current_app
# from flask_bcrypt import Bcrypt
# from flask_jwt_extended import JWTManager
# from models import QuintoSurvey
# from utils.quinto_survey_utils import (
#     obtener_y_guardar_quinto_survey,
#     build_surveymonkey_session,
#     get_json_or_raise,
#     DEFAULT_FIFTH_SURVEY_ID,
# )
# from logging_config import logger
# import os
# from dotenv import load_dotenv
# load_dotenv()
# import pandas as pd
# from io import BytesIO
# from datetime import datetime, timezone
# import threading
# import uuid
# import json


# quinto_survey_bp = Blueprint('quinto_survey_bp', __name__)
# bcrypt = Bcrypt()
# jwt = JWTManager()

# API_KEY = os.getenv('API_KEY')

# JOBS = {}
# LATEST_JOB_ID = None
# JOBS_LOCK = threading.Lock()


# def now_iso():
#     return datetime.now(timezone.utc).isoformat()


# def check_api_key(api_key):
#     return api_key == API_KEY


# def update_job(target_job_id, **changes):
#     with JOBS_LOCK:
#         job = JOBS.get(target_job_id, {})
#         job.update(changes)
#         JOBS[target_job_id] = job
#         return dict(job)


# def get_job(target_job_id):
#     with JOBS_LOCK:
#         job = JOBS.get(target_job_id)
#         return dict(job) if job else None


# def set_latest_job(job_id):
#     global LATEST_JOB_ID
#     with JOBS_LOCK:
#         LATEST_JOB_ID = job_id


# def get_latest_job():
#     with JOBS_LOCK:
#         if not LATEST_JOB_ID:
#             return None

#         job = JOBS.get(LATEST_JOB_ID)
#         return dict(job) if job else None


# @quinto_survey_bp.before_request
# def authorize():
#     if request.method == 'OPTIONS':
#         return

#     public_exact_paths = [
#         '/descargar_quinto_survey',
#         '/recuperar_quinto_survey',
#         '/estado_quinto_survey_actual',
#         '/test_quinto_survey_bp',
#         '/recuperar_segundo_survey',
#         '/test_encuestas_cursos_bp',
#         '/',
#         '/correccion_campos_vacios',
#         '/descargar_positividad_corregida',
#         '/download_comments_evaluation',
#         '/all_comments_evaluation',
#         '/download_resume_csv',
#         '/create_resumes_of_all',
#         '/descargar_excel',
#         '/create_resumes',
#         '/reportes_disponibles',
#         '/create_user',
#         '/login',
#         '/users',
#         '/update_profile',
#         '/update_profile_image',
#         '/update_admin',
#     ]

#     if request.path in public_exact_paths:
#         return

#     if request.path.startswith('/estado_quinto_survey/'):
#         return

#     api_key = request.headers.get('Authorization')
#     if not api_key or not check_api_key(api_key):
#         return jsonify({'message': 'Unauthorized'}), 401


# @quinto_survey_bp.route('/test_quinto_survey_bp', methods=['GET'])
# def test():
#     return jsonify({
#         'message': 'test bien sucedido',
#         'status': 'Si lees esto, las rutas de quinto_survey funcionan ok'
#     }), 200


# @quinto_survey_bp.route('/recuperar_quinto_survey', methods=['GET'])
# def iniciar_recuperacion_quinto():
#     from extensions import executor

#     job_id = str(uuid.uuid4())
#     app = current_app._get_current_object()

#     update_job(
#         job_id,
#         job_id=job_id,
#         status='queued',
#         created_at=now_iso(),
#         started_at=None,
#         finished_at=None,
#         error=None,
#         record_id=None,
#         rows=None,
#         columns=None,
#         binary_size_bytes=None,
#         skipped_details=None,
#     )
#     set_latest_job(job_id)

#     logger.info('0 - GET > /recuperar_quinto_survey iniciado. job_id=%s', job_id)
#     executor.submit(run_obtener_y_guardar_quinto, app, job_id)

#     return jsonify({
#         'message': 'El proceso de recuperacion del quinto survey ha comenzado',
#         'job_id': job_id,
#         'status': 'queued',
#         'status_url': '/estado_quinto_survey_actual',
#         'download_url': '/descargar_quinto_survey',
#         'job_status_url': f'/estado_quinto_survey/{job_id}',
#         'job_download_url': f'/descargar_quinto_survey?job_id={job_id}',
#     }), 202


# def run_obtener_y_guardar_quinto(app, job_id):
#     update_job(job_id, status='running', started_at=now_iso())
#     logger.info('1 - Job quinto survey corriendo. job_id=%s', job_id)

#     try:
#         with app.app_context():
#             result = obtener_y_guardar_quinto_survey(job_id=job_id)

#         update_job(
#             job_id,
#             status='completed',
#             finished_at=now_iso(),
#             error=None,
#             record_id=result.get('record_id'),
#             rows=result.get('rows'),
#             columns=result.get('columns'),
#             binary_size_bytes=result.get('binary_size_bytes'),
#             elapsed_time=result.get('elapsed_time'),
#             skipped_details=result.get('skipped_details'),
#         )

#         logger.info(
#             '2 - Job quinto survey completado. job_id=%s record_id=%s rows=%s size=%s omitidas=%s',
#             job_id,
#             result.get('record_id'),
#             result.get('rows'),
#             result.get('binary_size_bytes'),
#             result.get('skipped_details'),
#         )

#     except Exception as e:
#         logger.error('2 - Job quinto survey fallido. job_id=%s error=%s', job_id, str(e), exc_info=True)
#         update_job(
#             job_id,
#             status='failed',
#             finished_at=now_iso(),
#             error=str(e),
#         )


# @quinto_survey_bp.route('/estado_quinto_survey_actual', methods=['GET'])
# def estado_quinto_survey_actual():
#     job = get_latest_job()

#     if not job:
#         return jsonify({
#             'message': 'No hay job reciente en memoria',
#             'status': 'not_found',
#         }), 404

#     return jsonify(job), 200


# @quinto_survey_bp.route('/estado_quinto_survey/<job_id>', methods=['GET'])
# def estado_quinto_survey(job_id):
#     job = get_job(job_id)

#     if not job:
#         return jsonify({
#             'message': 'No se encontro ese job_id. Si el servidor se reinicio, el estado en memoria se perdio.',
#             'job_id': job_id,
#             'status': 'not_found',
#         }), 404

#     return jsonify(job), 200


# @quinto_survey_bp.route('/descargar_quinto_survey', methods=['GET'])
# def descargar_quinto():
#     try:
#         job_id = request.args.get('job_id')

#         if job_id:
#             job = get_job(job_id)
#         else:
#             job = get_latest_job()

#         if job:
#             if job.get('status') in ('queued', 'running'):
#                 return jsonify({
#                     'message': 'El proceso todavia no esta completo',
#                     'job_id': job.get('job_id'),
#                     'status': job.get('status'),
#                     'error': job.get('error'),
#                 }), 409

#             if job.get('status') == 'failed':
#                 return jsonify({
#                     'message': 'El ultimo proceso fallo. No se descarga para evitar usar datos viejos.',
#                     'job_id': job.get('job_id'),
#                     'status': job.get('status'),
#                     'error': job.get('error'),
#                 }), 409

#             if job.get('status') == 'completed':
#                 record_id = job.get('record_id')
#                 survey_record = QuintoSurvey.query.get(record_id)

#                 if not survey_record:
#                     return jsonify({
#                         'message': 'El job termino, pero no se encontro el registro asociado en DB',
#                         'job_id': job.get('job_id'),
#                         'record_id': record_id,
#                     }), 404
#             else:
#                 survey_record = QuintoSurvey.query.order_by(QuintoSurvey.id.desc()).first()
#         else:
#             survey_record = QuintoSurvey.query.order_by(QuintoSurvey.id.desc()).first()

#         if not survey_record:
#             return jsonify({'message': 'No hay datos del quinto survey en la DB'}), 404

#         logger.info('Recuperando quinto survey desde DB. record_id=%s', survey_record.id)

#         binary_data = survey_record.data
#         df_responses = pd.read_pickle(BytesIO(binary_data))

#         output = BytesIO()
#         with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
#             df_responses.to_excel(writer, index=False, sheet_name='Sheet1')

#         output.seek(0)

#         logger.info(
#             'Archivo Excel quinto survey listo. record_id=%s filas=%s columnas=%s',
#             survey_record.id,
#             df_responses.shape[0],
#             df_responses.shape[1],
#         )

#         return send_file(
#             output,
#             download_name='quinto_survey_respuestas.xlsx',
#             as_attachment=True,
#             mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
#         )

#     except Exception as e:
#         logger.error('Error al generar el archivo Excel quinto survey: %s', str(e), exc_info=True)
#         return jsonify({'message': 'Hubo un error al generar el archivo Excel'}), 500


# @quinto_survey_bp.route('/descargar_raw_quinto_survey', methods=['GET'])
# def descargar_raw_quinto_survey():
#     """
#     Baja las respuestas bulk del quinto survey en crudo y las devuelve como .json.
#     La dejo por compatibilidad con tu herramienta de diagnostico.
#     """
#     try:
#         access_token = os.getenv('SURVEYMONKEY_ACCESS_TOKEN')
#         survey_id = (
#             os.getenv('FIFTH_SURVEY_ID')
#             or os.getenv('QUINTO_SURVEY_ID')
#             or os.getenv('FIFTH_SURVEYMONKEY_ID')
#             or DEFAULT_FIFTH_SURVEY_ID
#         )

#         if not access_token:
#             return jsonify({'message': 'Falta SURVEYMONKEY_ACCESS_TOKEN en variables de entorno'}), 500

#         session = build_surveymonkey_session(access_token)
#         host = 'https://api.surveymonkey.com'
#         url = f'{host}/v3/surveys/{survey_id}/responses/bulk'
#         params = {'page': 1, 'per_page': 1000}
#         page = 1
#         all_data = []

#         logger.info('Bajando quinto survey raw. survey_id=%s', survey_id)

#         try:
#             while url:
#                 js = get_json_or_raise(
#                     session,
#                     url,
#                     params=params,
#                     context=f'quinto survey raw page {page}',
#                 )

#                 data = js.get('data', [])
#                 if not isinstance(data, list):
#                     return jsonify({'message': f'Formato inesperado en pagina raw {page}'}), 500

#                 all_data.extend(data)
#                 next_link = js.get('links', {}).get('next')

#                 if next_link:
#                     url = next_link
#                     params = None
#                     page += 1
#                 else:
#                     break
#         finally:
#             session.close()

#         buf = BytesIO()
#         buf.write(json.dumps(all_data, ensure_ascii=False, indent=2).encode('utf-8'))
#         buf.seek(0)

#         return send_file(
#             buf,
#             download_name='raw_quinto_survey.json',
#             as_attachment=True,
#             mimetype='application/json',
#         )

#     except Exception as e:
#         logger.error('Error bajando raw quinto survey: %s', str(e), exc_info=True)
#         return jsonify({'message': 'Error bajando raw survey'}), 500
