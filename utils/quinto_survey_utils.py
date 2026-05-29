import os
import re
import gc
from datetime import datetime
from io import BytesIO

import pandas as pd
import pytz
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError

from database import db
from models import QuintoSurvey
from logging_config import logger

load_dotenv()

tz = pytz.timezone('America/Sao_Paulo')


DEFAULT_FIFTH_SURVEY_ID = '520546031'


# ===================== MAPPING MANUAL PARA EL QUINTO SURVEY =====================
hardcoded_question_text = {
    '240286181': '¿Qué tan probable es que usted le recomiende este curso a un colega?',
    '213556722': 'De acuerdo a tu experiencia del día de hoy, ¿Cómo calificarías el desempeño del instructor?',
    '213556725': 'En líneas generales, ¿cómo calificarías a este curso/ actividad?',
    '213556723': 'Pensando en los contenidos vistos, considerás que la duración del curso fue:',
    '240267012': 'En cuanto a la información recibida, considerás que es:',
    '240267144': 'A continuación te presentamos una serie de frases, en cada caso decinos cuán de acuerdo estás:',
    '240267640': 'En líneas generales dirías que tu experiencia de aprendizaje con este curso fue:',
    '213556724': 'Para finalizar dejamos este espacio para que nos dejes tus sugerencias o comentarios relacionados a este curso',
}


FINAL_COLUMN_ORDER = [
    'respondent_id',
    'collector_id',
    'date_created',
    'date_modified',
    'ip_address',
    'email_address',
    'first_name',
    'last_name',
    'custom_1',
    'GestoresAprendizaje',
    'Curso',
    '¿Qué tan probable es que usted le recomiende este curso a un colega?',
    'De acuerdo a tu experiencia del día de hoy, ¿Cómo calificarías el desempeño del instructor?',
    'En líneas generales, ¿cómo calificarías a este curso/ actividad?',
    'Pensando en los contenidos vistos, considerás que la duración del curso fue:',
    'En cuanto a la información recibida, considerás que es:',
    'Los temas fueron tratados con claridad',
    'El contenido visto es de utilidad para mi tarea',
    'Las explicaciones, guías, videos, etc. ayudan a poner en práctica lo visto en el curso',
    'Las actividades propuestas refuerzan lo aprendido',
    'En líneas generales dirías que tu experiencia de aprendizaje con este curso fue:',
    'Para finalizar dejamos este espacio para que nos dejes tus sugerencias o comentarios relacionados a este curso',
]


MATRIX_SOURCE_COLUMN = 'A continuación te presentamos una serie de frases, en cada caso decinos cuán de acuerdo estás:'
MATRIX_SPLIT_COLUMNS = {
    0: 'Los temas fueron tratados con claridad',
    1: 'El contenido visto es de utilidad para mi tarea',
    2: 'Las explicaciones, guías, videos, etc. ayudan a poner en práctica lo visto en el curso',
    3: 'Las actividades propuestas refuerzan lo aprendido',
}


def build_surveymonkey_session(access_token):
    session = requests.Session()

    retry = Retry(
        total=4,
        connect=3,
        read=3,
        status=4,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(['GET']),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.mount('http://', adapter)

    session.headers.update({
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    })

    return session


def get_json_or_raise(session, url, params=None, context='request'):
    response = session.get(url, params=params, timeout=(10, 60))

    if response.status_code != 200:
        body_preview = response.text[:1000] if response.text else ''
        raise RuntimeError(
            f'Error en {context}: status={response.status_code}, body={body_preview}'
        )

    try:
        return response.json()
    except ValueError as e:
        raise RuntimeError(f'Respuesta no JSON en {context}: {str(e)}') from e


def extract_text_from_html(html_text):
    if not isinstance(html_text, str):
        return html_text
    return re.sub(r'<[^>]*>', '', html_text).strip()


def safe_text(value, fallback=''):
    if value is None:
        return fallback
    value = extract_text_from_html(value)
    return str(value).strip()


def get_heading(question):
    question_id = question.get('id')
    headings = question.get('headings', [])

    if headings and isinstance(headings, list):
        heading = headings[0].get('heading')
        if heading:
            return safe_text(heading)

    if question_id in hardcoded_question_text:
        return hardcoded_question_text[question_id]

    return safe_text(question_id, fallback='pregunta_sin_id')


def build_survey_maps(survey_details):
    question_text_mapping = {}
    choice_mapping = {}
    row_mapping = {}
    col_mapping = {}

    for page in survey_details.get('pages', []):
        for question in page.get('questions', []):
            q_id = question.get('id')
            if not q_id:
                continue

            question_text_mapping[q_id] = get_heading(question)
            choice_mapping[q_id] = {}
            row_mapping[q_id] = {}
            col_mapping[q_id] = {}

            answers_info = question.get('answers', {}) or {}

            for choice in answers_info.get('choices', []) or []:
                cid = choice.get('id')
                text = choice.get('text') or choice.get('visible') or choice.get('label')
                if cid:
                    choice_mapping[q_id][cid] = safe_text(text, fallback=cid)

            # En matrices, SurveyMonkey puede guardar las opciones en cols.
            for col in answers_info.get('cols', []) or []:
                cid = col.get('id')
                text = col.get('text') or col.get('visible') or col.get('label')
                if cid:
                    col_mapping[q_id][cid] = safe_text(text, fallback=cid)
                    choice_mapping[q_id][cid] = safe_text(text, fallback=cid)

            for row in answers_info.get('rows', []) or []:
                rid = row.get('id')
                text = row.get('text') or row.get('visible') or row.get('label')
                if rid:
                    row_mapping[q_id][rid] = safe_text(text, fallback=rid)

    return question_text_mapping, choice_mapping, row_mapping, col_mapping


def get_answer_text(answer, q_id, choice_mapping, row_mapping, col_mapping):
    text = answer.get('text')
    if text not in (None, ''):
        return safe_text(text)

    choice_id = answer.get('choice_id')
    if choice_id:
        return choice_mapping.get(q_id, {}).get(choice_id, f'[ID:{choice_id}]')

    col_id = answer.get('col_id')
    if col_id:
        return col_mapping.get(q_id, {}).get(col_id, choice_mapping.get(q_id, {}).get(col_id, f'[ID:{col_id}]'))

    # Compatibilidad con formas raras del detail endpoint.
    if answer.get('row') and answer.get('column'):
        return f"Fila:{answer['row']} Col:{answer['column']}"

    row_id = answer.get('row_id')
    if row_id:
        return row_mapping.get(q_id, {}).get(row_id, f'[ROW:{row_id}]')

    return str(answer)


def flatten_response(response_json, question_text_mapping, choice_mapping, row_mapping, col_mapping):
    flat = {
        'respondent_id': response_json.get('id'),
        'collector_id': response_json.get('collector_id'),
        'date_created': response_json.get('date_created'),
        'date_modified': response_json.get('date_modified'),
    }

    meta = response_json.get('metadata', {}) or {}
    if meta:
        flat.update({
            'ip_address': meta.get('ip_address'),
            'email_address': meta.get('email_address'),
            'first_name': meta.get('first_name'),
            'last_name': meta.get('last_name'),
            'custom_1': meta.get('custom_1'),
        })

    for key, val in (response_json.get('custom_variables', {}) or {}).items():
        flat[key] = val

    for page in response_json.get('pages', []) or []:
        for question in page.get('questions', []) or []:
            q_id = question.get('id')
            if not q_id:
                continue

            column_name = question_text_mapping.get(q_id, str(q_id))
            answers = question.get('answers', []) or []
            texts = []

            if not answers:
                texts.append('Sin respuesta')
            else:
                for answer in answers:
                    texts.append(get_answer_text(answer, q_id, choice_mapping, row_mapping, col_mapping))

            flat[column_name] = ', '.join(str(item) for item in texts if item not in (None, ''))

    return flat


def get_detail_response(session, survey_id, response_id, question_text_mapping, choice_mapping, row_mapping, col_mapping):
    url = f'https://api.surveymonkey.com/v3/surveys/{survey_id}/responses/{response_id}/details'
    detail_json = get_json_or_raise(
        session,
        url,
        context=f'quinto survey response detail {response_id}',
    )
    return flatten_response(detail_json, question_text_mapping, choice_mapping, row_mapping, col_mapping)


def split_matrix_column(df):
    if MATRIX_SOURCE_COLUMN not in df.columns:
        return df

    logger.info('7.1 - Separando columna matriz del quinto survey')

    split = df[MATRIX_SOURCE_COLUMN].fillna('').astype(str).str.split(',', expand=True)
    split = split.rename(columns=MATRIX_SPLIT_COLUMNS)

    for col in split.columns:
        split[col] = split[col].astype(str).str.strip()

    df = df.join(split)
    df.drop(columns=[MATRIX_SOURCE_COLUMN], inplace=True)
    return df


def order_final_columns(df):
    existing = [col for col in FINAL_COLUMN_ORDER if col in df.columns]
    extra = [col for col in df.columns if col not in existing]

    # Primero respetamos el formato historico del quinto survey.
    # Si aparece alguna columna nueva de SurveyMonkey, no la tiramos a la basura: va al final.
    return df[existing + extra]


def obtener_y_guardar_quinto_survey(job_id=None):
    access_token = os.getenv('SURVEYMONKEY_ACCESS_TOKEN')
    survey_id = (
        os.getenv('FIFTH_SURVEY_ID')
        or os.getenv('QUINTO_SURVEY_ID')
        or os.getenv('FIFTH_SURVEYMONKEY_ID')
        or DEFAULT_FIFTH_SURVEY_ID
    )

    if not access_token:
        raise RuntimeError('Falta SURVEYMONKEY_ACCESS_TOKEN en variables de entorno')

    if not survey_id:
        raise RuntimeError('Falta FIFTH_SURVEY_ID / QUINTO_SURVEY_ID en variables de entorno')

    hora_inicio = datetime.now()
    logger.info('2 - Iniciando recuperacion del quinto survey. job_id=%s survey_id=%s', job_id, survey_id)

    host = 'https://api.surveymonkey.com'
    survey_details_url = f'{host}/v3/surveys/{survey_id}/details'
    survey_responses_url = f'{host}/v3/surveys/{survey_id}/responses/bulk'

    session = build_surveymonkey_session(access_token)

    try:
        logger.info('3 - Obteniendo detalles del quinto survey. job_id=%s', job_id)
        survey_details = get_json_or_raise(session, survey_details_url, context='quinto survey details')
        q_map, c_map, row_map, col_map = build_survey_maps(survey_details)

        if not q_map:
            raise RuntimeError('No se pudieron obtener mapeos del quinto survey')

        logger.info(
            '4 - Mapeos quinto survey cargados. job_id=%s preguntas=%s opciones=%s filas_matriz=%s columnas_matriz=%s',
            job_id,
            len(q_map),
            sum(len(v) for v in c_map.values()),
            sum(len(v) for v in row_map.values()),
            sum(len(v) for v in col_map.values()),
        )

        all_responses = []
        skipped_details = 0
        page = 1
        next_url = survey_responses_url
        params = {'page': page, 'per_page': 1000}

        while next_url:
            logger.info('5.%s - Bajando pagina bulk quinto survey. job_id=%s', page, job_id)

            bulk_json = get_json_or_raise(
                session,
                next_url,
                params=params,
                context=f'quinto survey responses bulk page {page}',
            )

            data = bulk_json.get('data', [])
            if not isinstance(data, list):
                raise RuntimeError(f'Formato inesperado en pagina {page}: data no es lista')

            logger.info(
                '5.%s - Pagina recibida quinto survey. job_id=%s respuestas=%s acumuladas=%s',
                page,
                job_id,
                len(data),
                len(all_responses) + len(data),
            )

            if not data:
                break

            for index, resp in enumerate(data, start=1):
                response_id = resp.get('id')
                if not response_id:
                    skipped_details += 1
                    continue

                try:
                    detail = get_detail_response(session, survey_id, response_id, q_map, c_map, row_map, col_map)
                    if detail:
                        all_responses.append(detail)
                    else:
                        skipped_details += 1
                except Exception as e:
                    skipped_details += 1
                    logger.error(
                        'Detalle quinto survey fallido. job_id=%s response_id=%s error=%s',
                        job_id,
                        response_id,
                        str(e),
                        exc_info=True,
                    )

                if index % 100 == 0:
                    logger.info(
                        '5.%s - Detalles procesados en pagina quinto survey. job_id=%s procesados=%s/%s acumulados=%s omitidos=%s',
                        page,
                        job_id,
                        index,
                        len(data),
                        len(all_responses),
                        skipped_details,
                    )

            next_link = bulk_json.get('links', {}).get('next')
            if next_link:
                next_url = next_link
                params = None
                page += 1
            else:
                break

        if not all_responses:
            raise RuntimeError('SurveyMonkey no devolvio respuestas utiles para el quinto survey. No se pisa la DB.')

        logger.info(
            '6 - Procesando respuestas quinto survey. job_id=%s total_respuestas=%s omitidas=%s',
            job_id,
            len(all_responses),
            skipped_details,
        )

        df = pd.DataFrame(all_responses)

        if df.empty:
            raise RuntimeError('El DataFrame final del quinto survey quedo vacio. No se pisa la DB.')

        df = split_matrix_column(df)
        df = order_final_columns(df)

        logger.info(
            '7 - DataFrame quinto survey listo. job_id=%s filas=%s columnas=%s',
            job_id,
            df.shape[0],
            df.shape[1],
        )

        # Mantengo tu intercepcion original para el flujo de encuestas presenciales.
        logger.info('7.2 - Enviando DataFrame quinto survey a procesar_encuestas_presenciales. job_id=%s', job_id)
        import io
        from utils.rescate_utils import procesar_encuestas_presenciales

        csv_buffer = io.BytesIO()
        csv_buffer.write(df.to_csv(index=False).encode('utf-8'))
        csv_buffer.seek(0)
        procesar_encuestas_presenciales(csv_buffer)
        logger.info('7.3 - procesar_encuestas_presenciales termino correctamente. job_id=%s', job_id)

        with BytesIO() as output:
            df.to_pickle(output)
            binary_data = output.getvalue()

        binary_size_bytes = len(binary_data)
        if binary_size_bytes <= 0:
            raise RuntimeError('El binario generado del quinto survey esta vacio. No se pisa la DB.')

        logger.info(
            '8 - Guardando quinto survey en DB. job_id=%s binary_size_bytes=%s',
            job_id,
            binary_size_bytes,
        )

        try:
            db.session.query(QuintoSurvey).delete()
            db.session.flush()

            new_survey = QuintoSurvey(data=binary_data)
            db.session.add(new_survey)
            db.session.commit()

        except SQLAlchemyError as e:
            db.session.rollback()
            raise RuntimeError(f'Error SQLAlchemy guardando quinto survey: {str(e)}') from e

        elapsed_time = datetime.now() - hora_inicio

        logger.info(
            '9 - Quinto survey guardado correctamente. job_id=%s record_id=%s filas=%s columnas=%s tiempo=%s omitidas=%s',
            job_id,
            new_survey.id,
            df.shape[0],
            df.shape[1],
            str(elapsed_time),
            skipped_details,
        )

        result = {
            'record_id': new_survey.id,
            'rows': int(df.shape[0]),
            'columns': int(df.shape[1]),
            'binary_size_bytes': int(binary_size_bytes),
            'elapsed_time': str(elapsed_time),
            'skipped_details': int(skipped_details),
        }

        gc.collect()
        return result

    finally:
        session.close()
