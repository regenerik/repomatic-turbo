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
from models import CuartoSurvey
from logging_config import logger

load_dotenv()

tz = pytz.timezone('America/Sao_Paulo')


# Fallback usado por tu ruta raw vieja. Si despues lo pasas a .env, mejor.
DEFAULT_FOURTH_SURVEY_ID = '514508354'


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


def put_answer(target, column_key, value):
    if value is None:
        return

    if isinstance(value, str):
        value = value.strip()
        if value == '':
            return

    if column_key not in target or target[column_key] in (None, ''):
        target[column_key] = value
        return

    existing = target[column_key]

    if isinstance(existing, list):
        existing.append(value)
        return

    target[column_key] = [existing, value]


def normalize_multi_answers(row):
    for key, value in list(row.items()):
        if isinstance(value, list):
            row[key] = ' | '.join(str(item) for item in value)


def make_unique_columns(columns):
    """
    Evita columnas duplicadas si SurveyMonkey tiene headings repetidos.
    Excel y pandas toleran duplicados, pero despues depurar eso es un picnic en Mordor.
    """
    used = {}
    unique = []

    for col in columns:
        base = safe_text(col, fallback='sin_nombre') or 'sin_nombre'
        count = used.get(base, 0)

        if count == 0:
            unique.append(base)
        else:
            unique.append(f'{base} ({count + 1})')

        used[base] = count + 1

    return unique


def get_heading(question):
    headings = question.get('headings', [])
    if headings and isinstance(headings, list):
        heading = headings[0].get('heading')
        if heading:
            return safe_text(heading)
    return safe_text(question.get('id'), fallback='pregunta_sin_id')


def build_survey_maps(survey_details):
    """
    Arma mapas para preguntas simples, opciones y matrices.

    Para matrices usamos una columna por fila:
    "Pregunta - Texto de fila" => "Texto de opcion elegida".
    Eso respeta la estructura logica del cuarto survey sin meter el formato del segundo.
    """
    choice_map = {}
    question_map = {}
    row_map = {}
    matrix_column_map = {}

    simple_questions = 0
    matrix_questions = 0

    for page in survey_details.get('pages', []):
        for question in page.get('questions', []):
            question_id = question.get('id')
            if not question_id:
                continue

            heading = get_heading(question)
            question_map[question_id] = heading

            answers = question.get('answers', {}) or {}
            choices = answers.get('choices', []) or []
            rows = answers.get('rows', []) or []
            cols = answers.get('cols', []) or []

            for choice in choices:
                choice_id = choice.get('id')
                choice_text = choice.get('text') or choice.get('visible') or choice.get('label')
                if choice_id:
                    choice_map[choice_id] = safe_text(choice_text, fallback=choice_id)

            # Algunas matrices guardan las columnas como cols en lugar de choices.
            for col in cols:
                col_id = col.get('id')
                col_text = col.get('text') or col.get('visible') or col.get('label')
                if col_id:
                    choice_map[col_id] = safe_text(col_text, fallback=col_id)

            if rows:
                matrix_questions += 1
                for row in rows:
                    row_id = row.get('id')
                    row_text = row.get('text') or row.get('visible') or row.get('label')
                    if not row_id:
                        continue

                    clean_row_text = safe_text(row_text, fallback=row_id)
                    row_map[row_id] = clean_row_text
                    matrix_column_map[(question_id, row_id)] = f'{heading} - {clean_row_text}'
            else:
                simple_questions += 1

    return {
        'choice_map': choice_map,
        'question_map': question_map,
        'row_map': row_map,
        'matrix_column_map': matrix_column_map,
        'simple_questions': simple_questions,
        'matrix_questions': matrix_questions,
    }


def get_answer_value(answer, choice_map, row_map):
    text = answer.get('text')
    choice_id = answer.get('choice_id')
    col_id = answer.get('col_id')
    row_id = answer.get('row_id')

    choice_label = None
    if choice_id:
        choice_label = choice_map.get(choice_id, choice_id)
    elif col_id:
        choice_label = choice_map.get(col_id, col_id)

    # Caso "Otro": viene opcion + texto. Guardamos ambas cosas para no perder info.
    if text not in (None, '') and choice_label:
        return f'{choice_label}: {safe_text(text)}'

    if text not in (None, ''):
        return safe_text(text)

    if choice_label:
        return choice_label

    # Si solo vino row_id, al menos no perdemos el dato.
    if row_id:
        return row_map.get(row_id, row_id)

    return None


def get_column_key(question_id, answer, matrix_column_map):
    row_id = answer.get('row_id')

    if row_id and (question_id, row_id) in matrix_column_map:
        return f'{question_id}__row__{row_id}'

    return question_id


def obtener_y_guardar_cuarto_survey(job_id=None):
    access_token = os.getenv('SURVEYMONKEY_ACCESS_TOKEN')
    survey_id = (
        os.getenv('FOURTH_SURVEY_ID')
        or os.getenv('CUARTO_SURVEY_ID')
        or os.getenv('FOURTH_SURVEYMONKEY_ID')
        or DEFAULT_FOURTH_SURVEY_ID
    )

    if not access_token:
        raise RuntimeError('Falta SURVEYMONKEY_ACCESS_TOKEN en variables de entorno')

    if not survey_id:
        raise RuntimeError('Falta FOURTH_SURVEY_ID / CUARTO_SURVEY_ID en variables de entorno')

    hora_inicio = datetime.now()
    logger.info('2 - Iniciando recuperacion del cuarto survey. job_id=%s survey_id=%s', job_id, survey_id)

    host = 'https://api.surveymonkey.com'
    survey_responses_url = f'{host}/v3/surveys/{survey_id}/responses/bulk'
    survey_details_url = f'{host}/v3/surveys/{survey_id}/details'

    session = build_surveymonkey_session(access_token)

    logger.info('3 - Obteniendo detalles del cuarto survey. job_id=%s', job_id)
    survey_details = get_json_or_raise(session, survey_details_url, context='cuarto survey details')

    maps = build_survey_maps(survey_details)
    choice_map = maps['choice_map']
    question_map = maps['question_map']
    row_map = maps['row_map']
    matrix_column_map = maps['matrix_column_map']

    logger.info(
        '4 - Mapeos cuarto survey cargados. job_id=%s simples=%s matrices=%s opciones=%s filas_matriz=%s',
        job_id,
        maps['simple_questions'],
        maps['matrix_questions'],
        len(choice_map),
        len(row_map),
    )

    logger.info('5 - Obteniendo respuestas del cuarto survey. job_id=%s', job_id)

    page = 1
    per_page = 1000
    all_responses = []
    next_url = survey_responses_url
    params = {'page': page, 'per_page': per_page}

    while next_url:
        logger.info('5.%s - Bajando pagina SurveyMonkey cuarto survey. job_id=%s', page, job_id)

        response_json = get_json_or_raise(
            session,
            next_url,
            params=params,
            context=f'cuarto survey responses bulk page {page}',
        )

        responses_page = response_json.get('data', [])

        if not isinstance(responses_page, list):
            raise RuntimeError(f'Formato inesperado en pagina {page}: data no es lista')

        logger.info(
            '5.%s - Pagina recibida cuarto survey. job_id=%s respuestas=%s acumuladas=%s',
            page,
            job_id,
            len(responses_page),
            len(all_responses) + len(responses_page),
        )

        if not responses_page:
            break

        all_responses.extend(responses_page)

        next_link = response_json.get('links', {}).get('next')

        if next_link:
            next_url = next_link
            params = None
            page += 1
        else:
            break

    if not all_responses:
        raise RuntimeError('SurveyMonkey no devolvio respuestas para el cuarto survey. No se pisa la DB con un dataset vacio.')

    logger.info('6 - Procesando respuestas cuarto survey. job_id=%s total_respuestas=%s', job_id, len(all_responses))

    rows = []

    for response in all_responses:
        respondent_id = response.get('id')
        if not respondent_id:
            continue

        row = {
            'response_id': respondent_id,
            'custom_variables': '',
            'STORE_CODE': '',
            'date_created': response.get('date_created', '')[:10],
        }

        custom_vars = response.get('custom_variables', {}) or {}
        row['custom_variables'] = custom_vars.get('ID_CODE', '')
        row['STORE_CODE'] = custom_vars.get('STORE_CODE', '')

        # Dejamos disponibles tambien las custom vars extra, por si el cuarto survey trae otros nombres.
        for custom_key, custom_value in custom_vars.items():
            if custom_key not in ('ID_CODE', 'STORE_CODE'):
                row[f'custom_{custom_key}'] = custom_value

        for page_data in response.get('pages', []):
            for question in page_data.get('questions', []):
                question_id = question.get('id')
                if not question_id:
                    continue

                for answer in question.get('answers', []):
                    column_key = get_column_key(question_id, answer, matrix_column_map)
                    value = get_answer_value(answer, choice_map, row_map)
                    put_answer(row, column_key, value)

        normalize_multi_answers(row)
        rows.append(row)

    all_responses = []
    df_responses = pd.DataFrame(rows)

    if df_responses.empty:
        raise RuntimeError('El DataFrame final del cuarto survey quedo vacio. No se pisa la DB.')

    rename_map = {}
    for col in df_responses.columns:
        if '__row__' in str(col):
            question_id, row_id = str(col).split('__row__', 1)
            rename_map[col] = matrix_column_map.get((question_id, row_id), col)
        else:
            rename_map[col] = question_map.get(col, col)

    df_responses.rename(columns=rename_map, inplace=True)
    df_responses.columns = make_unique_columns(df_responses.columns)

    logger.info(
        '7 - DataFrame cuarto survey listo. job_id=%s filas=%s columnas=%s',
        job_id,
        df_responses.shape[0],
        df_responses.shape[1],
    )

    with BytesIO() as output:
        df_responses.to_pickle(output)
        binary_data = output.getvalue()

    binary_size_bytes = len(binary_data)

    if binary_size_bytes <= 0:
        raise RuntimeError('El binario generado del cuarto survey esta vacio. No se pisa la DB.')

    logger.info(
        '8 - Guardando cuarto survey en DB. job_id=%s binary_size_bytes=%s',
        job_id,
        binary_size_bytes,
    )

    try:
        db.session.query(CuartoSurvey).delete()
        db.session.flush()

        new_survey = CuartoSurvey(data=binary_data)
        db.session.add(new_survey)
        db.session.commit()

    except SQLAlchemyError as e:
        db.session.rollback()
        raise RuntimeError(f'Error SQLAlchemy guardando cuarto survey: {str(e)}') from e

    elapsed_time = datetime.now() - hora_inicio

    logger.info(
        '9 - Cuarto survey guardado correctamente. job_id=%s record_id=%s filas=%s columnas=%s tiempo=%s',
        job_id,
        new_survey.id,
        df_responses.shape[0],
        df_responses.shape[1],
        str(elapsed_time),
    )

    result = {
        'record_id': new_survey.id,
        'rows': int(df_responses.shape[0]),
        'columns': int(df_responses.shape[1]),
        'binary_size_bytes': int(binary_size_bytes),
        'elapsed_time': str(elapsed_time),
    }

    gc.collect()

    return result
