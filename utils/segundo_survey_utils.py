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
from models import SegundoSurvey
from logging_config import logger

load_dotenv()

tz = pytz.timezone('America/Sao_Paulo')


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
    return re.sub(r'<[^>]*>', '', html_text)


def put_answer(target, question_id, value):
    if value is None:
        return

    if question_id not in target or target[question_id] in (None, ''):
        target[question_id] = value
        return

    existing = target[question_id]

    if isinstance(existing, list):
        existing.append(value)
        return

    target[question_id] = [existing, value]


def normalize_multi_answers(row):
    for key, value in list(row.items()):
        if isinstance(value, list):
            row[key] = ' | '.join(str(item) for item in value)


def obtener_y_guardar_survey(job_id=None):
    access_token = os.getenv('SURVEYMONKEY_ACCESS_TOKEN')
    survey_id = os.getenv('SECOND_SURVEY_ID')

    if not access_token:
        raise RuntimeError('Falta SURVEYMONKEY_ACCESS_TOKEN en variables de entorno')

    if not survey_id:
        raise RuntimeError('Falta SECOND_SURVEY_ID en variables de entorno')

    hora_inicio = datetime.now()
    logger.info("2 - Iniciando recuperacion del segundo survey. job_id=%s survey_id=%s", job_id, survey_id)

    host = 'https://api.surveymonkey.com'
    survey_responses_url = f'{host}/v3/surveys/{survey_id}/responses/bulk'
    survey_details_url = f'{host}/v3/surveys/{survey_id}/details'

    session = build_surveymonkey_session(access_token)

    logger.info("3 - Obteniendo detalles de la encuesta. job_id=%s", job_id)
    survey_details = get_json_or_raise(session, survey_details_url, context='survey details')

    choice_map = {}
    question_map = {}

    for page in survey_details.get('pages', []):
        for question in page.get('questions', []):
            question_id = question.get('id')
            headings = question.get('headings', [])
            heading = headings[0].get('heading') if headings else question_id

            if question_id:
                question_map[question_id] = heading

            answers = question.get('answers', {})
            for choice in answers.get('choices', []):
                choice_id = choice.get('id')
                choice_text = choice.get('text')
                if choice_id:
                    choice_map[choice_id] = choice_text

    logger.info(
        "4 - Mapeos cargados. job_id=%s preguntas=%s opciones=%s",
        job_id,
        len(question_map),
        len(choice_map),
    )

    logger.info("5 - Obteniendo respuestas de la encuesta. job_id=%s", job_id)

    page = 1
    per_page = 1000
    all_responses = []
    next_url = survey_responses_url
    params = {'page': page, 'per_page': per_page}

    while next_url:
        logger.info("5.%s - Bajando pagina SurveyMonkey. job_id=%s", page, job_id)

        response_json = get_json_or_raise(
            session,
            next_url,
            params=params,
            context=f'responses bulk page {page}',
        )

        responses_page = response_json.get('data', [])

        if not isinstance(responses_page, list):
            raise RuntimeError(f'Formato inesperado en pagina {page}: data no es lista')

        logger.info(
            "5.%s - Pagina recibida. job_id=%s respuestas=%s acumuladas=%s",
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
        raise RuntimeError('SurveyMonkey no devolvio respuestas. No se pisa la DB con un dataset vacio.')

    logger.info("6 - Procesando respuestas. job_id=%s total_respuestas=%s", job_id, len(all_responses))

    responses_dict = {}

    for response in all_responses:
        respondent_id = response.get('id')

        if not respondent_id:
            continue

        if respondent_id not in responses_dict:
            responses_dict[respondent_id] = {}

        row = responses_dict[respondent_id]
        row['response_id'] = respondent_id

        custom_vars = response.get('custom_variables', {})
        row['custom_variables'] = custom_vars.get('ID_CODE', '')
        row['STORE_CODE'] = custom_vars.get('STORE_CODE', '')
        row['date_created'] = response.get('date_created', '')[:10]

        for page_data in response.get('pages', []):
            for question in page_data.get('questions', []):
                question_id = question.get('id')

                if not question_id:
                    continue

                for answer in question.get('answers', []):
                    value = None

                    if 'choice_id' in answer:
                        value = choice_map.get(answer.get('choice_id'), answer.get('choice_id'))
                    elif 'text' in answer:
                        value = answer.get('text')
                    elif 'row_id' in answer:
                        value = answer.get('row_id')

                    put_answer(row, question_id, value)

        normalize_multi_answers(row)

    df_responses = pd.DataFrame.from_dict(responses_dict, orient='index')
    all_responses = []

    if df_responses.empty:
        raise RuntimeError('El DataFrame final quedo vacio. No se pisa la DB.')

    if '152421787' in df_responses.columns:
        df_responses['152421787'] = df_responses['152421787'].apply(extract_text_from_html)

    df_responses.rename(columns=question_map, inplace=True)
    df_responses.columns = [extract_text_from_html(col) for col in df_responses.columns]

    logger.info(
        "7 - DataFrame listo. job_id=%s filas=%s columnas=%s",
        job_id,
        df_responses.shape[0],
        df_responses.shape[1],
    )

    with BytesIO() as output:
        df_responses.to_pickle(output)
        binary_data = output.getvalue()

    binary_size_bytes = len(binary_data)

    if binary_size_bytes <= 0:
        raise RuntimeError('El binario generado esta vacio. No se pisa la DB.')

    logger.info(
        "8 - Guardando segundo survey en DB. job_id=%s binary_size_bytes=%s",
        job_id,
        binary_size_bytes,
    )

    try:
        db.session.query(SegundoSurvey).delete()
        db.session.flush()

        new_survey = SegundoSurvey(data=binary_data)
        db.session.add(new_survey)
        db.session.commit()

    except SQLAlchemyError as e:
        db.session.rollback()
        raise RuntimeError(f'Error SQLAlchemy guardando segundo survey: {str(e)}') from e

    elapsed_time = datetime.now() - hora_inicio

    logger.info(
        "9 - Segundo survey guardado correctamente. job_id=%s record_id=%s filas=%s columnas=%s tiempo=%s",
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