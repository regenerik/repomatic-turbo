from flask import Blueprint, send_file, make_response, request, jsonify, render_template, current_app, Response # Blueprint para modularizar y relacionar con app
from flask_bcrypt import Bcrypt                                  # Bcrypt para encriptación
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity   # Jwt para tokens
from database import db                                          # importa la db desde database.py
from logging_config import logger
import os                                                        # Para datos .env
from dotenv import load_dotenv                                   # Para datos .env
load_dotenv()
from utils.data_mentor_utils import query_assistant_mentor
import urllib.request
import urllib.error
import json
import pandas as pd
from models import Usuarios_Por_Asignacion, Usuarios_Sin_ID, ValidaUsuarios,DetalleApies, AvanceCursada, DetallesDeCursos, CursadasAgrupadas,FormularioGestor,CuartoSurveySql, QuintoSurveySql, Comentarios2023, Comentarios2024, Comentarios2025, BaseLoopEstaciones, FichasGoogleCompetencia, FichasGoogle, SalesForce, ComentariosCompetencia, FileDailyID
import hashlib
from sqlalchemy.exc import SQLAlchemyError
import csv
import time
import tempfile
from openai import OpenAI
import httpx
from tempfile import NamedTemporaryFile



OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("Debes definir la variable de entorno OPENAI_API_KEY con tu clave de API.")

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {OPENAI_API_KEY}",
    "OpenAI-Beta": "assistants=v2"
}


data_mentor_bp = Blueprint('data_mentor_bp', __name__)     # instanciar admin_bp desde clase Blueprint para crear las rutas.
bcrypt = Bcrypt()
jwt = JWTManager()

# Sistema de key base pre rutas ------------------------:

API_KEY = os.getenv('API_KEY')

def check_api_key(api_key):
    return api_key == API_KEY

@data_mentor_bp.before_request
def authorize():
    if request.method == 'OPTIONS':
        return
    if request.path in ['/horas-por-curso','/test_data_mentor_bp']:
        return
    api_key = request.headers.get('Authorization')
    if not api_key or not check_api_key(api_key):
        return jsonify({'message': 'Unauthorized'}), 401
    
# RUTA TEST:

@data_mentor_bp.route('/test_data_mentor_bp', methods=['GET'])
def test():
    logger.info("Chat data mentor bp rutas funcionando ok segun test.")
    return jsonify({'message': 'test bien sucedido','status':"Si lees esto, chat data mentor rutas funcionan bien..."}),200

@data_mentor_bp.route("/chat_mentor", methods=["POST"])
def chat_mentor():
    logger.info("1 - Entró en la ruta Chat_mentor.")
    """
    Recibe prompt y opcionalmente thread_id.
    """
    data = request.get_json()
    if not data or "prompt" not in data:
        return jsonify({"error": "Falta el prompt en el cuerpo de la solicitud"}), 400

    prompt = data["prompt"]
    thread_id = data.get("thread_id")  # puede ser None
    logger.info("2 - Encontró la data del prompt...")
    try:
        response_text, current_thread = query_assistant_mentor(prompt, thread_id)
        return jsonify({"response": response_text, "thread_id": current_thread}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

@data_mentor_bp.route("/close_chat_mentor", methods=["POST"])
def close_chat():
    """
    Ruta para cerrar el thread del chat.
    Se espera recibir un JSON con la clave "thread_id".
    Llama al endpoint DELETE de la API para cerrar el hilo usando urllib.
    """
    data = request.get_json()
    if not data or "thread_id" not in data:
        return jsonify({"error": "Falta el thread_id en el cuerpo de la solicitud"}), 400

    thread_id = data["thread_id"]
    delete_url = f"https://api.openai.com/v1/threads/{thread_id}"

    try:
        req = urllib.request.Request(delete_url, headers=HEADERS, method="DELETE")
        with urllib.request.urlopen(req) as response:
            result_data = response.read().decode("utf-8")
            result = json.loads(result_data)
        return jsonify(result), 200
    except urllib.error.HTTPError as e:
        error_message = e.read().decode("utf-8")
        return jsonify({"error": f"HTTPError {e.code}: {error_message}"}), e.code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@data_mentor_bp.route('/horas-por-curso', methods=['GET'])
def horas_por_curso():
    data = [
        {"curso": "Node.js Básico", "horas": 5},
        {"curso": "React Intermedio", "horas": 7},
        {"curso": "Flask Fullstack", "horas": 9}
    ]
    return jsonify(data)

# -------------------------- MODELOS QUE SE TIENEN EN CUENTA PARA CONTABILIZAR SUS REGISTROS ----------------------------------

# Diccionario para mapear nombres de string a clases reales
MODELS = {
    'Usuarios_Por_Asignacion': Usuarios_Por_Asignacion,
    'Usuarios_Sin_ID': Usuarios_Sin_ID,
    'ValidaUsuarios': ValidaUsuarios,
    'DetalleApies' : DetalleApies,
    'AvanceCursada': AvanceCursada,
    'DetallesDeCursos' : DetallesDeCursos,
    'CursadasAgrupadas' : CursadasAgrupadas,
    'FormularioGestor' :FormularioGestor,
    'CuartoSurveySql': CuartoSurveySql,
    'QuintoSurveySql' : QuintoSurveySql,
    'Comentarios2023': Comentarios2023,
    'Comentarios2024': Comentarios2024,
    'Comentarios2025': Comentarios2025,
    'BaseLoopEstaciones' : BaseLoopEstaciones,
    'FichasGoogleCompetencia' : FichasGoogleCompetencia,
    'FichasGoogle' : FichasGoogle,
    'SalesForce' : SalesForce,
    'ComentariosCompetencia' : ComentariosCompetencia
    # Agregá los modelos que quieras habilitar acá
}

# -------------------------- Contabilizar longitud de cualquier tabla ----------------------------------

@data_mentor_bp.route('/contar-registros', methods=['POST'])
def contar_registros():
    data = request.get_json()
    nombre_tabla = data.get('tabla')

    if not nombre_tabla:
        return jsonify({"error": "Falta el nombre de la tabla"}), 400

    modelo = MODELS.get(nombre_tabla)
    if not modelo:
        return jsonify({"error": f"La tabla '{nombre_tabla}' no está habilitada o no existe"}), 404

    try:
        cantidad = db.session.query(modelo).count()
        return jsonify({"tabla": nombre_tabla, "cantidad_registros": cantidad}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# -------------------------- ACA VIENEN LAS RUTAS DE LAS TABLAS DE REPORTES ----------------------------


@data_mentor_bp.route('/usuarios_por_asignacion/<int:registro_id>', methods=['GET'])
def get_usuario_por_asignacion(registro_id):
    """
    Devuelve el registro de Usuarios_Por_Asignacion con el id dado,
    usando el método serialize() del modelo.
    """
    logger.info(f"Buscando Usuarios_Por_Asignacion id={registro_id}")
    registro = Usuarios_Por_Asignacion.query.get(registro_id)

    if not registro:
        logger.warning(f"Usuarios_Por_Asignacion id={registro_id} no encontrado")
        return jsonify({
            'error': 'Registro no encontrado',
            'status': 404
        }), 404

    logger.info(f"Registro encontrado: {registro}")
    return jsonify(registro.serialize()), 200

@data_mentor_bp.route('/usuarios_sin_id/<int:registro_id>', methods=['GET'])
def get_usuario_sin_id(registro_id):
    """
    Devuelve el registro de Usuarios_Sin_ID con el id dado,
    usando el método serialize() del modelo.
    """
    logger.info(f"Buscando Usuarios_Sin_ID id={registro_id}")
    registro = Usuarios_Sin_ID.query.get(registro_id)

    if not registro:
        logger.warning(f"Usuarios_Sin_ID id={registro_id} no encontrado")
        return jsonify({
            'error': 'Registro no encontrado',
            'status': 404
        }), 404

    logger.info(f"Registro encontrado: {registro}")
    return jsonify(registro.serialize()), 200




# RUTAS PARA CARGAR TABLAS DE EXPERIENCIA 2023 24 y 25

@data_mentor_bp.route('/cargar_comentarios_2023', methods=['POST'])
def cargar_comentarios_encuesta_2023():
    """
    Recibe un archivo .xlsx vía form-data (campo: 'file') y guarda sus registros en la DB
    """
    archivo = request.files.get('file')
    if not archivo:
        return jsonify({'error': 'No se envió ningún archivo', 'status': 400}), 400

    try:
        df = pd.read_excel(archivo)

        registros = []
        for _, fila in df.iterrows():
            fecha_raw = fila.get('FECHA')
            try:
                fecha = pd.to_datetime(fecha_raw) if pd.notnull(fecha_raw) else None
            except:
                fecha = None

            nuevo = Comentarios2023(
                fecha=fecha,
                apies=str(fila.get('APIES', '')).strip(),
                comentario=str(fila.get('COMENTARIO', '')).strip(),
                canal=str(fila.get('CANAL', '')).strip(),
                topico=str(fila.get('TÓPICO', '')).strip(),
                sentiment=str(fila.get('SENTIMENT', '')).strip()
            )
            registros.append(nuevo)

        db.session.add_all(registros)
        db.session.commit()

        return jsonify({'mensaje': f'Se guardaron {len(registros)} comentarios', 'status': 200}), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': f'Error al procesar el archivo: {str(e)}', 'status': 500}), 500
    
@data_mentor_bp.route('/cargar_comentarios_2024', methods=['POST'])
def cargar_comentarios_encuesta_2024():
    """
    Recibe un archivo .xlsx vía form-data (campo: 'file') y guarda sus registros en la DB
    """
    archivo = request.files.get('file')
    if not archivo:
        return jsonify({'error': 'No se envió ningún archivo', 'status': 400}), 400

    try:
        df = pd.read_excel(archivo)

        registros = []
        for _, fila in df.iterrows():
            fecha_raw = fila.get('FECHA')
            try:
                fecha = pd.to_datetime(fecha_raw) if pd.notnull(fecha_raw) else None
            except:
                fecha = None

            nuevo = Comentarios2024(
                fecha=fecha,
                apies=str(fila.get('APIES', '')).strip(),
                comentario=str(fila.get('COMENTARIO', '')).strip(),
                canal=str(fila.get('CANAL', '')).strip(),
                topico=str(fila.get('TÓPICO', '')).strip(),
                sentiment=str(fila.get('SENTIMENT', '')).strip()
            )
            registros.append(nuevo)

        db.session.add_all(registros)
        db.session.commit()

        return jsonify({'mensaje': f'Se guardaron {len(registros)} comentarios', 'status': 200}), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': f'Error al procesar el archivo: {str(e)}', 'status': 500}), 500
    
@data_mentor_bp.route('/cargar_comentarios_2025', methods=['POST'])
def cargar_comentarios_encuesta_2025():
    """
    Carga masiva de comentarios desde archivo .xlsx.
    Detecta duplicados por hash_id (tanto en DB como en el archivo actual)
    y usa bulk_save_objects para velocidad.
    """
    archivo = request.files.get('file')
    if not archivo:
        return jsonify({'error': 'No se envió ningún archivo', 'status': 400}), 400

    try:
        df = pd.read_excel(archivo)

        # Paso 1: Preparamos todos los registros con hash,
        #         filtrando duplicados DENTRO del archivo actual
        records_from_file = []
        # Usamos un set para rastrear los hashes ya vistos en ESTE ARCHIVO
        unique_hashes_in_current_file = set()

        for _, fila in df.iterrows():
            fecha_raw = fila.get('FECHA')
            try:
                fecha = pd.to_datetime(fecha_raw) if pd.notnull(fecha_raw) else None
            except Exception as e:
                logger.warning(f"Error al parsear fecha '{fecha_raw}': {e}. Asignando None.")
                fecha = None

            apies = str(fila.get('APIES', '')).strip()
            comentario = str(fila.get('COMENTARIO', '')).strip()
            canal = str(fila.get('CANAL', '')).strip()
            topico = str(fila.get('TÓPICO', '')).strip()
            sentiment = str(fila.get('SENTIMENT', '')).strip()

            # Generar hash único para este registro
            hash_id = Comentarios2025.generar_hash(fecha, apies, comentario, canal)

            # Verificar si este hash_id ya fue visto en el archivo ACTUAL
            if hash_id in unique_hashes_in_current_file:
                logger.info(f"Saltando registro duplicado DENTRO DEL ARCHIVO con hash_id: {hash_id}")
                continue # Saltar esta fila, ya la procesamos o es un duplicado interno

            unique_hashes_in_current_file.add(hash_id) # Registrar este hash_id como visto

            comentario_obj = Comentarios2025(
                fecha=fecha,
                apies=apies,
                comentario=comentario,
                canal=canal,
                topico=topico,
                sentiment=sentiment,
                hash_id=hash_id
            )
            records_from_file.append(comentario_obj)

        # Paso 2: Buscar cuáles de los hashes únicos del archivo ya existen en la base de datos
        all_unique_hashes_from_file = [r.hash_id for r in records_from_file]

        existentes_en_db = set(
            r[0] for r in db.session.query(Comentarios2025.hash_id)
            .filter(Comentarios2025.hash_id.in_(all_unique_hashes_from_file))
            .all()
        )

        # Paso 3: Filtrar duplicados que ya existen en la DB
        nuevos = [c for c in records_from_file if c.hash_id not in existentes_en_db]

        # Paso 4: Insertar de forma masiva
        if nuevos:
            db.session.bulk_save_objects(nuevos)
            db.session.commit()

        return jsonify({
            'mensaje': f'Se guardaron {len(nuevos)} comentarios nuevos',
            'duplicados_ignorados_en_archivo': len(df) - len(records_from_file), # Nuevos: cuántos se descartaron por ser duplicados en el archivo
            'duplicados_ignorados_en_db': len(records_from_file) - len(nuevos),  # Cuántos se descartaron por ya estar en la DB
            'status': 200
        }), 200

    except Exception as e:
        db.session.rollback()
        logger.error(f"Error al procesar el archivo de comentarios 2025: {e}", exc_info=True)
        return jsonify({'error': f'Error al procesar el archivo: {str(e)}', 'status': 500}), 500
    
@data_mentor_bp.route('/cargar_base_loop', methods=['POST'])
def cargar_base_loop():
    archivo = request.files.get('file')
    if not archivo:
        return jsonify({"error": "No se envió ningún archivo"}), 400

    try:
        # Detectar delimitador (coma o punto y coma)
        sample = archivo.read(2048).decode('utf-8')
        archivo.seek(0)
        delimiter = csv.Sniffer().sniff(sample).delimiter

        df = pd.read_csv(archivo, sep=delimiter, encoding='utf-8', on_bad_lines='skip')
    except Exception as e:
        return jsonify({"error": f"Error al leer el archivo CSV: {e}"}), 400

    try:
        columnas_db = {col.name: col.key for col in BaseLoopEstaciones.__table__.columns}

        registros = []
        for _, fila in df.iterrows():
            datos_instancia = {}
            for nombre_col_csv, valor in fila.items():
                if nombre_col_csv in columnas_db:
                    campo_para_modelo = columnas_db[nombre_col_csv]
                    datos_instancia[campo_para_modelo] = valor

            # Instanciamos así para evitar el error de keyword inválido
            instancia = BaseLoopEstaciones()
            for key, val in datos_instancia.items():
                setattr(instancia, key, val)
            registros.append(instancia)

        db.session.bulk_save_objects(registros)
        db.session.commit()

        return jsonify({"mensaje": f"{len(registros)} registros insertados correctamente"}), 200

    except SQLAlchemyError as e:
        db.session.rollback()
        return jsonify({"error": f"Error al insertar en la base de datos: {e}"}), 500
    except Exception as e:
        return jsonify({"error": f"Error al procesar el archivo: {e}"}), 500
    
@data_mentor_bp.route('/cargar_fichas_google_competencia', methods=['POST'])
def cargar_fichas_google_competencia():
    archivo = request.files.get('file')
    if not archivo:
        return jsonify({'error': 'No se envió ningún archivo', 'status': 400}), 400

    try:
        df = pd.read_excel(archivo)

        candidatos = []
        hash_ids = []
        hash_set_memoria = set()

        for _, fila in df.iterrows():
            id_loop = str(fila.get('idLoop', '')).strip()
            total_review_count = str(fila.get('totalReviewCount', '')).strip()
            average_rating = str(fila.get('averageRating', '')).strip()

            # Saltear filas con campos vacíos importantes
            if not id_loop or not total_review_count or not average_rating:
                continue

            hash_id = FichasGoogleCompetencia.generar_hash(id_loop, total_review_count, average_rating)

            # Saltear si ya está en memoria (repetido en el mismo archivo)
            if hash_id in hash_set_memoria:
                continue
            hash_set_memoria.add(hash_id)

            ficha_obj = FichasGoogleCompetencia(
                id_loop=id_loop,
                total_review_count=total_review_count,
                average_rating=average_rating,
                hash_id=hash_id
            )

            candidatos.append(ficha_obj)
            hash_ids.append(hash_id)

        # Buscar duplicados ya existentes en la DB
        existentes = set(
            r[0] for r in db.session.query(FichasGoogleCompetencia.hash_id)
            .filter(FichasGoogleCompetencia.hash_id.in_(hash_ids))
            .all()
        )

        nuevos = [f for f in candidatos if f.hash_id not in existentes]

        if nuevos:
            db.session.bulk_save_objects(nuevos)
            db.session.commit()

        return jsonify({
            'mensaje': f'Se guardaron {len(nuevos)} fichas nuevas',
            'preexistentes_ignorados': len(candidatos) - len(nuevos),
            'status': 200
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': f'Error al procesar el archivo: {str(e)}', 'status': 500}), 500

@data_mentor_bp.route('/cargar_fichas_google', methods=['POST'])
def cargar_fichas_google():
    archivo = request.files.get('file')
    if not archivo:
        return jsonify({'error': 'No se envió ningún archivo', 'status': 400}), 400

    try:
        df = pd.read_excel(archivo)

        candidatos = []
        hash_ids = []
        hash_set_memoria = set()

        for _, fila in df.iterrows():
            store_code = str(fila.get('Store Code', '')).strip()
            cantidad_de_calificaciones = str(fila.get('Cantidad de calificaciones', '')).strip()
            start_rating = str(fila.get('Star Rating', '')).strip()

            # Saltear filas con campos vacíos importantes
            if not store_code or not cantidad_de_calificaciones or not start_rating:
                continue

            hash_id = FichasGoogle.generar_hash(store_code, cantidad_de_calificaciones, start_rating)

            # Saltear si ya está en memoria (repetido en el mismo archivo)
            if hash_id in hash_set_memoria:
                continue
            hash_set_memoria.add(hash_id)

            ficha_obj = FichasGoogle(
                store_code=store_code,
                cantidad_de_calificaciones=cantidad_de_calificaciones,
                start_rating=start_rating,
                hash_id=hash_id
            )

            candidatos.append(ficha_obj)
            hash_ids.append(hash_id)

        # Buscar duplicados ya existentes en la DB
        existentes = set(
            r[0] for r in db.session.query(FichasGoogle.hash_id)
            .filter(FichasGoogle.hash_id.in_(hash_ids))
            .all()
        )

        nuevos = [f for f in candidatos if f.hash_id not in existentes]

        if nuevos:
            db.session.bulk_save_objects(nuevos)
            db.session.commit()

        return jsonify({
            'mensaje': f'Se guardaron {len(nuevos)} fichas nuevas',
            'preexistentes_ignorados': len(candidatos) - len(nuevos),
            'status': 200
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': f'Error al procesar el archivo: {str(e)}', 'status': 500}), 500
    
@data_mentor_bp.route('/cargar_salesforce', methods=['POST'])
def cargar_salesforce():
    archivo = request.files.get('file')
    if not archivo:
        return jsonify({'error': 'No se envió ningún archivo', 'status': 400}), 400

    try:
        df = pd.read_excel(archivo)

        candidatos = []
        hash_ids = []
        hash_set_memoria = set()

        for _, fila in df.iterrows():
            valores = [
                fila.get('Estacion de Servicio: Zona', ''),
                fila.get('Número del caso', ''),
                fila.get('Estado', ''),
                fila.get('Tipificación Caso', ''),
                fila.get('Asunto', ''),
                fila.get('Fecha/Hora de apertura', ''),
                fila.get('Cantidad de Reclamos', ''),
                fila.get('Defensa al Consumidor', ''),
                fila.get('GGRR/COLA Asignado', ''),
                fila.get('Propietario del caso: Nombre completo', ''),
                fila.get('Descripción', ''),
                fila.get('Nombre del contacto: Nombre completo', ''),
                fila.get('Comentarios', ''),
                fila.get('Estacion de Servicio: Razón Social', ''),
                fila.get('Estacion de Servicio: Red', ''),
                fila.get('Estacion de Servicio: Regional', ''),
            ]

            hash_id = SalesForce.generar_hash(*valores)

            if hash_id in hash_set_memoria:
                continue
            hash_set_memoria.add(hash_id)

            registro = SalesForce(
                estacion_servicio_zona=valores[0],
                numero_de_caso=valores[1],
                estado=valores[2],
                tipificacion_caso=valores[3],
                asunto=valores[4],
                fecha_apertura=valores[5],
                cantidad_reclamos=valores[6],
                defensa_consumidor=valores[7],
                ggrr_cola_asignado=valores[8],
                propietario_nombre=valores[9],
                descripcion=valores[10],
                contacto_nombre=valores[11],
                comentarios=valores[12],
                razon_social=valores[13],
                red=valores[14],
                regional=valores[15],
                hash_id=hash_id
            )

            candidatos.append(registro)
            hash_ids.append(hash_id)

        existentes = set(
            r[0] for r in db.session.query(SalesForce.hash_id)
            .filter(SalesForce.hash_id.in_(hash_ids))
            .all()
        )

        nuevos = [r for r in candidatos if r.hash_id not in existentes]

        if nuevos:
            db.session.bulk_save_objects(nuevos)
            db.session.commit()

        return jsonify({
            'mensaje': f'Se guardaron {len(nuevos)} casos nuevos',
            'preexistentes_ignorados': len(candidatos) - len(nuevos),
            'status': 200
        }), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': f'Error al procesar el archivo: {str(e)}', 'status': 500}), 500
    
@data_mentor_bp.route('/cargar_comentarios_competencia', methods=['POST'])
def cargar_comentarios_competencia():
    archivo = request.files.get('file')
    if not archivo:
        return jsonify({'error': 'No se recibió ningún archivo'}), 400

    try:
        df = pd.read_excel(archivo)

        # Normalizar la fecha
        if 'FECHA' in df.columns:
            df['FECHA'] = df['FECHA'].astype(str)


        # Renombrar columna ID si existe
        if 'ID' in df.columns:
            df.rename(columns={'ID': 'ID_ORIGINAL'}, inplace=True)

        # Reemplazamos espacios y ponemos mayúsculas para asegurar
        df.columns = [col.upper().replace(" ", "_") for col in df.columns]

        nuevos = []
        for _, fila in df.iterrows():
            hash_id = ComentariosCompetencia.generar_hash(
                fila.get('ID_ORIGINAL', ''),
                fila.get('FECHA', ''),
                fila.get('IDLOOP', ''),
                fila.get('COMENTARIO', ''),
                fila.get('RATING', ''),
                fila.get('SENTIMIENTO', ''),
                fila.get('TÓPICO', '')
            )

            # Chequeamos si ya existe
            if not ComentariosCompetencia.query.filter_by(hash_id=hash_id).first():
                nuevo = ComentariosCompetencia(
                    id_original=fila.get('ID_ORIGINAL'),
                    fecha=fila.get('FECHA'),
                    id_loop=fila.get('IDLOOP'),
                    comentario=fila.get('COMENTARIO'),
                    rating=fila.get('RATING'),
                    sentimiento=fila.get('SENTIMIENTO'),
                    topico=fila.get('TÓPICO'),
                    hash_id=hash_id
                )
                nuevos.append(nuevo)

        db.session.bulk_save_objects(nuevos)
        db.session.commit()

        return jsonify({'guardados': len(nuevos)})

    except Exception as e:
        return jsonify({'error': f'Error al procesar el archivo: {str(e)}'}), 500
    


# LA COMPILACION Y SUBIDA DEL JSON DE TODA LA DATA>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>


@data_mentor_bp.route("/actualizar-archivos-asistente", methods=["POST"])
def actualizar_archivos_asistente():
    tmpfile_path = None
    try:
        logger.info("Iniciando el proceso de actualización del archivo de conocimiento diario para OpenAI.")
        logger.info("Recopilando datos de todas las tablas...")

        # --- 1. Recopilar datos de todas las tablas ---
        comentarios_2025 = Comentarios2025.query.all()
        fichas_google = FichasGoogle.query.all()
        fichas_google_competencia = FichasGoogleCompetencia.query.all()
        usuarios_por_asignacion = Usuarios_Por_Asignacion.query.all()
        usuarios_sin_id = Usuarios_Sin_ID.query.all()
        valida_usuarios = ValidaUsuarios.query.all()
        detalle_apies = DetalleApies.query.all()
        avance_cursada = AvanceCursada.query.all()
        detalles_de_cursos = DetallesDeCursos.query.all()
        cursadas_agrupadas = CursadasAgrupadas.query.all()
        formulario_gestor = FormularioGestor.query.all()
        cuarto_survey_sql = CuartoSurveySql.query.all()
        quinto_survey_sql = QuintoSurveySql.query.all()
        comentarios_2023 = Comentarios2023.query.all()
        comentarios_2024 = Comentarios2024.query.all()
        base_loop_estaciones = BaseLoopEstaciones.query.all()
        sales_force = SalesForce.query.all()
        comentarios_competencia = ComentariosCompetencia.query.all()

        # --- Creación del diccionario JSON con la guía de uso y los datos ---
        data_json = {
            "guia_de_uso_de_datos": {
                "descripcion_general": "Este archivo contiene una base de conocimiento integral sobre las operaciones, experiencia del cliente y aprendizaje comercial de nuestra empresa. El objetivo es proporcionar información detallada para análisis, resolución de consultas y comparación con la competencia. La tabla 'base_loop_estaciones' es central para la mayoría de las relaciones.",
                "secciones_disponibles": {
                    "base_loop_estaciones": {
                        "descripcion": "Tabla PRINCIPAL. Contiene el detalle exhaustivo de cada estación de servicio. Incluye información operativa (volúmenes de venta, tipo de establecimiento, dotación, etc.), geográfica y administrativa. Sus campos 'APIES' e 'Id' son claves para relacionarla con otras secciones.",
                        "relaciones_clave": {
                            "BaseLoopEstaciones.APIES": "Se relaciona con 'comentarios_2023.APIES', 'comentarios_2024.APIES', 'comentarios_2025.APIES'.",
                            "BaseLoopEstaciones.Id": "Se relaciona con 'fichas_google.Store_Code', 'fichas_google_competencia.Idloop', 'comentarios_competencia.Idloop', 'usuarios_por_asignacion.ID_Pertenencia'."
                        },
                        "ejemplo_consulta": "Para la estación con ID 1234, ¿cuál es su volumen promedio de Nafta y qué comentarios de clientes tiene de 2025?"
                    },
                    "comentarios_2023": {
                        "descripcion": "Comentarios de encuestas de clientes recibidos en 2023. Incluye 'fecha', 'apies' (ID de estación), 'comentario' (texto libre), 'canal', 'topico', 'sentiment'.",
                        "relaciones_clave": "Relacionado con 'base_loop_estaciones' mediante 'APIES'.",
                        "ejemplo_consulta": "¿Qué comentarios positivos hubo en la estación 5678 en 2023 sobre la atención?"
                    },
                    "comentarios_2024": {
                        "descripcion": "Comentarios de encuestas de clientes recibidos en 2024. Formato y campos similares a 2023.",
                        "relaciones_clave": "Relacionado con 'base_loop_estaciones' mediante 'APIES'.",
                        "ejemplo_consulta": "Dame los tópicos más frecuentes en los comentarios negativos de 2024 para la región 'Norte'."
                    },
                    "comentarios_2025": {
                        "descripcion": "Comentarios de encuestas de clientes recibidos en 2025. Formato y campos similares a 2023 y 2024.",
                        "relaciones_clave": "Relacionado con 'base_loop_estaciones' mediante 'APIES'.",
                        "ejemplo_consulta": "¿Cuáles son los comentarios recientes (2025) sobre el 'precio' en estaciones de Capital Federal?"
                    },
                    "fichas_google": {
                        "descripcion": "Datos de nuestras fichas de Google (reseñas, valoraciones, información de la estación). Contiene 'Store_Code' que es el ID de la estación.",
                        "relaciones_clave": "Relacionado con 'base_loop_estaciones' mediante 'Store_Code' (que es igual a BaseLoopEstaciones.Id).",
                        "ejemplo_consulta": "¿Cuál es la valoración promedio de las fichas de Google para las estaciones de Buenos Aires?"
                    },
                    "fichas_google_competencia": {
                        "descripcion": "Datos de fichas de Google de la competencia. Permite analizar y comparar métricas y comentarios de nuestros rivales. Contiene 'Idloop' que es el ID de la estación asociada.",
                        "relaciones_clave": "Relacionado con 'base_loop_estaciones' mediante 'Idloop' (que es igual a BaseLoopEstaciones.Id).",
                        "ejemplo_consulta": "¿Qué comentarios negativos hay en las fichas de Google de la competencia sobre la 'velocidad de servicio'?"
                    },
                    "comentarios_competencia": {
                        "descripcion": "**¡ATENCIÓN!** Esta sección contiene **comentarios textuales de clientes específicamente sobre nuestros competidores.** Busca aquí para analizar el tipo de feedback que reciben nuestros rivales en temas como precio, atención, calidad de producto, etc. Los campos incluyen 'competidor', 'comentario', 'sentimiento'.",
                        "relaciones_clave": "Relacionado con 'base_loop_estaciones' mediante 'Idloop' (que es igual a BaseLoopEstaciones.Id).",
                        "ejemplo_consulta": "Dame los comentarios negativos de la competencia sobre el precio en el último mes."
                    },
                    "usuarios_por_asignacion": {
                        "descripcion": "Detalles sobre la asignación de usuarios a estaciones. 'ID_Pertenencia' corresponde al ID de la estación en BaseLoopEstaciones.",
                        "relaciones_clave": "Relacionado con 'base_loop_estaciones' mediante 'ID_Pertenencia' (que es igual a BaseLoopEstaciones.Id).",
                        "ejemplo_consulta": "¿Cuántos usuarios están asignados a la estación con ID 1234 y cuál es su tipo de operador?"
                    },
                    "usuarios_sin_id": {
                        "descripcion": "Información sobre usuarios que no tienen un ID de sistema asignado."
                    },
                    "valida_usuarios": {
                        "descripcion": "Datos utilizados para la validación de usuarios."
                    },
                    "detalle_apies": {
                        "descripcion": "Detalle de identificadores de APIES."
                    },
                    "avance_cursada": {
                        "descripcion": "Seguimiento del progreso de los usuarios en cursos específicos. Contiene 'ID_Usuario' y 'ID_Curso'."
                    },
                    "detalles_de_cursos": {
                        "descripcion": "Información detallada sobre los cursos disponibles, como nombre del curso, duración, etc. 'ID_Curso' es la clave."
                    },
                    "cursadas_agrupadas": {
                        "descripcion": "Resumen o agrupación de datos de cursadas."
                    },
                    "formulario_gestor": {
                        "descripcion": "Datos recopilados de formularios gestionados."
                    },
                    "cuarto_survey_sql": {
                        "descripcion": "Resultados de la Cuarta Encuesta SQL."
                    },
                    "quinto_survey_sql": {
                        "descripcion": "Resultados de la Quinta Encuesta SQL."
                    },
                    "sales_force": {
                        "descripcion": "Datos provenientes de SalesForce, relacionados con ventas o gestión de relaciones con clientes."
                    }
                },
                "instrucciones_especificas_para_ia": "Cuando un usuario haga una pregunta, primero identifica la sección más relevante en este documento. Si la pregunta requiere combinar información de diferentes secciones (ej. 'comentarios' con 'base_loop_estaciones'), utiliza las 'relaciones_clave' indicadas en cada sección para entender cómo se vinculan. Por ejemplo, para obtener comentarios de una estación específica, usa el campo 'APIES' de los comentarios y de 'base_loop_estaciones'. Siempre correlaciona la pregunta del usuario con la sección del JSON que contenga la información más probable. Si la información no está disponible en una sección o en la mezcla de dos o mas secciones por medio de joins de tablas, indícalo claramente. Proporciona respuestas claras, concisas y directas, citando la sección del documento de donde proviene la información si es necesario."
            },
            "comentarios_2025": [c.serialize() for c in comentarios_2025],
            "fichas_google": [f.serialize() for f in fichas_google],
            "fichas_google_competencia": [f.serialize() for f in fichas_google_competencia],
            "usuarios_por_asignacion": [u.serialize() for u in usuarios_por_asignacion],
            "usuarios_sin_id": [u.serialize() for u in usuarios_sin_id],
            "valida_usuarios": [v.serialize() for v in valida_usuarios],
            "detalle_apies": [d.serialize() for d in detalle_apies],
            "avance_cursada": [a.serialize() for a in avance_cursada],
            "detalles_de_cursos": [d.serialize() for d in detalles_de_cursos],
            "cursadas_agrupadas": [c.serialize() for c in cursadas_agrupadas],
            "formulario_gestor": [f.serialize() for f in formulario_gestor],
            "cuarto_survey_sql": [c.serialize() for c in cuarto_survey_sql],
            "quinto_survey_sql": [q.serialize() for q in quinto_survey_sql],
            "comentarios_2023": [c.serialize() for c in comentarios_2023],
            "comentarios_2024": [c.serialize() for c in comentarios_2024],
            "base_loop_estaciones": [b.serialize() for b in base_loop_estaciones],
            "sales_force": [s.serialize() for s in sales_force],
            "comentarios_competencia": [c.serialize() for c in comentarios_competencia]
        }

        # Crear un archivo JSON temporal para escribir los datos
        with NamedTemporaryFile(mode="w+", delete=False, suffix=".json", encoding="utf-8") as tmpfile:
            json.dump(data_json, tmpfile, indent=2, ensure_ascii=False)
            tmpfile.flush()
            tmpfile_path = tmpfile.name
        
        file_size = os.path.getsize(tmpfile_path) / (1024 * 1024)
        logger.info(f"Tamaño final del archivo JSON temporal: {file_size:.2f} MB")

        # --- 2. Subir el nuevo archivo JSON a OpenAI ---
        logger.info("Subiendo el nuevo archivo JSON a OpenAI...")
        with open(tmpfile_path, "rb") as file_to_upload:
            uploaded_file = client.files.create(
                file=file_to_upload,
                purpose="assistants"
            )
        new_file_id = uploaded_file.id
        logger.info(f"Nuevo archivo JSON subido con éxito. File ID: {new_file_id}")

        # --- 3. Eliminar archivo existente previamente de OpenAI (si lo hay) ---
        existing_file_record = FileDailyID.query.first()

        if existing_file_record:
            old_file_id = existing_file_record.current_file_id
            logger.info(f"Se encontró un archivo antiguo para eliminar con ID: {old_file_id}")
            try:
                client.files.delete(old_file_id)
                logger.info(f"Archivo antiguo '{old_file_id}' eliminado exitosamente de OpenAI.")
            except Exception as e:
                logger.warning(f"No se pudo eliminar el archivo antiguo '{old_file_id}' de OpenAI. Causa: {e}")
            
            existing_file_record.current_file_id = new_file_id
            db.session.add(existing_file_record)
            db.session.commit()
            logger.info(f"ID de archivo actualizado en la base de datos a: {new_file_id}")
        else:
            logger.info("No se encontró un archivo anterior registrado en la base de datos.")
            new_record = FileDailyID(current_file_id=new_file_id)
            db.session.add(new_record)
            db.session.commit()
            logger.info(f"Nuevo registro de ID de archivo creado en la base de datos: {new_file_id}")

        return jsonify({
            "success": True,
            "message": "Archivo de conocimiento diario actualizado y gestionado exitosamente.",
            "new_file_id": new_file_id
        }), 200

    except Exception as e:
        logger.error("Error en la gestión del archivo de conocimiento diario para OpenAI", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if tmpfile_path and os.path.exists(tmpfile_path):
            try:
                os.remove(tmpfile_path)
                logger.info(f"Archivo temporal '{tmpfile_path}' eliminado.")
            except PermissionError as pe:
                logger.error(f"Error de permiso al intentar eliminar el archivo temporal en el finally block: {pe}")
            except Exception as final_e:
                logger.error(f"Error inesperado al eliminar el archivo temporal en el finally block: {final_e}")