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
from models import Usuarios_Por_Asignacion, Usuarios_Sin_ID, ValidaUsuarios,DetalleApies, AvanceCursada, DetallesDeCursos, CursadasAgrupadas,FormularioGestor,CuartoSurveySql, QuintoSurveySql, Comentarios2023, Comentarios2024, Comentarios2025, BaseLoopEstaciones, FichasGoogleCompetencia, FichasGoogle, SalesForce, ComentariosCompetencia
import hashlib
from sqlalchemy.exc import SQLAlchemyError
import csv


OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("Debes definir la variable de entorno OPENAI_API_KEY con tu clave de API.")

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
    Detecta duplicados por hash_id antes de insertar y usa bulk_save_objects para velocidad.
    """
    archivo = request.files.get('file')
    if not archivo:
        return jsonify({'error': 'No se envió ningún archivo', 'status': 400}), 400

    try:
        df = pd.read_excel(archivo)

        # Paso 1: Preparamos todos los registros con hash
        candidatos = []
        hash_ids = []

        for _, fila in df.iterrows():
            fecha_raw = fila.get('FECHA')
            try:
                fecha = pd.to_datetime(fecha_raw) if pd.notnull(fecha_raw) else None
            except:
                fecha = None

            apies = str(fila.get('APIES', '')).strip()
            comentario = str(fila.get('COMENTARIO', '')).strip()
            canal = str(fila.get('CANAL', '')).strip()
            topico = str(fila.get('TÓPICO', '')).strip()
            sentiment = str(fila.get('SENTIMENT', '')).strip()

            # Hash único
            hash_id = Comentarios2025.generar_hash(fecha, apies, comentario, canal)

            comentario_obj = Comentarios2025(
                fecha=fecha,
                apies=apies,
                comentario=comentario,
                canal=canal,
                topico=topico,
                sentiment=sentiment,
                hash_id=hash_id
            )

            candidatos.append(comentario_obj)
            hash_ids.append(hash_id)

        # Paso 2: Buscar cuáles ya existen
        existentes = set(
            r[0] for r in db.session.query(Comentarios2025.hash_id)
            .filter(Comentarios2025.hash_id.in_(hash_ids))
            .all()
        )

        # Paso 3: Filtrar duplicados
        nuevos = [c for c in candidatos if c.hash_id not in existentes]

        # Paso 4: Insertar de forma masiva
        if nuevos:
            db.session.bulk_save_objects(nuevos)
            db.session.commit()

        return jsonify({
            'mensaje': f'Se guardaron {len(nuevos)} comentarios nuevos',
            'duplicados_ignorados': len(candidatos) - len(nuevos),
            'status': 200
        }), 200

    except Exception as e:
        db.session.rollback()
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