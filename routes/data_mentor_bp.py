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
    Recibe un archivo .xlsx vía form-data (campo: 'file') y guarda sus registros en la DB.
    Optimizado para bajo uso de memoria mediante procesamiento por lotes (chunks).
    Incluye logs detallados de progreso.
    """
    archivo = request.files.get('file')
    if not archivo:
        logger.error("No se envió ningún archivo en la solicitud.")
        return jsonify({'error': 'No se envió ningún archivo', 'status': 400}), 400

    total_registros_guardados = 0
    # Define un tamaño de lote. Ajusta este valor según tu memoria disponible y tamaño de fila.
    BATCH_SIZE = 2000 

    logger.info("======================================================")
    logger.info("============= INICIANDO PROCESO DE CARGA =============")
    logger.info("======================================================")
    logger.info(f"Archivo recibido: '{archivo.filename}' ({archivo.content_length / (1024*1024):.2f} MB)")
    logger.info(f"Tamaño de lote (BATCH_SIZE) configurado: {BATCH_SIZE} filas por chunk.")

    try:
        # Usar ExcelFile para abrir el archivo una vez y verificar hojas
        xls = pd.ExcelFile(archivo)
        
        # Determinar la hoja a leer. Por defecto, la primera.
        if not xls.sheet_names:
            logger.error("El archivo Excel recibido no contiene hojas.")
            return jsonify({'error': 'El archivo Excel no contiene hojas.', 'status': 400}), 400
        sheet_name_to_read = xls.sheet_names[0] # Lee la primera hoja
        logger.info(f"Se procesará la hoja: '{sheet_name_to_read}'")

        chunk_counter = 0
        # Leer el archivo Excel en chunks. pd.read_excel con chunksize devuelve un iterador.
        for chunk_df in pd.read_excel(archivo, chunksize=BATCH_SIZE, sheet_name=sheet_name_to_read):
            chunk_counter += 1
            registros_chunk = []
            
            logger.info(f"--- Iniciando procesamiento del CHUNK {chunk_counter} ({len(chunk_df)} filas) ---")
            
            for index, fila in chunk_df.iterrows(): # Usamos 'index' para referencia de fila dentro del chunk
                fecha_raw = fila.get('FECHA')
                try:
                    fecha = pd.to_datetime(fecha_raw, errors='coerce') if pd.notnull(fecha_raw) else None
                except Exception as date_e:
                    logger.warning(f"CHUNK {chunk_counter}, Fila {index+2} (línea Excel): Error al convertir fecha '{fecha_raw}': {date_e}. Se usará None.")
                    fecha = None

                nuevo = Comentarios2024(
                    fecha=fecha,
                    apies=str(fila.get('APIES', '')).strip(),
                    comentario=str(fila.get('COMENTARIO', '')).strip(),
                    canal=str(fila.get('CANAL', '')).strip(),
                    topico=str(fila.get('TÓPICO', '')).strip(),
                    sentiment=str(fila.get('SENTIMENT', '')).strip()
                )
                registros_chunk.append(nuevo)

            # Agregar y commitear el lote si no está vacío
            if registros_chunk:
                db.session.add_all(registros_chunk)
                db.session.commit()
                total_registros_guardados += len(registros_chunk)
                logger.info(f"CHUNK {chunk_counter} COMPLETO. Guardados {len(registros_chunk)} registros en DB.")
                logger.info(f"TOTAL DE REGISTROS GUARDADOS HASTA AHORA: {total_registros_guardados}")
            else:
                logger.warning(f"CHUNK {chunk_counter} estaba vacío o no contenía registros válidos para guardar.")
            
            # Liberar memoria de los objetos del chunk
            db.session.expunge_all() 
            del registros_chunk 
            del chunk_df 
            # Si hay muchos logs de memoria, podrías añadir un gc.collect() aquí, pero suele ser automático.
            # import gc; gc.collect() 
            
            logger.info(f"--- Memoria del CHUNK {chunk_counter} liberada. ---")


        logger.info("======================================================")
        logger.info("============ PROCESO DE CARGA FINALIZADO =============")
        logger.info(f"TOTAL DE REGISTROS GUARDADOS EN LA DB: {total_registros_guardados}")
        logger.info("======================================================")
        
        return jsonify({'mensaje': f'Se guardaron {total_registros_guardados} comentarios en total.', 'status': 200}), 200

    except Exception as e:
        db.session.rollback() # Asegura que la transacción se revierta en caso de error
        logger.error("======================================================")
        logger.error("============ ERROR FATAL DURANTE LA CARGA ============")
        logger.error(f"Error: {str(e)}", exc_info=True)
        logger.error("======================================================")
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

        # --- 1. Recopilar datos de todas las tablas y preparar resumen de contenido ---
        content_summary = [] # Lista para el resumen de contenido

        # Función auxiliar para serializar y calcular el tamaño de una sección
        def get_serialized_data_and_size(data_list, section_name):
            serialized_data = [item.serialize() for item in data_list]
            # Convertir a JSON string para estimar el tamaño real que ocupará en el archivo
            json_string = json.dumps(serialized_data, ensure_ascii=False)
            size_bytes = len(json_string.encode('utf-8')) # Tamaño en bytes
            size_mb = size_bytes / (1024 * 1024) # Tamaño en MB
            
            content_summary.append({
                "nombre": section_name,
                "incluido": bool(len(data_list) > 0),
                "peso_mb": round(size_mb, 4), # Redondear para mejor legibilidad
                "total_registros": len(data_list)
            })
            return serialized_data, len(data_list) # Devolver datos serializados y conteo

        comentarios_2025_serialized, comentarios_2025_count = get_serialized_data_and_size(Comentarios2025.query.all(), "comentarios_2025")
        fichas_google_serialized, fichas_google_count = get_serialized_data_and_size(FichasGoogle.query.all(), "fichas_google")
        fichas_google_competencia_serialized, fichas_google_competencia_count = get_serialized_data_and_size(FichasGoogleCompetencia.query.all(), "fichas_google_competencia")
        usuarios_por_asignacion_serialized, usuarios_por_asignacion_count = get_serialized_data_and_size(Usuarios_Por_Asignacion.query.all(), "usuarios_por_asignacion")
        usuarios_sin_id_serialized, usuarios_sin_id_count = get_serialized_data_and_size(Usuarios_Sin_ID.query.all(), "usuarios_sin_id")
        valida_usuarios_serialized, valida_usuarios_count = get_serialized_data_and_size(ValidaUsuarios.query.all(), "valida_usuarios")
        detalle_apies_serialized, detalle_apies_count = get_serialized_data_and_size(DetalleApies.query.all(), "detalle_apies")
        avance_cursada_serialized, avance_cursada_count = get_serialized_data_and_size(AvanceCursada.query.all(), "avance_cursada")
        detalles_de_cursos_serialized, detalles_de_cursos_count = get_serialized_data_and_size(DetallesDeCursos.query.all(), "detalles_de_cursos")
        cursadas_agrupadas_serialized, cursadas_agrupadas_count = get_serialized_data_and_size(CursadasAgrupadas.query.all(), "cursadas_agrupadas")
        formulario_gestor_serialized, formulario_gestor_count = get_serialized_data_and_size(FormularioGestor.query.all(), "formulario_gestor")
        cuarto_survey_sql_serialized, cuarto_survey_sql_count = get_serialized_data_and_size(CuartoSurveySql.query.all(), "cuarto_survey_sql")
        quinto_survey_sql_serialized, quinto_survey_sql_count = get_serialized_data_and_size(QuintoSurveySql.query.all(), "quinto_survey_sql")
        comentarios_2023_serialized, comentarios_2023_count = get_serialized_data_and_size(Comentarios2023.query.all(), "comentarios_2023")
        comentarios_2024_serialized, comentarios_2024_count = get_serialized_data_and_size(Comentarios2024.query.all(), "comentarios_2024")
        base_loop_estaciones_serialized, base_loop_estaciones_count = get_serialized_data_and_size(BaseLoopEstaciones.query.all(), "base_loop_estaciones")
        sales_force_serialized, sales_force_count = get_serialized_data_and_size(SalesForce.query.all(), "sales_force")
        comentarios_competencia_serialized, comentarios_competencia_count = get_serialized_data_and_size(ComentariosCompetencia.query.all(), "comentarios_competencia")


        # --- Creación del diccionario JSON final ---
        data_json = {
            "descripcion_contenido_archivo": "Este archivo JSON contiene datos operativos y de experiencia del cliente de YPF, organizados por sección. Cada sección (ej., 'comentarios_2025', 'base_loop_estaciones') incluye un campo 'total_registros' y los 'datos' detallados. Se incluye una sección 'resumen_conteos_totales' para acceso directo a los conteos por sección.",
            "resumen_conteos_totales": { # <--- ¡NUEVA SECCIÓN AGREGADA AQUÍ!
                "comentarios_2025": comentarios_2025_count,
                "fichas_google": fichas_google_count,
                "fichas_google_competencia": fichas_google_competencia_count,
                "usuarios_por_asignacion": usuarios_por_asignacion_count,
                "usuarios_sin_id": usuarios_sin_id_count,
                "valida_usuarios": valida_usuarios_count,
                "detalle_apies": detalle_apies_count,
                "avance_cursada": avance_cursada_count,
                "detalles_de_cursos": detalles_de_cursos_count,
                "cursadas_agrupadas": cursadas_agrupadas_count,
                "formulario_gestor": formulario_gestor_count,
                "cuarto_survey_sql": cuarto_survey_sql_count,
                "quinto_survey_sql": quinto_survey_sql_count,
                "comentarios_2023": comentarios_2023_count,
                "comentarios_2024": comentarios_2024_count,
                "base_loop_estaciones": base_loop_estaciones_count,
                "sales_force": sales_force_count,
                "comentarios_competencia": comentarios_competencia_count
            },
            "comentarios_2025": {
                "total_registros": comentarios_2025_count,
                "datos": comentarios_2025_serialized
            },
            "fichas_google": {
                "total_registros": fichas_google_count,
                "datos": fichas_google_serialized
            },
            "fichas_google_competencia": {
                "total_registros": fichas_google_competencia_count,
                "datos": fichas_google_competencia_serialized
            },
            "usuarios_por_asignacion": {
                "total_registros": usuarios_por_asignacion_count,
                "datos": usuarios_por_asignacion_serialized
            },
            "usuarios_sin_id": {
                "total_registros": usuarios_sin_id_count,
                "datos": usuarios_sin_id_serialized
            },
            "valida_usuarios": {
                "total_registros": valida_usuarios_count,
                "datos": valida_usuarios_serialized
            },
            "detalle_apies": {
                "total_registros": detalle_apies_count,
                "datos": detalle_apies_serialized
            },
            "avance_cursada": {
                "total_registros": avance_cursada_count,
                "datos": avance_cursada_serialized
            },
            "detalles_de_cursos": {
                "total_registros": detalles_de_cursos_count,
                "datos": detalles_de_cursos_serialized
            },
            "cursadas_agrupadas": {
                "total_registros": cursadas_agrupadas_count,
                "datos": cursadas_agrupadas_serialized
            },
            "formulario_gestor": {
                "total_registros": formulario_gestor_count,
                "datos": formulario_gestor_serialized
            },
            "cuarto_survey_sql": {
                "total_registros": cuarto_survey_sql_count,
                "datos": cuarto_survey_sql_serialized
            },
            "quinto_survey_sql": {
                "total_registros": quinto_survey_sql_count,
                "datos": quinto_survey_sql_serialized
            },
            "comentarios_2023": {
                "total_registros": comentarios_2023_count,
                "datos": comentarios_2023_serialized
            },
            "comentarios_2024": {
                "total_registros": comentarios_2024_count,
                "datos": comentarios_2024_serialized
            },
            "base_loop_estaciones": {
                "total_registros": base_loop_estaciones_count,
                "datos": base_loop_estaciones_serialized
            },
            "sales_force": {
                "total_registros": sales_force_count,
                "datos": sales_force_serialized
            },
            "comentarios_competencia": {
                "total_registros": comentarios_competencia_count,
                "datos": comentarios_competencia_serialized
            }
        }

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
            new_record = FileDailyID(
                current_file_id=new_file_id,
            )
            db.session.add(new_record)
            db.session.commit()
            logger.info(f"Nuevo registro de ID de archivo creado en la base de datos: {new_file_id}")

        return jsonify({
            "success": True,
            "message": "Archivo de conocimiento diario actualizado y gestionado exitosamente.",
            "new_file_id": new_file_id,
            "final_file_size_mb": round(file_size, 4), # <-- Tamaño del archivo final
            "contenido_incluido": content_summary # <-- Resumen detallado del contenido
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


# ESTOS SON TEST DE PORQUE NO ENCUENTRA EL ARCHIVO QUE SE SUPONE QUE YA ESTA ONLINE >>>>>>>>>>>>>>>>>>>> ( DESPUES SE PUEDE BORRAR )

@data_mentor_bp.route("/test-openai-file-status", methods=["GET"])
def test_openai_file_status():
    """
    Ruta para verificar la conectividad a OpenAI y el estado del archivo de conocimiento diario.
    """
    logger.info("Iniciando prueba de estado de archivo OpenAI...")
    
    # 1. Verificar la clave API
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        return jsonify({"error": "OPENAI_API_KEY no está configurada como variable de entorno.",
                        "instrucciones": "Asegúrate de configurar 'OPENAI_API_KEY' en tu entorno local (ej. 'set OPENAI_API_KEY=sk-...' en CMD y reiniciar la terminal/IDE).",
                        "status": 500}), 500

    # 2. Obtener el file_id más reciente desde tu DB local
    try:
        daily_file_record = FileDailyID.query.first()
        if not daily_file_record:
            return jsonify({"error": "No se encontró ningún registro de FileDailyID en la base de datos local.",
                            "instrucciones": "Asegúrate de que la ruta '/actualizar-archivos-asistente' se haya ejecutado al menos una vez con éxito.",
                            "status": 404}), 404
        
        current_file_id = daily_file_record.current_file_id
        logger.info(f"File ID encontrado en DB local: {current_file_id}")

    except Exception as db_e:
        logger.error(f"Error al acceder a la base de datos local para FileDailyID: {db_e}", exc_info=True)
        return jsonify({"error": f"Error interno al acceder a DB local: {str(db_e)}", "status": 500}), 500

    # 3. Consultar a OpenAI el estado del archivo
    try:
        file_obj = client.files.retrieve(file_id=current_file_id)
        
        response_data = {
            "success": True,
            "message": f"Consulta exitosa a OpenAI para el archivo {current_file_id}.",
            "file_details": {
                "id": file_obj.id,
                "status": file_obj.status,
                "purpose": file_obj.purpose,
                "size_bytes": file_obj.bytes,
                "created_at": file_obj.created_at
            }
        }
        
        if file_obj.status == "failed":
            response_data["file_details"]["error"] = str(file_obj.error) # Añadir el error si el status es 'failed'
            logger.error(f"El archivo {current_file_id} está en estado 'failed': {file_obj.error}")
        elif file_obj.status == "processed":
            logger.info(f"El archivo {current_file_id} está completamente procesado.")
        else:
            logger.warning(f"El archivo {current_file_id} está en estado '{file_obj.status}' (aún no 'processed').")

        return jsonify(response_data), 200

    except Exception as openai_e:
        logger.error(f"Error al consultar el estado del archivo {current_file_id} en OpenAI: {openai_e}", exc_info=True)
        return jsonify({"error": f"Error al consultar OpenAI: {str(openai_e)}", "status": 500}), 500
    


@data_mentor_bp.route("/debug-assistant-thread", methods=["GET"])
def debug_assistant_thread():
    """
    Ruta para depurar un Thread específico del Assistant,
    mostrando mensajes y detalles de Runs (incluyendo tool_calls y outputs).
    
    Parámetros de consulta:
    - thread_id: El ID del thread a depurar.
    """
    thread_id = request.args.get("thread_id")
    if not thread_id:
        return jsonify({"error": "Falta el 'thread_id' como parámetro de consulta.", "status": 400}), 400

    logger.info(f"Iniciando depuración para el Thread ID: {thread_id}")

    try:
        # Asegúrate de que el cliente de OpenAI esté inicializado aquí si no lo está globalmente
        # o si prefieres que se inicialice por cada petición para este endpoint de depuración.
        # client = OpenAI(api_key=os.getenv('OPENAI_API_KEY')) # Descomenta si lo necesitas aquí

        # 1. Recuperar Mensajes del Thread
        messages_response = client.beta.threads.messages.list(thread_id=thread_id, order="asc", limit=100)
        messages_data = []
        for msg in messages_response.data:
            content_text = ""
            for content_block in msg.content:
                if content_block.type == "text":
                    content_text += content_block.text.value
                elif content_block.type == "image_file":
                    content_text += f"[Contenido de imagen: file_id={content_block.image_file.file_id}]"
                elif content_block.type == "tool_use": # Para tool_use en los mensajes del asistente (ej. output de Code Interpreter)
                    content_text += f"[Uso de Herramienta: {content_block.tool_use.name}, ID: {content_block.tool_use.id}]"
                elif content_block.type == "file_search": # Si hay file_search directamente en el contenido (menos común en mensajes)
                     content_text += f"[Búsqueda de Archivo: file_ids={content_block.file_search.file_ids}]"
            
            messages_data.append({
                "id": msg.id,
                "role": msg.role,
                "content": content_text,
                "created_at": msg.created_at,
                "attachments": [att.file_id for att in msg.attachments if hasattr(att, 'file_id')] if msg.attachments else [],
                "annotations": [] # Puedes expandir esto si las anotaciones son relevantes para tu depuración
            })
        logger.info(f"Recuperados {len(messages_data)} mensajes del Thread.")

        # 2. Recuperar Runs del Thread y sus Pasos
        runs_response = client.beta.threads.runs.list(thread_id=thread_id, order="asc", limit=20)
        runs_details = []

        for run_obj in runs_response.data:
            run_detail = {
                "id": run_obj.id,
                "status": run_obj.status,
                "assistant_id": run_obj.assistant_id,
                "created_at": run_obj.created_at,
                "failed_error": None,
                "tool_calls_executed": [], # Herramientas que el Assistant QUISO llamar
                "tool_outputs_received": [] # Lo que las herramientas realmente DEVOLVIERON
            }

            if run_obj.status == "failed" and run_obj.last_error:
                run_detail["failed_error"] = {
                    "code": run_obj.last_error.code,
                    "message": run_obj.last_error.message
                }
            
            # Recuperar pasos del Run para ver Tool Calls y Outputs
            run_steps_response = client.beta.threads.runs.steps.list(thread_id=thread_id, run_id=run_obj.id, order="asc", limit=50)
            
            for step in run_steps_response.data:
                if step.type == "tool_calls" and step.step_details and step.step_details.tool_calls:
                    for tool_call_detail in step.step_details.tool_calls:
                        call_info = {
                            "tool_call_id": tool_call_detail.id,
                            "type": tool_call_detail.type
                        }
                        if tool_call_detail.type == "function" and tool_call_detail.function:
                            call_info["function_name"] = tool_call_detail.function.name
                            call_info["function_arguments"] = json.loads(tool_call_detail.function.arguments) # Argumentos como JSON
                        elif tool_call_detail.type == "file_search":
                            # La query exacta que File Search hizo no siempre está expuesta en tool_calls en este nivel
                            # Puede inferirse del output o de logs internos.
                            call_info["file_search_details"] = tool_call_detail.file_search.model_dump() if hasattr(tool_call_detail.file_search, 'model_dump') else "Detalles de File Search disponibles"
                        
                        run_detail["tool_calls_executed"].append(call_info)

                elif step.type == "tool_outputs" and step.step_details and hasattr(step.step_details, 'tool_outputs'):
                    for output in step.step_details.tool_outputs:
                        output_info = {
                            "tool_call_id": output.tool_call_id,
                            "output": output.output # Este es el resultado que la herramienta devolvió
                        }
                        run_detail["tool_outputs_received"].append(output_info)
            
            runs_details.append(run_detail)

        return jsonify({
            "success": True,
            "thread_id": thread_id,
            "messages": messages_data,
            "runs_details": runs_details
        }), 200

    except Exception as e:
        logger.error(f"Error al depurar el Thread {thread_id}: {e}", exc_info=True)
        # Puedes añadir instrucciones si el error es por clave API o Thread ID no encontrado
        error_message = f"Error al depurar el Thread: {str(e)}"
        if "No such thread" in str(e):
            error_message += ". El Thread ID proporcionado no existe."
        elif "authentication" in str(e).lower() or "api_key" in str(e).lower():
            error_message += ". Problema de autenticación con la API Key."
        return jsonify({"error": error_message, "status": 500}), 500