from flask import Blueprint, send_file, make_response, request, jsonify, render_template, current_app, Response # Blueprint para modularizar y relacionar con app
from flask_bcrypt import Bcrypt                                  # Bcrypt para encriptación
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity   # Jwt para tokens
from database import db                                          # importa la db desde database.py
from logging_config import logger
import os                                                        # Para datos .env
from dotenv import load_dotenv                                   # Para datos .env
load_dotenv()
import pandas as pd
from io import BytesIO
from utils.data_mentor_cursos_utils import query_assistant
import urllib.request
import urllib.error
import json
from models import HistoryUserCourses, User

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("Debes definir la variable de entorno OPENAI_API_KEY con tu clave de API.")

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {OPENAI_API_KEY}",
    "OpenAI-Beta": "assistants=v2"
}

data_mentor_cursos_bp = Blueprint('data_mentor_cursos_bp', __name__)     # instanciar admin_bp desde clase Blueprint para crear las rutas.
bcrypt = Bcrypt()
jwt = JWTManager()

# Sistema de key base pre rutas ------------------------:

API_KEY = os.getenv('API_KEY')

def check_api_key(api_key):
    return api_key == API_KEY

@data_mentor_cursos_bp.before_request
def authorize():
    if request.method == 'OPTIONS':
        return
    if request.path in ['/test_clasifica_data_mentor_cursos_bp']:
        return
    api_key = request.headers.get('Authorization')
    if not api_key or not check_api_key(api_key):
        return jsonify({'message': 'Unauthorized'}), 401
    
# RUTA TEST:

@data_mentor_cursos_bp.route('/test_clasifica_data_mentor_cursos_bp', methods=['GET'])
def test():
    logger.info("data_mentor_cursos bp rutas funcionando ok segun test.")
    return jsonify({'message': 'test bien sucedido','status':"Si lees esto, data mentor cursos rutas funcionan bien..."}),200

@data_mentor_cursos_bp.route("/chat_mentor_cursos", methods=["POST"])
def chat():
    """
    Recibe prompt y opcionalmente thread_id.
    """
    data = request.get_json()
    if not data or "prompt" not in data:
        return jsonify({"error": "Falta el prompt en el cuerpo de la solicitud"}), 400

    prompt = data["prompt"]
    thread_id = data.get("thread_id")  # puede ser None

    try:
        response_text, current_thread = query_assistant(prompt, thread_id)
        return jsonify({"response": response_text, "thread_id": current_thread}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

@data_mentor_cursos_bp.route("/close_chat_cursos", methods=["POST"])
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


@data_mentor_cursos_bp.route("/history-user-add", methods=['POST'])
def add_user_history():
    # Se extrae la autorización y se valida
    auth_header = request.headers.get('Authorization')
    if auth_header != "1803-1989-1803-1989":
        return jsonify({"resultado": "no guardado", "error": "Unauthorized"}), 401

    # Se obtienen los datos del cuerpo de la solicitud JSON
    data = request.get_json()
    if not data:
        return jsonify({"resultado": "no guardado", "error": "No JSON data received"}), 400

    titulo = data.get('titulo')
    email = data.get('email')
    texto = data.get('texto')

    # Validación de datos básicos
    if not all([titulo, email, texto]):
        return jsonify({"resultado": "no guardado", "error": "Missing data: titulo, email, or texto"}), 400

    # 1. Verifica si el usuario existe en la base de datos
    user_exists = User.query.filter_by(email=email).first()
    if not user_exists:
        return jsonify({"resultado": "no guardado", "error": "User with this email does not exist"}), 404
        
    try:
        # 2. Crea una nueva instancia de HistoryUserCourses
        new_history = HistoryUserCourses(
            titulo=titulo,
            email=email,
            texto=texto
        )
        
        # 3. Agrega y guarda en la base de datos
        db.session.add(new_history)
        db.session.commit()

        return jsonify({"resultado": "guardado"}), 201
    
    except Exception as e:
        # En caso de cualquier error, se revierte la transacción de la base de datos
        db.session.rollback()
        print(f"Error al guardar el historial: {e}")
        return jsonify({"resultado": "no guardado", "error": str(e)}), 500
    
@data_mentor_cursos_bp.route("/get-history-by-user", methods=['POST'])
def get_user_history():
    # Extraer el email del usuario de la solicitud JSON
    data = request.get_json()
    if not data or 'email' not in data:
        return jsonify({"error": "Email no proporcionado en el cuerpo de la solicitud"}), 400

    user_email = data.get('email')

    # Verificar si el usuario existe antes de buscar su historial
    user_exists = User.query.filter_by(email=user_email).first()
    if not user_exists:
        return jsonify({"error": "Usuario con este email no existe"}), 404

    try:
        # Buscar todas las entradas de historial para el email del usuario
        history_records = HistoryUserCourses.query.filter_by(email=user_email).order_by(HistoryUserCourses.created_at.desc()).all()
        
        # Serializar cada registro
        serialized_history = [record.serialize() for record in history_records]

        return jsonify(serialized_history), 200
    
    except Exception as e:
        print(f"Error al obtener el historial del usuario: {e}")
        return jsonify({"error": "Error interno del servidor"}), 500
       
@data_mentor_cursos_bp.route("/delete-individual-chat", methods=['POST'])
def delete_individual_chat():
    # Extrae la autorización y la valida
    auth_header = request.headers.get('Authorization')
    if auth_header != "1803-1989-1803-1989":
        return jsonify({"resultado": "no borrado", "error": "Unauthorized"}), 401

    # Obtiene el ID del chat del cuerpo de la solicitud JSON
    data = request.get_json()
    chat_id = data.get('id')

    if not chat_id:
        return jsonify({"resultado": "no borrado", "error": "ID del chat no proporcionado"}), 400
    
    try:
        # Busca el chat por ID
        chat_to_delete = HistoryUserCourses.query.get(chat_id)
        
        if not chat_to_delete:
            return jsonify({"resultado": "no borrado", "error": "Chat no encontrado"}), 404
            
        # Elimina el chat de la sesión y guarda los cambios
        db.session.delete(chat_to_delete)
        db.session.commit()

        return jsonify({"resultado": "borrado"}), 200
        
    except Exception as e:
        db.session.rollback()
        print(f"Error al intentar borrar el chat: {e}")
        return jsonify({"resultado": "no borrado", "error": "Error interno del servidor"}), 500