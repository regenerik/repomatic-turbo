from flask import Blueprint,make_response,send_file, request, jsonify, render_template, current_app # Blueprint para modularizar y relacionar con app
from flask_bcrypt import Bcrypt                                  # Bcrypt para encriptación
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity   # Jwt para tokens
from models import User                                          # importar tabla "User" de models
from database import db                                          # importa la db desde database.py
from datetime import timedelta                                   # importa tiempo especifico para rendimiento de token válido
from routes.utils import exportar_reporte_json, exportar_y_guardar_reporte, obtener_reporte, iniciar_sesion_y_obtener_sesskey, compilar_reportes_existentes
from threading import Thread
import os                                                        # Para datos .env
from dotenv import load_dotenv                                   # Para datos .env
load_dotenv()

admin_bp = Blueprint('admin', __name__)     # instanciar admin_bp desde clase Blueprint para crear las rutas.

bcrypt = Bcrypt()
jwt = JWTManager()

# Sistema de key base pre rutas ------------------------:

API_KEY = os.getenv('API_KEY')

def check_api_key(api_key):
    return api_key == API_KEY

@admin_bp.before_request
def authorize():
    api_key = request.headers.get('Authorization')
    if not api_key or not check_api_key(api_key):
        return jsonify({'message': 'Unauthorized'}), 401
    
#--------------------------------RUTAS SINGLE---------------------------------

# Ruta de prueba time-out-test------------------------------------------------
@admin_bp.route('/test', methods=['GET'])
def test():
    return jsonify({'message': 'test bien sucedido','status':"Si lees esto, tenemos que ver como manejar el timeout porque los archivos llegan..."}),200

# RUTA DOCUMENTACION
@admin_bp.route('/', methods=['GET'])
def show_hello_world():
         return render_template('instructions.html')

# RUTA REPORTES DISPONIBLES CON DATOS-----------------------------------------
@admin_bp.route('/reportes_disponibles', methods=['GET'])
def reportes_disponibles():
    lista_reportes = compilar_reportes_existentes()
    return jsonify({'lista_reportes': lista_reportes , 'result':'ok'}), 200

# Ruta para Obtener USUARIOS POR ASIGNACIÓN PARA GESTORES ( sin parámetros )
@admin_bp.route('/usuarios_por_asignacion_para_gestores', methods=['POST'])
def exportar_reporte():
    print("funciona la ruta")
    data = request.get_json()
    if 'username' not in data or 'password' not in data or 'url' not in data:
        return jsonify({"error": "Falta username,password o url en el cuerpo JSON"}), 400
    username = data['username']
    password = data['password']
    url = data['url']

    # Llamas a la función de utils para exportar el reporte a Html
    json_file = exportar_reporte_json(username, password, url)
    if json_file:
        print("Compilando paquete response con json dentro...")
        response = make_response(json_file)
        response.headers['Content-Type'] = 'application/json'
        print("Devolviendo JSON - log final")
        return response, 200
    else:
        return jsonify({"error": "Error al obtener el reporte en HTML, log final error"}), 500
    
# Ruta 2 para obtener usuarios por asignacion para gestores ( via params )
@admin_bp.route('/usuarios_por_asignacion_para_gestores_v2', methods=['GET'])
def exportar_reporte_v2():
    username = request.args.get('username')
    password = request.args.get('password')
    url = request.args.get('url')

    if not username or not password or not url:
        return jsonify({"error": "Falta username o password en los parámetros de la URL"}), 400

    print("los datos username y password fueron recuperados OK, se va a ejecutar la funcion de utils ahora...")

    json_file = exportar_reporte_json(username, password, url)
    if json_file:
        print("Compilando paquete response con json dentro...")
        response = make_response(json_file)
        response.headers['Content-Type'] = 'application/json'
        print("Devolviendo JSON - log final")
        return response, 200
    else:
        return jsonify({"error": "Error al obtener el reporte en HTML, log final error"}), 500

# --------------------------------------------------------------------------------


#--------------------------------RUTAS MULTIPLES-----------------------------------

@admin_bp.route('/recuperar_reporte', methods=['POST'])
def exportar_y_guardar_reporte_ruta():
    print("funciona la ruta")
    data = request.get_json()
    if 'username' not in data or 'password' not in data or 'url' not in data:
        return jsonify({"error": "Falta username, password, url o user_id en el cuerpo JSON"}), 400
    
    username = data['username']
    password = data['password']
    url = data['url']

    # Llamando al inicio de session por separado y recuperando resultados...
    session, sesskey = iniciar_sesion_y_obtener_sesskey(username, password, url)
    if not session or not sesskey:
        print("Error al iniciar sesión o al obtener el sesskey.")
        return jsonify({"error": "Error al iniciar sesión o al obtener el sesskey"}), 500
    
    # Lanzar la función de exportar y guardar reporte en un hilo separado
    thread = Thread(target=run_exportar_y_guardar_reporte, args=(current_app._get_current_object(),session, sesskey, username, url))
    thread.start()

    return jsonify({"message": "El proceso de recuperación del reporte ha comenzado"}), 200

def run_exportar_y_guardar_reporte(app, session, sesskey, username, url):
    with app.app_context():
        exportar_y_guardar_reporte(session, sesskey, username, url)


    
    

@admin_bp.route('/obtener_reporte', methods=['POST'])
def descargar_reporte():
    print("Funciona la ruta de descarga")
    data = request.get_json()
    if 'reporte_url' not in data or 'username' not in data:
        return jsonify({"error": "Falta reporte_id, username o tipo de archivo en el cuerpo JSON"}), 400
    
    reporte_url = data['reporte_url']
    username = data['username']
    file_type = data.get('file_type', 'csv')
    
    reporte_data = obtener_reporte(reporte_url, username)
    if reporte_data:
        if file_type == 'xlsx':
            response = make_response(reporte_data)
            response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            response.headers['Content-Disposition'] = 'attachment; filename=reporte_usuarios_por_asignacion_para_gestores.xlsx'
        elif file_type == 'json':
            # Assuming reporte_data is already in JSON format
            response = make_response(reporte_data)
            response.headers['Content-Type'] = 'application/json'
            response.headers['Content-Disposition'] = 'attachment; filename=reporte_usuarios_por_asignacion_para_gestores.json'
        elif file_type == 'html':
            # Assuming reporte_data is in HTML format
            response = make_response(reporte_data)
            response.headers['Content-Type'] = 'text/html'
            response.headers['Content-Disposition'] = 'attachment; filename=reporte_usuarios_por_asignacion_para_gestores.html'
        elif file_type == 'csv':
            # Assuming reporte_data is in CSV format
            response = make_response(reporte_data)
            response.headers['Content-Type'] = 'text/csv'
            response.headers['Content-Disposition'] = 'attachment; filename=reporte_usuarios_por_asignacion_para_gestores.csv'
        else:
            # Default to CSV if the file_type is unknown
            response = make_response(reporte_data)
            response.headers['Content-Type'] = 'text/csv'
            response.headers['Content-Disposition'] = 'attachment; filename=reporte_usuarios_por_asignacion_para_gestores.csv'

        return response, 200
    else:
        return jsonify({"error": "No se encontró el reporte"}), 404








# RUTA CREAR USUARIO
@admin_bp.route('/users', methods=['POST'])
def create_user():
    try:
        email = request.json.get('email')
        password = request.json.get('password')
        name = request.json.get('name')

        if not email or not password or not name:
            return jsonify({'error': 'Email, password and Name are required.'}), 400

        existing_user = User.query.filter_by(email=email).first()
        if existing_user:
            return jsonify({'error': 'Email already exists.'}), 409

        password_hash = bcrypt.generate_password_hash(password).decode('utf-8')


        # Ensamblamos el usuario nuevo
        new_user = User(email=email, password=password_hash, name=name)

        db.session.add(new_user)
        db.session.commit()

        good_to_share_to_user = {
            'id': new_user.id,
            'name':new_user.name,
            'email':new_user.email
        }

        return jsonify({'message': 'User created successfully.','user_created':good_to_share_to_user}), 201

    except Exception as e:
        return jsonify({'error': 'Error in user creation: ' + str(e)}), 500


#RUTA LOG-IN ( CON TOKEN DE RESPUESTA )
@admin_bp.route('/token', methods=['POST'])
def get_token():
    try:
        #  Primero chequeamos que por el body venga la info necesaria:
        email = request.json.get('email')
        password = request.json.get('password')

        if not email or not password:
            return jsonify({'error': 'Email and password are required.'}), 400
        
        # Buscamos al usuario con ese correo electronico ( si lo encuentra lo guarda ):
        login_user = User.query.filter_by(email=request.json['email']).one()

        # Verificamos que el password sea correcto:
        password_from_db = login_user.password #  Si loguin_user está vacio, da error y se va al "Except".
        true_o_false = bcrypt.check_password_hash(password_from_db, password)
        
        # Si es verdadero generamos un token y lo devuelve en una respuesta JSON:
        if true_o_false:
            expires = timedelta(minutes=30)  # pueden ser "hours", "minutes", "days","seconds"

            user_id = login_user.id       # recuperamos el id del usuario para crear el token...
            access_token = create_access_token(identity=user_id, expires_delta=expires)   # creamos el token con tiempo vencimiento
            return jsonify({ 'access_token':access_token}), 200  # Enviamos el token al front ( si es necesario serializamos el "login_user" y tambien lo enviamos en el objeto json )

        else:
            return {"Error":"Contraseña  incorrecta"}
    
    except Exception as e:
        return {"Error":"El email proporcionado no corresponde a ninguno registrado: " + str(e)}, 500
    
# EJEMPLO DE RUTA RESTRINGIDA POR TOKEN. ( LA MISMA RECUPERA TODOS LOS USERS Y LO ENVIA PARA QUIEN ESTÉ LOGUEADO )
    
@admin_bp.route('/users')
@jwt_required()  # Decorador para requerir autenticación con JWT
def show_users():
    current_user_id = get_jwt_identity()  # Obtiene la id del usuario del token
    if current_user_id:
        users = User.query.all()
        user_list = []
        for user in users:
            user_dict = {
                'id': user.id,
                'email': user.email
            }
            user_list.append(user_dict)
        return jsonify(user_list), 200
    else:
        return {"Error": "Token inválido o vencido"}, 401