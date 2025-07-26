import json
from datetime import datetime
from models import InstruccionesGenerales, InstruccionesIndividuales
from database import db 
# Asegúrate de que logging_config esté configurado y disponible
from logging_config import logger # Asumiendo que tu logger se importa así


def carga_base_instrucciones_ia_data_mentor():
    """
    Carga las instrucciones iniciales para el Assistant de IA en la base de datos.
    Verifica si los registros ya existen para evitar duplicados.
    """
    logger.info("Iniciando carga de instrucciones de IA para Data Mentor...")

    try:
        # --- Cargar Instrucciones Generales ---
        inst_gen = InstruccionesGenerales.query.first()
        if not inst_gen:
            logger.info("No se encontraron Instrucciones Generales. Creando registro inicial...")
            inst_gen = InstruccionesGenerales(
                descripcion_general="Este archivo contiene una base de conocimiento integral sobre las operaciones, experiencia del cliente y aprendizaje comercial de nuestra empresa. El objetivo es proporcionar información detallada para análisis, resolución de consultas y comparación con la competencia. La tabla 'base_loop_estaciones' es central para la mayoría de las relaciones. Cada sección de datos ahora incluye un campo 'total_registros' para facilitar conteos directos, y los datos reales están bajo la clave 'datos'.",
                instrucciones_especificas_para_ia="Cuando un usuario haga una pregunta, primero identifica la sección más relevante usando 'secciones_disponibles'. Cada sección de datos contiene un campo 'total_registros' con la cantidad total de entradas en esa sección, y los datos detallados se encuentran bajo la clave 'datos'. Si la pregunta requiere combinar información de diferentes secciones (ej. 'comentarios' con 'base_loop_estaciones'), utiliza las 'relaciones_clave' indicadas para vincularlas. Por ejemplo, para obtener comentarios de una estación específica, usa el campo 'APIES' de los comentarios y de 'base_loop_estaciones'. Siempre correlaciona la pregunta del usuario con la sección del JSON que contenga la información más probable. Si la información no está disponible en una sección o en la mezcla de dos o más secciones por medio de joins de tablas, indícalo claramente. Proporciona respuestas claras, concisas y directas, citando la sección del documento de donde proviene la información si es necesario."
            )
            db.session.add(inst_gen)
            db.session.commit()
            logger.info("Instrucciones Generales cargadas exitosamente.")
        else:
            logger.info("Instrucciones Generales ya existen en la base de datos. No se realizaron cambios.")

        # --- Cargar Instrucciones Individuales ---
        secciones_data = [
            {
                "name": "base_loop_estaciones",
                "descripcion": "Tabla PRINCIPAL. Detalle de estaciones (operativa, geográfica, administrativa). Campos 'APIES' e 'Id' claves para relaciones. Los datos reales están en 'datos' y el conteo total en 'total_registros'.",
                "relaciones_clave": {
                    "BaseLoopEstaciones.APIES": "Relaciona con 'comentarios_2023.APIES', 'comentarios_2024.APIES', 'comentarios_2025.APIES'.",
                    "BaseLoopEstaciones.Id": "Relaciona con 'fichas_google.Store_Code', 'fichas_google_competencia.Idloop', 'comentarios_competencia.Idloop', 'usuarios_por_asignacion.ID_Pertenencia'."
                },
                "ejemplo_consulta": "Para la estación con ID 1234, ¿cuál es su volumen promedio de Nafta y qué comentarios de clientes tiene de 2025?"
            },
            {
                "name": "comentarios_2023",
                "descripcion": "Comentarios de encuestas de clientes recibidos en 2023. Incluye 'fecha', 'apies' (ID de estación), 'comentario' (texto libre), 'canal', 'topico', 'sentiment'. Los datos reales están en 'datos' y el conteo total en 'total_registros'.",
                "relaciones_clave": {"nota": "Relacionado con 'base_loop_estaciones' mediante 'APIES'."}, # CORREGIDO: Ahora es un diccionario
                "ejemplo_consulta": "¿Qué comentarios positivos hubo en la estación 5678 en 2023 sobre la atención?"
            },
            {
                "name": "comentarios_2024",
                "descripcion": "Comentarios de encuestas de clientes recibidos en 2024. Formato y campos similares a 2023. Los datos reales están en 'datos' y el conteo total en 'total_registros'.",
                "relaciones_clave": {"nota": "Relacionado con 'base_loop_estaciones' mediante 'APIES'."}, # CORREGIDO: Ahora es un diccionario
                "ejemplo_consulta": "Dame los tópicos más frecuentes en los comentarios negativos de 2024 para la región 'Norte'."
            },
            {
                "name": "comentarios_2025",
                "descripcion": "Comentarios de encuestas de clientes recibidos en 2025. Formato y campos similares a 2023 y 2024. Los datos reales están en 'datos' y el conteo total en 'total_registros'.",
                "relaciones_clave": {"nota": "Relacionado con 'base_loop_estaciones' mediante 'APIES'."}, # CORREGIDO: Ahora es un diccionario
                "ejemplo_consulta": "¿Cuáles son los comentarios recientes (2025) sobre el 'precio' en estaciones de Capital Federal?"
            },
            {
                "name": "fichas_google",
                "descripcion": "Datos de nuestras fichas de Google (reseñas, valoraciones, información de la estación). Contiene 'Store_Code' que es el ID de la estación. Los datos reales están en 'datos' y el conteo total en 'total_registros'.",
                "relaciones_clave": {"nota": "Relacionado con 'base_loop_estaciones' mediante 'Store_Code' (que es igual a BaseLoopEstaciones.Id)."}, # CORREGIDO: Ahora es un diccionario
                "ejemplo_consulta": "¿Cuál es la valoración promedio de las fichas de Google para las estaciones de Buenos Aires?"
            },
            {
                "name": "fichas_google_competencia",
                "descripcion": "Datos de fichas de Google de la competencia. Permite analizar y comparar métricas y comentarios de nuestros rivales. Contiene 'Idloop' que es el ID de la estación asociada. Los datos reales están en 'datos' y el conteo total en 'total_registros'.",
                "relaciones_clave": {"nota": "Relacionado con 'base_loop_estaciones' mediante 'Idloop' (que es igual a BaseLoopEstaciones.Id)."}, # CORREGIDO: Ahora es un diccionario
                "ejemplo_consulta": "¿Qué comentarios negativos hay en las fichas de Google de la competencia sobre la 'velocidad de servicio'?"
            },
            {
                "name": "comentarios_competencia",
                "descripcion": "**¡ATENCIÓN!** Esta sección contiene **comentarios textuales de clientes específicamente sobre nuestros competidores.** Busca aquí para analizar el tipo de feedback que reciben nuestros rivales en temas como precio, atención, calidad de producto, etc. Los campos incluyen 'competidor', 'comentario', 'sentimiento'. Los datos reales están en 'datos' y el conteo total en 'total_registros'.",
                "relaciones_clave": {"nota": "Relacionado con 'base_loop_estaciones' mediante 'Idloop' (que es igual a BaseLoopEstaciones.Id)."}, # CORREGIDO: Ahora es un diccionario
                "ejemplo_consulta": "Dame los comentarios negativos de la competencia sobre el precio en el último mes."
            },
            {
                "name": "usuarios_por_asignacion",
                "descripcion": "Detalles sobre la asignación de usuarios a estaciones. 'ID_Pertenencia' corresponde al ID de la estación en BaseLoopEstaciones. Los datos reales están en 'datos' y el conteo total en 'total_registros'.",
                "relaciones_clave": {"nota": "Relacionado con 'base_loop_estaciones' mediante 'ID_Pertenencia' (que es igual a BaseLoopEstaciones.Id)."}, # CORREGIDO: Ahora es un diccionario
                "ejemplo_consulta": "¿Cuántos usuarios están asignados a la estación con ID 1234 y cuál es su tipo de operador?"
            },
            {
                "name": "usuarios_sin_id",
                "descripcion": "Información sobre usuarios que no tienen un ID de sistema asignado. Los datos reales están en 'datos' y el conteo total en 'total_registros'.",
                "relaciones_clave": {"nota": "No hay una relación directa con BaseLoopEstaciones basada en IDs de estación."} # CORREGIDO: Ahora es un diccionario
            },
            {
                "name": "valida_usuarios",
                "descripcion": "Datos utilizados para la validación de usuarios. Los datos reales están en 'datos' y el conteo total en 'total_registros'.",
                "relaciones_clave": {"nota": "No hay una relación directa con BaseLoopEstaciones basada en IDs de estación."} # CORREGIDO: Ahora es un diccionario
            },
            {
                "name": "detalle_apies",
                "descripcion": "Detalle de identificadores de APIES. Los datos reales están en 'datos' y el conteo total en 'total_registros'.",
                "relaciones_clave": {"nota": "Contiene IDs de APIES, que pueden ser usados para correlacionar con BaseLoopEstaciones."} # CORREGIDO: Ahora es un diccionario
            },
            {
                "name": "avance_cursada",
                "descripcion": "Seguimiento del progreso de los usuarios en cursos específicos. Contiene 'ID_Usuario' y 'ID_Curso'. Los datos reales están en 'datos' y el conteo total en 'total_registros'.",
                "relaciones_clave": {"nota": "No hay una relación directa con BaseLoopEstaciones basada en IDs de estación."} # CORREGIDO: Ahora es un diccionario
            },
            {
                "name": "detalles_de_cursos",
                "descripcion": "Información detallada sobre los cursos disponibles, como nombre del curso, duración, etc. 'ID_Curso' es la clave. Los datos reales están en 'datos' y el conteo total en 'total_registros'.",
                "relaciones_clave": {"nota": "No hay una relación directa con BaseLoopEstaciones basada en IDs de estación."} # CORREGIDO: Ahora es un diccionario
            },
            {
                "name": "cursadas_agrupadas",
                "descripcion": "Resumen o agrupación de datos de cursadas. Los datos reales están en 'datos' y el conteo total en 'total_registros'.",
                "relaciones_clave": {"nota": "No hay una relación directa con BaseLoopEstaciones basada en IDs de estación."} # CORREGIDO: Ahora es un diccionario
            },
            {
                "name": "formulario_gestor",
                "descripcion": "Datos recopilados de formularios gestionados. Los datos reales están en 'datos' y el conteo total en 'total_registros'.",
                "relaciones_clave": {"nota": "No hay una relación directa con BaseLoopEstaciones basada en IDs de estación."} # CORREGIDO: Ahora es un diccionario
            },
            {
                "name": "cuarto_survey_sql",
                "descripcion": "Resultados de la Cuarta Encuesta SQL. Los datos reales están en 'datos' y el conteo total en 'total_registros'.",
                "relaciones_clave": {"nota": "No hay una relación directa con BaseLoopEstaciones basada en IDs de estación."} # CORREGIDO: Ahora es un diccionario
            },
            {
                "name": "quinto_survey_sql",
                "descripcion": "Resultados de la Quinta Encuesta SQL. Los datos reales están en 'datos' y el conteo total en 'total_registros'.",
                "relaciones_clave": {"nota": "No hay una relación directa con BaseLoopEstaciones basada en IDs de estación."} # CORREGIDO: Ahora es un diccionario
            },
            {
                "name": "sales_force",
                "descripcion": "Datos provenientes de SalesForce, relacionados con ventas o gestión de relaciones con clientes. Los datos reales están en 'datos' y el conteo total en 'total_registros'.",
                "relaciones_clave": {"nota": "No hay una relación directa con BaseLoopEstaciones basada en IDs de estación."} # CORREGIDO: Ahora es un diccionario
            }
        ]

        for item_data in secciones_data:
            inst_ind = InstruccionesIndividuales.query.filter_by(name=item_data['name']).first()
            if not inst_ind:
                logger.info(f"No se encontró Instrucción Individual para '{item_data['name']}'. Creando registro inicial...")
                
                new_inst_ind = InstruccionesIndividuales(
                    name=item_data['name'],
                    descripcion=item_data['descripcion'],
                    ejemplo_consulta=item_data.get('ejemplo_consulta'),
                    # Aseguramos que sea un diccionario para json.dumps
                    relaciones_clave=json.dumps(item_data.get('relaciones_clave', {}), ensure_ascii=False) 
                )
                db.session.add(new_inst_ind)
            else:
                logger.info(f"Instrucción Individual para '{item_data['name']}' ya existe. No se realizaron cambios.")
        
        db.session.commit()
        logger.info("Carga de instrucciones de IA para Data Mentor completada.")

    except Exception as e:
        db.session.rollback()
        logger.error(f"Error durante la carga de instrucciones de IA: {e}", exc_info=True)