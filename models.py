from database import db
from datetime import datetime
import hashlib


class User(db.Model):
    dni = db.Column(db.Integer, primary_key=True)
    id = db.Column(db.Integer)
    name = db.Column(db.String(50))
    email = db.Column(db.String(100), unique=True)
    password = db.Column(db.String(255))
    url_image = db.Column(db.String(255))
    admin = db.Column(db.Boolean)

class Permitido(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    dni = db.Column(db.Integer, db.ForeignKey('user.id'))


class Reporte(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    report_url = db.Column(db.String(255), nullable=False)
    data = db.Column(db.LargeBinary, nullable=False)
    size = db.Column(db.Float, nullable=False)
    elapsed_time = db.Column(db.String(50), nullable=True)
    title = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow) # revisar si .UTC va o si cambiamos a .utcnow

class TodosLosReportes(db.Model):
    id = db.Column(db.Integer, primary_key=True)  # Primary Key
    report_url = db.Column(db.String(255), unique=True, nullable=False)  # La URL del reporte
    title = db.Column(db.String(255), nullable=False)  # El título del reporte
    size_megabytes = db.Column(db.Float, nullable=True)  # El tamaño del reporte en megabytes, puede ser NULL si no está disponible
    created_at = db.Column(db.DateTime, nullable=True)  # La fecha de creación, puede ser NULL si no está disponible

class Survey(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    data = db.Column(db.LargeBinary, nullable=False)

class SegundoSurvey(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    data = db.Column(db.LargeBinary, nullable=False)

class TercerSurvey(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    data = db.Column(db.LargeBinary, nullable=False)

class CuartoSurvey(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    data = db.Column(db.LargeBinary, nullable=False)

class QuintoSurvey(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    data = db.Column(db.LargeBinary, nullable=False)

class TotalComents(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    data = db.Column(db.LargeBinary, nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)

class AllApiesResumes(db.Model):
    __tablename__ = 'archivo_resumido'
    id = db.Column(db.Integer, primary_key=True)
    archivo_binario = db.Column(db.LargeBinary)

class AllCommentsWithEvaluation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    archivo_binario = db.Column(db.LargeBinary)


class FilteredExperienceComments(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    archivo_binario = db.Column(db.LargeBinary)


class DailyCommentsWithEvaluation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    archivo_binario = db.Column(db.LargeBinary)

# TABLAS PARA GUARDAR NECESIDADES>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

class TopicoNecesidad(db.Model):
    __tablename__ = 'topico_necesidad'
    id = db.Column(db.Integer, primary_key=True)
    nombre_topico = db.Column(db.String(255), unique=True, nullable=False)

class ComentarioNecesidad(db.Model):
    __tablename__ = 'comentario_necesidad'
    id = db.Column(db.Integer, primary_key=True)
    id_unico = db.Column(db.String(255), unique=True, nullable=False)
    fecha = db.Column(db.Date, nullable=False)
    api_es = db.Column(db.Integer, nullable=False)
    comentario = db.Column(db.Text, nullable=False)
    canal = db.Column(db.String(50), nullable=False)
    sentimiento = db.Column(db.String(50), nullable=False)
    topico = db.Column(db.String(255), nullable=True)
    topico_necesidad_id = db.Column(db.Integer, db.ForeignKey('topico_necesidad.id'))
    topico_rel = db.relationship('TopicoNecesidad')
    user_id = db.Column(db.Integer, nullable=False) 

class ProcesoNecesidadesEstado(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    total = db.Column(db.Integer, nullable=False)
    done = db.Column(db.Integer, default=0)
    finish = db.Column(db.Boolean, default=False)
    creado_en = db.Column(db.DateTime, default=datetime.utcnow)

# TABLAS PARA GUARDAR REPORTES EN SQL>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>


class Usuarios_Por_Asignacion(db.Model):
    __tablename__ = 'usuarios_por_asignacion'

    id = db.Column(db.Integer, primary_key=True)
    id_asignacion    = db.Column(db.String(50),  nullable=True, default="")
    dni              = db.Column(db.String(20),  nullable=True, default="")
    nombre_completo  = db.Column(db.String(255), nullable=True, default="")
    rol_funcion      = db.Column(db.String(255), nullable=True, default="")
    id_pertenencia   = db.Column(db.String(50),  nullable=True, default="")
    pertenencia      = db.Column(db.String(255), nullable=True, default="")
    estatus_usuario  = db.Column(db.String(100), nullable=True, default="")
    fecha_suspension = db.Column(db.DateTime,   nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def serialize(self):
        return {
            'id': self.id,
            'id_asignacion': self.id_asignacion,
            'dni': self.dni,
            'nombre_completo': self.nombre_completo,
            'rol_funcion': self.rol_funcion,
            'id_pertenencia': self.id_pertenencia,
            'pertenencia': self.pertenencia,
            'estatus_usuario': self.estatus_usuario,
            'fecha_suspension': self.fecha_suspension.isoformat() if self.fecha_suspension else None,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

    def __repr__(self):
        return f"<Usuarios_Por_Asignacion(id={self.id}, asignacion={self.id_asignacion})>"
    
class Usuarios_Sin_ID(db.Model):
    __tablename__ = 'usuarios_sin_id'

    id = db.Column(db.Integer, primary_key=True)
    nombre_usuario            = db.Column(db.String(255), nullable=True, default="")
    dni                       = db.Column(db.String(20), nullable=True, default="")
    email                     = db.Column(db.String(255), nullable=True, default="")
    ultimo_inicio_sesion      = db.Column(db.DateTime, nullable=True)
    estatus_usuario           = db.Column(db.String(100), nullable=True, default="")
    ultimo_acceso             = db.Column(db.DateTime, nullable=True)
    fecha_ingreso             = db.Column(db.DateTime, nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def serialize(self):
        return {
            'id': self.id,
            'nombre_usuario': self.nombre_usuario,
            'dni': self.dni,
            'email': self.email,
            'ultimo_inicio_sesion': self.ultimo_inicio_sesion.isoformat() if self.ultimo_inicio_sesion else None,
            'estatus_usuario': self.estatus_usuario,
            'ultimo_acceso': self.ultimo_acceso.isoformat() if self.ultimo_acceso else None,
            'fecha_ingreso': self.fecha_ingreso.isoformat() if self.fecha_ingreso else None,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

    def __repr__(self):
        return f"<Usuarios_Sin_ID(id={self.id}, nombre_usuario={self.nombre_usuario})>"
    
class ValidaUsuarios(db.Model):
    __tablename__ = 'valida_usuarios'

    id = db.Column(db.Integer, primary_key=True)
    nombre_completo       = db.Column(db.String(255), nullable=True, default="")
    ciudad                = db.Column(db.String(100), nullable=True, default="")
    nivel_estudios        = db.Column(db.String(100), nullable=True, default="")
    fecha_nacimiento      = db.Column(db.DateTime, nullable=True)
    traslado_moto         = db.Column(db.String(10), nullable=True, default="")  # puede ser Sí / No
    traslado_bicicleta    = db.Column(db.String(10), nullable=True, default="")  # puede ser Sí / No
    anio_ingreso          = db.Column(db.String(10), nullable=True, default="")
    socio_serviclub       = db.Column(db.String(10), nullable=True, default="")  # puede ser Sí / No
    estatus_usuario       = db.Column(db.String(100), nullable=True, default="")
    dni                   = db.Column(db.String(20), nullable=True, default="")

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def serialize(self):
        return {
            "id": self.id,
            "nombre_completo": self.nombre_completo,
            "ciudad": self.ciudad,
            "nivel_estudios": self.nivel_estudios,
            "fecha_nacimiento": self.fecha_nacimiento.isoformat() if self.fecha_nacimiento else None,
            "traslado_moto": self.traslado_moto,
            "traslado_bicicleta": self.traslado_bicicleta,
            "anio_ingreso": self.anio_ingreso,
            "socio_serviclub": self.socio_serviclub,
            "estatus_usuario": self.estatus_usuario,
            "dni": self.dni,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }

    def __repr__(self):
        return f"<ValidaUsuarios(id={self.id}, nombre_completo={self.nombre_completo})>"
    
class DetalleApies(db.Model):
    __tablename__ = 'detalle_apies'

    id = db.Column(db.Integer, primary_key=True)
    apies              = db.Column(db.String(100), nullable=True, default="")
    apies_razon_social = db.Column(db.String(255), nullable=True, default="")
    cuadro             = db.Column(db.String(100), nullable=True, default="")
    numero_id_padre    = db.Column(db.String(100), nullable=True, default="")
    red                = db.Column(db.String(100), nullable=True, default="")
    region             = db.Column(db.String(100), nullable=True, default="")
    segmento           = db.Column(db.String(100), nullable=True, default="")
    zona               = db.Column(db.String(100), nullable=True, default="")

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def serialize(self):
        return {
            "id": self.id,
            "apies": self.apies,
            "apies_razon_social": self.apies_razon_social,
            "cuadro": self.cuadro,
            "numero_id_padre": self.numero_id_padre,
            "red": self.red,
            "region": self.region,
            "segmento": self.segmento,
            "zona": self.zona,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }

    def __repr__(self):
        return f"<DetalleApies(id={self.id}, apies={self.apies})>"
    
class AvanceCursada(db.Model):
    __tablename__ = 'avance_cursada'

    id = db.Column(db.Integer, primary_key=True)
    apies                    = db.Column(db.String(100), nullable=True, default="")
    apies_razon_social       = db.Column(db.String(255), nullable=True, default="")
    dni                      = db.Column(db.String(20), nullable=True, default="")
    nombre_completo_usuario  = db.Column(db.String(255), nullable=True, default="")
    rol_funcion              = db.Column(db.String(100), nullable=True, default="")
    estatus_usuario          = db.Column(db.String(100), nullable=True, default="")
    nombre_programa          = db.Column(db.String(255), nullable=True, default="")
    nombre_corto_curso       = db.Column(db.String(255), nullable=True, default="")
    estatus_curso            = db.Column(db.String(100), nullable=True, default="")
    fecha_fin_curso          = db.Column(db.DateTime, nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def serialize(self):
        return {
            "id": self.id,
            "apies": self.apies,
            "apies_razon_social": self.apies_razon_social,
            "dni": self.dni,
            "nombre_completo_usuario": self.nombre_completo_usuario,
            "rol_funcion": self.rol_funcion,
            "estatus_usuario": self.estatus_usuario,
            "nombre_programa": self.nombre_programa,
            "nombre_corto_curso": self.nombre_corto_curso,
            "estatus_curso": self.estatus_curso,
            "fecha_fin_curso": self.fecha_fin_curso.isoformat() if self.fecha_fin_curso else None,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }

    def __repr__(self):
        return f"<AvanceCursada(id={self.id}, usuario={self.nombre_completo_usuario})>"
    
class CursadasAgrupadas(db.Model):
    __tablename__ = 'cursadas_agrupadas'

    id = db.Column(db.Integer, primary_key=True)
    dni = db.Column(db.String(20), nullable=False)
    id_curso = db.Column(db.String(100), nullable=False)
    estatus_finalizacion = db.Column(db.String(100), nullable=True, default="")
    fecha_finalizacion = db.Column(db.String(50), nullable=True, default="")
    id_concat = db.Column(db.String(120), unique=True, nullable=False)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def serialize(self):
        return {
            "id": self.id,
            "dni": self.dni,
            "id_curso": self.id_curso,
            "estatus_finalizacion": self.estatus_finalizacion,
            "fecha_finalizacion": self.fecha_finalizacion,
            "id_concat": self.id_concat,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }

    def __repr__(self):
        return f"<CursosNoRetail2025(id={self.id}, id_concat={self.id_concat})>"

class DetallesDeCursos(db.Model):
    __tablename__ = 'detalles_de_cursos'

    id = db.Column(db.Integer, primary_key=True)
    nombre_curso           = db.Column(db.String(255), nullable=True, default="")
    id_curso               = db.Column(db.String(100), nullable=True, default="")
    negocio_solicitante    = db.Column(db.String(255), nullable=True, default="")
    horas_formacion        = db.Column(db.String(50), nullable=True, default="")
    modalidad              = db.Column(db.String(100), nullable=True, default="")
    resumen_curso          = db.Column(db.Text, nullable=True, default="")
    fecha_creacion         = db.Column(db.DateTime, nullable=True)
    visible_oculto         = db.Column(db.String(50), nullable=True, default="")
    capacidad_marco        = db.Column(db.String(100), nullable=True, default="")
    tematica               = db.Column(db.String(100), nullable=True, default="")
    impacto_negocio        = db.Column(db.String(255), nullable=True, default="")
    impacto_segmento       = db.Column(db.String(255), nullable=True, default="")

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def serialize(self):
        return {
            "id": self.id,
            "nombre_curso": self.nombre_curso,
            "id_curso": self.id_curso,
            "negocio_solicitante": self.negocio_solicitante,
            "horas_formacion": self.horas_formacion,
            "modalidad": self.modalidad,
            "resumen_curso": self.resumen_curso,
            "fecha_creacion": self.fecha_creacion.isoformat() if self.fecha_creacion else None,
            "visible_oculto": self.visible_oculto,
            "capacidad_marco": self.capacidad_marco,
            "tematica": self.tematica,
            "impacto_negocio": self.impacto_negocio,
            "impacto_segmento": self.impacto_segmento,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }

    def __repr__(self):
        return f"<DetallesDeCursos(id={self.id}, curso={self.nombre_curso})>"
    
class FormularioGestor(db.Model):
    __tablename__ = 'formulario_gestor'

    id = db.Column(db.Integer, primary_key=True)
    apies = db.Column(db.String(50), nullable=False)
    curso = db.Column(db.String(100), nullable=False)
    fecha_usuario = db.Column(db.Date, nullable=False)
    gestor = db.Column(db.String(100), nullable=False)
    duracion_horas = db.Column(db.Integer, nullable=False)
    objetivo = db.Column(db.Text, nullable=True)
    contenido_desarrollado = db.Column(db.Text, nullable=True)
    ausentes = db.Column(db.Integer, nullable=False)
    presentes = db.Column(db.Integer, nullable=False)
    resultados_logros = db.Column(db.Text, nullable=True)
    compromiso = db.Column(db.String(20), nullable=True)
    participacion_actividades = db.Column(db.String(20), nullable=True)
    concentracion = db.Column(db.String(20), nullable=True)
    cansancio = db.Column(db.String(20), nullable=True)
    interes_temas = db.Column(db.String(20), nullable=True)
    recomendaciones = db.Column(db.Text, nullable=True)
    otros_aspectos = db.Column(db.Text, nullable=True)
    jornada = db.Column(db.String(20), nullable=False)
    dotacion_real_estacion = db.Column(db.Integer, nullable=True)
    dotacion_en_campus = db.Column(db.Integer, nullable=True)
    dotacion_dni_faltantes = db.Column(db.Text, nullable=True)
    firma_file = db.Column(db.LargeBinary, nullable=True)
    nombre_firma = db.Column(db.String(100), nullable=True)
    email_gestor = db.Column(db.String(120), nullable=False)
    creado_en = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    def serialize(self):
        return {
            "id": self.id,
            "apies": self.apies,
            "curso": self.curso,
            "fecha_usuario": self.fecha_usuario.isoformat(),
            "gestor": self.gestor,
            "duracion_horas": self.duracion_horas,
            "objetivo": self.objetivo,
            "contenido_desarrollado": self.contenido_desarrollado,
            "ausentes": self.ausentes,
            "presentes": self.presentes,
            "resultados_logros": self.resultados_logros,
            "compromiso": self.compromiso,
            "participacion_actividades": self.participacion_actividades,
            "concentracion": self.concentracion,
            "cansancio": self.cansancio,
            "interes_temas": self.interes_temas,
            "recomendaciones": self.recomendaciones,
            "otros_aspectos": self.otros_aspectos,
            "jornada": self.jornada,
            "dotacion_real_estacion": self.dotacion_real_estacion,
            "dotacion_dni_faltantes": self.dotacion_dni_faltantes,
            "nombre_firma": self.nombre_firma,
            "email_gestor": self.email_gestor,
            "creado_en": self.creado_en.isoformat()
        }
    
class QuintoSurveySql(db.Model):
    __tablename__ = 'quinto_survey_sql'

    id = db.Column(db.Integer, primary_key=True)
    id_concat = db.Column(db.String(255), unique=True, nullable=False)  # antes era "ID"
    date_created             = db.Column(db.DateTime, nullable=True)
    gestores_aprendizaje     = db.Column('GestoresAprendizaje', db.String(255), nullable=True, default="")
    curso                    = db.Column('Curso', db.String(255), nullable=True, default="")
    recomendacion_colega     = db.Column(
        '¿Qué tan probable es que usted le recomiende este curso a un colega?',
        db.String(255), nullable=True, default=""
    )
    desempeno_instructor     = db.Column(
        'De acuerdo a tu experiencia del día de hoy, ¿Cómo calificarías el desempeño del instructor?',
        db.String(255), nullable=True, default=""
    )
    calificacion_general     = db.Column(
        'En líneas generales, ¿cómo calificarías a este curso/ actividad?',
        db.String(255), nullable=True, default=""
    )
    duracion_curso           = db.Column(
        'Pensando en los contenidos vistos, considerás que la duración del curso fue:',
        db.String(255), nullable=True, default=""
    )
    info_recibida            = db.Column(
        'En cuanto a la información recibida, considerás que es:',
        db.String(255), nullable=True, default=""
    )
    claridad_temas           = db.Column(
        'Los temas fueron tratados con claridad',
        db.String(255), nullable=True, default=""
    )
    utilidad_contenido       = db.Column(
        'El contenido visto es de utilidad para mi tarea',
        db.String(255), nullable=True, default=""
    )
    ayudas_practica          = db.Column(
        'Las explicaciones, guías, videos, etc. ayudan a poner en práctica lo visto en el curso',
        db.String(255), nullable=True, default=""
    )
    actividades_refuerzo     = db.Column(
        'Las actividades propuestas refuerzan lo aprendido',
        db.String(255), nullable=True, default=""
    )
    experiencia_aprendizaje  = db.Column(
        'En líneas generales dirías que tu experiencia de aprendizaje con este curso fue:',
        db.String(255), nullable=True, default=""
    )
    sugerencias              = db.Column(
        'Para finalizar dejamos este espacio para que nos dejes tus sugerencias o comentarios relacionados a este curso',
        db.Text, nullable=True, default=""
    )

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<QuintoSurveySql(id_concat={self.id_concat}, curso={self.curso})>"
    
class CuartoSurveySql(db.Model):
    __tablename__ = 'cuarto_survey_sql'

    id         = db.Column(db.Integer, primary_key=True)
    id_code    = db.Column('ID_CODE', db.String(255), nullable=True)  # si lo querés seguir guardando
    id_concat  = db.Column(db.String(512), unique=True, nullable=False)  # date_modified + ip_address

    # tus campos de encuesta...
    recomendacion_colega    = db.Column(
        '¿Qué tan probable es que usted le recomiende este curso a un colega?',
        db.String(255), nullable=True, default=""
    )
    calificacion_general    = db.Column(
        'En líneas generales, ¿cómo calificarías a este curso/ actividad?',
        db.String(255), nullable=True, default=""
    )
    duracion_curso          = db.Column(
        'Pensando en los contenidos vistos, considerás que la duración del curso fue:',
        db.String(255), nullable=True, default=""
    )
    info_recibida           = db.Column(
        'En cuanto a la información recibida, considerás que es:',
        db.String(255), nullable=True, default=""
    )
    claridad_temas          = db.Column(
        'Los temas fueron tratados con claridad',
        db.String(255), nullable=True, default=""
    )
    utilidad_contenido      = db.Column(
        'El contenido visto es de utilidad para mi tarea',
        db.String(255), nullable=True, default=""
    )
    ayudas_practica         = db.Column(
        'Las explicaciones, guías, videos, etc. ayudan a poner en práctica lo visto en el curso',
        db.String(255), nullable=True, default=""
    )
    actividades_refuerzo    = db.Column(
        'Las actividades propuestas refuerzan lo aprendido',
        db.String(255), nullable=True, default=""
    )
    problema_campus         = db.Column(
        'Al momento de realizar el curso, ¿tuviste algún problema con el Campus de aprendizaje?',
        db.String(255), nullable=True, default=""
    )
    detalle_problema        = db.Column(
        'Si tuviste algún problema, por favor, contanos que sucedió',
        db.Text, nullable=True, default=""
    )
    experiencia_aprendizaje = db.Column(
        'En líneas generales dirías que tu experiencia de aprendizaje con este curso fue:',
        db.String(255), nullable=True, default=""
    )
    sugerencias             = db.Column(
        'Para finalizar dejamos este espacio para que nos dejes tus sugerencias o comentarios relacionados a este curso',
        db.Text, nullable=True, default=""
    )

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<CuartoSurveySql(id_concat={self.id_concat})>"
    

# TABLAS EXPERIENCIA DE CLIENTE

class Comentarios2023(db.Model):
    __tablename__ = 'comentarios_encuesta_2023'

    id = db.Column(db.Integer, primary_key=True)
    fecha = db.Column(db.DateTime, nullable=True)
    apies = db.Column(db.String(255), nullable=True, default="")
    comentario = db.Column(db.Text, nullable=True, default="")
    canal = db.Column(db.String(255), nullable=True, default="")
    topico = db.Column(db.String(255), nullable=True, default="")
    sentiment = db.Column(db.String(50), nullable=True, default="")

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def serialize(self):
        return {
            'id': self.id,
            'fecha': self.fecha.isoformat() if self.fecha else None,
            'apies': self.apies,
            'comentario': self.comentario,
            'canal': self.canal,
            'topico': self.topico,
            'sentiment': self.sentiment,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

    def __repr__(self):
        return f"<ComentarioEncuesta id={self.id} apies={self.apies}>"
    
class Comentarios2024(db.Model):
    __tablename__ = 'comentarios_encuesta_2024'

    id = db.Column(db.Integer, primary_key=True)
    fecha = db.Column(db.DateTime, nullable=True)
    apies = db.Column(db.String(255), nullable=True, default="")
    comentario = db.Column(db.Text, nullable=True, default="")
    canal = db.Column(db.String(255), nullable=True, default="")
    topico = db.Column(db.String(255), nullable=True, default="")
    sentiment = db.Column(db.String(50), nullable=True, default="")

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def serialize(self):
        return {
            'id': self.id,
            'fecha': self.fecha.isoformat() if self.fecha else None,
            'apies': self.apies,
            'comentario': self.comentario,
            'canal': self.canal,
            'topico': self.topico,
            'sentiment': self.sentiment,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

    def __repr__(self):
        return f"<ComentarioEncuesta id={self.id} apies={self.apies}>"


class Comentarios2025(db.Model):
    __tablename__ = 'comentarios_encuesta_2025'

    id = db.Column(db.Integer, primary_key=True)
    fecha = db.Column(db.DateTime, nullable=True)
    apies = db.Column(db.String(255), nullable=True, default="")
    comentario = db.Column(db.Text, nullable=True, default="")
    canal = db.Column(db.String(255), nullable=True, default="")
    topico = db.Column(db.String(255), nullable=True, default="")
    sentiment = db.Column(db.String(50), nullable=True, default="")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    # Nuevo campo hash único
    hash_id = db.Column(db.String(64), unique=True, index=True)

    def serialize(self):
        return {
            'id': self.id,
            'fecha': self.fecha.isoformat() if self.fecha else None,
            'apies': self.apies,
            'comentario': self.comentario,
            'canal': self.canal,
            'topico': self.topico,
            'sentiment': self.sentiment,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'hash_id': self.hash_id
        }

    def __repr__(self):
        return f"<ComentarioEncuesta id={self.id} apies={self.apies}>"

    @staticmethod
    def generar_hash(fecha, apies, comentario, canal):
        """
        Método auxiliar para generar hash_id a partir de los campos clave.
        """
        texto = f"{fecha}|{apies}|{comentario}|{canal}"
        return hashlib.md5(texto.encode('utf-8')).hexdigest()