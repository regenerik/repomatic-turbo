from database import db
from datetime import datetime


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