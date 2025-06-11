import requests
from bs4 import BeautifulSoup
import re
import tempfile
import pandas as pd
from io import BytesIO
import json

def obtener_sesskey(html):
    soup = BeautifulSoup(html, 'html.parser')
    sesskey_link = soup.find('a', href=re.compile(r'/login/logout.php\?sesskey='))
    if sesskey_link:
        sesskey_url = sesskey_link['href']
        sesskey = re.search(r'sesskey=([a-zA-Z0-9]+)', sesskey_url)
        if sesskey:
            return sesskey.group(1)
    return None

def exportar_reporte_json(username, password):
    session = requests.Session()
    print("Utils inciando. Entrando y recuperando token inicial...")

    # Paso 1: Obtener el logintoken
    login_page_url = "https://www.campuscomercialypf.com/login/index.php"
    login_page_response = session.get(login_page_url)
    login_page_soup = BeautifulSoup(login_page_response.text, 'html.parser')
    logintoken_input = login_page_soup.find('input', {'name': 'logintoken'})
    logintoken = logintoken_input['value'] if logintoken_input else None
    print("token recuperado. Iniciando login")

    # Paso 2: Realizar el inicio de sesión
    login_payload = {
        "username": username,
        "password": password,
        "logintoken": logintoken,
        "anchor": ""
    }
    login_headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    login_response = session.post(login_page_url, data=login_payload, headers=login_headers)

    if login_response.status_code == 200 and "TotaraSession" in session.cookies:
        print("Inicio de sesión exitoso")
    else:
        print("Error en el inicio de sesión")
        return None

    # Paso 3: Obtener el sesskey dinámicamente desde la página
    dashboard_url = "https://www.campuscomercialypf.com/totara/reportbuilder/report.php?id=133"
    dashboard_response = session.get(dashboard_url)
    dashboard_html = dashboard_response.text
    sesskey = obtener_sesskey(dashboard_html)

    if not sesskey:
        print("Error: No se pudo obtener el sesskey")
        return None
    print("sESSION KEY - Recuperado, recuperando reporte...")

    # Paso 4: Traer los datos en excel
    export_payload = {
        "sesskey": sesskey,
        "_qf__report_builder_export_form": "1",
        "format": "excel",
        "export": "Exportar"
    }
    export_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": dashboard_url
    }

    export_response = session.post(dashboard_url, data=export_payload, headers=export_headers)
    print("ESTE ES EL EXPORT RESPONSE: ", export_response)

    
    if export_response.status_code == 200:
        print("Excel recuperado. Transformando a json...")

        # Leer el archivo Excel y convertir a JSON
        excel_data = BytesIO(export_response.content)
        df = pd.read_excel(excel_data, engine='openpyxl')
        json_data = df.to_json(orient='records')  # Convertir DataFrame a JSON
        print("Enviando json de utils a la ruta...")
        return json_data

    else:
        print("Error en la exportación")
        return None