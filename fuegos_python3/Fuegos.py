# -*- coding: utf-8 -*-
"""
Procesamiento diario de datos de fuegos

- author juanmendez@gkudos.com
- require: python 2.7 Arcgis Desktop

Importante: desde el dia 26 de agosto de 2022 el sensor VNP dejo de operar segun la NASA, por lo tanto para
que el script pueda seguir operando se dejo el archivo que se descarga con los datos de ese sensor vacio
con la misma estrcutura de datos, se encuentra en la siguiente URL, del servidor APACHE en el subdominio
documentos.siatac.co, se configuro en el archivo de configuraciones config_no_vpn_sensor.json, verificar que
el archivo config.json este configurado con esta nueva ruta:
"url_vnp" : "https://documentos.siatac.co/monitoreo_ambiental/SUOMI_VIIRS_C2_South_America_24h_vacio.zip",
si el sensor vuelve a operar por favor usar los urls originales de la NASA:
"url_vnp" : "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/shapes/zips/SUOMI_VIIRS_C2_South_America_24h.zip",
"url_vnp_2" : "https://firms2.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/shapes/zips/SUOMI_VIIRS_C2_South_America_24h.zip",

Se habilito nuevamente el 7 de agosto de 2022
"""

import logging, os, sys, traceback, json, glob, shutil, time, zipfile, smtplib
import arcpy
import requests
import datetime, collections
import pytz
from arcpy import env
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from collections import Counter

################################################################################
################################################################################
'''
imprimir error
'''
def print_error(e):
    logging.error("*** " + str(e.args[0]))
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    logging.error(tbinfo)


##################################################################
##################################################################
'''
crear conexiones de sde
'''
def create_conn(data, username, password, instance, database_name):
    try:
        logging.debug("***********************************")
        temp_dir = data['current_day_temp_dir']
        authType = "DATABASE_AUTH"
        saveUserInfo = "SAVE_USERNAME"
        versionName = "SDE.DEFAULT"
        out_workspace = arcpy.CreateDatabaseConnection_management(temp_dir,
                                                                  username + "_" + database_name, "POSTGRESQL",
                                                                  instance, authType, username, password, saveUserInfo,
                                                                  database_name, "#", "TRANSACTIONAL", versionName)
        conn_path = out_workspace.getOutput(0)
        logging.debug("** conn_path:" + conn_path)
        return conn_path
    except Exception as e:
        print_error(e)
        raise e


##################################################################
##################################################################
'''
http://joshwerts.com/blog/2018/01/16/group-by-query-in-a-file-geodatabase/
'''
def group_by_count(table_or_fc, fields):
    """ Returns dictionary containing count of unique items """
    counter = Counter()
    with arcpy.da.SearchCursor(table_or_fc, fields) as curs:
        for row in curs:
            # no need to store as a tuple if only 1 field, just store the value
            if len(row) == 1:
                row = row[0]
            counter[row] += 1
    return counter


##################################################################
##################################################################
'''
Contar registros de un shapefile dentro de un directorio
'''
def count_shapefile_records(directory):
    """
    Busca el primer archivo .shp en un directorio y cuenta sus registros

    Args:
        directory: Ruta del directorio donde buscar el shapefile

    Returns:
        int: Número de registros, o -1 si no se encuentra shapefile o hay error
    """
    try:
        # Buscar archivos .shp en el directorio
        shp_files = glob.glob(os.path.join(directory, "*.shp"))

        if not shp_files:
            logging.warning("No se encontró archivo .shp en {}".format(directory))
            return -1

        shp_path = shp_files[0]
        logging.debug("Shapefile encontrado: {}".format(shp_path))

        # Contar registros
        count = int(arcpy.GetCount_management(shp_path)[0])
        logging.info("Registros encontrados en shapefile: {}".format(count))

        return count
    except Exception as e:
        logging.error("Error al contar registros del shapefile: {}".format(e))
        return -1


##################################################################
##################################################################
'''
descargar los shps de la nasa
'''
def download_nasa_files(data):
    try:
        logging.debug("***********************************")
        logging.debug("** download_nasa_files **")
        logging.debug("***********************************")
        current_day_temp_dir = data['current_day_temp_dir']
        url_modis = data['url_modis']
        url_vnp = data['url_vnp']
        url_noaa = data['url_noaa']

        url_modis_2 = data['url_modis_2']
        url_vnp_2 = data['url_vnp_2']
        url_noaa_2 = data['url_noaa_2']
        url_noaa_21 = data['url_noaa_21']

        logging.debug("url_modis : {} ".format(url_modis))
        logging.debug("url_vnp : {} ".format(url_vnp))
        logging.debug("url_noaa : {} ".format(url_noaa))

        logging.debug("url_modis_2 : {} ".format(url_modis_2))
        logging.debug("url_vnp_2 : {} ".format(url_vnp_2))
        logging.debug("url_noaa_2 : {} ".format(url_noaa_2))
        logging.debug("url_noaa_21 : {} ".format(url_noaa_21))

        #######################################################################################
        ## MODIS
        #######################################################################################
        # check file
        try:
            logging.debug('check file....')
            r = requests.head(url_modis)
            logging.debug(r.status_code)
            if r.status_code != 200:
                # logging.debug(r.headers['content-type'])
                logging.debug(r.headers)
                logging.debug('switch to server 2....')
                url_modis = url_modis_2
                logging.debug("url_modis : {} ".format(url_modis))

            logging.debug('Beginning file download....')

            modis_zip_path = os.path.join(current_day_temp_dir, "modis.zip")

            logging.debug("**************")
            logging.debug("modis_zip_path : {} ".format(modis_zip_path))
            logging.debug("**************")

            r = requests.get(url_modis)
            logging.debug(r.status_code)
            logging.debug(r.headers['content-type'])
            # logging.debug(r.encoding)
            with open(modis_zip_path, 'wb') as f:
                f.write(r.content)
            with zipfile.ZipFile(modis_zip_path, 'r') as zip_ref:
                zip_ref.extractall(current_day_temp_dir)

            # Validar que el shapefile tenga registros
            record_count = count_shapefile_records(current_day_temp_dir)
            logging.info("MODIS: Registros antes de usar URL alterna: {}".format(record_count))
            
            if record_count == 0:
                logging.warning("MODIS: Shapefile descargado tiene 0 registros. Intentando con URL alterna...")
                # Usar URL alterna
                url_modis = url_modis_2
                logging.info("Descargando desde URL alterna: {}".format(url_modis))

                r = requests.get(url_modis)
                logging.debug(r.status_code)
                logging.debug(r.headers['content-type'])
                with open(modis_zip_path, 'wb') as f:
                    f.write(r.content)
                with zipfile.ZipFile(modis_zip_path, 'r') as zip_ref:
                    zip_ref.extractall(current_day_temp_dir)

                # Validar nuevamente
                record_count = count_shapefile_records(current_day_temp_dir)
                logging.info("MODIS: Registros después de usar URL alterna: {}".format(record_count))
        except Exception as e:
            logging.debug('No se puede descargar información para MODIS, {}'.format(e))

        #######################################################################################
        ## suomi
        #######################################################################################
        # check file
        try:
            logging.debug('check file....')
            r = requests.head(url_vnp)
            logging.debug(r.status_code)
            if r.status_code != 200:
                # logging.debug(r.headers['content-type'])
                logging.debug(r.headers)
                logging.debug('switch to server 2....')
                url_vnp = url_vnp_2
                logging.debug("url_vnp : {} ".format(url_vnp))

            vpn_zip_path = os.path.join(current_day_temp_dir, "vnp.zip")
            logging.debug("**************")
            logging.debug("vpn_zip_path : {} ".format(vpn_zip_path))
            logging.debug("**************")

            logging.debug('Beginning file download....')
            r = requests.get(url_vnp)

            logging.debug(r.status_code)
            logging.debug(r.headers['content-type'])
            # logging.debug(r.encoding)
            with open(vpn_zip_path, 'wb') as f:
                f.write(r.content)
            with zipfile.ZipFile(vpn_zip_path, 'r') as zip_ref:
                zip_ref.extractall(current_day_temp_dir)

            # Validar que el shapefile tenga registros
            record_count = count_shapefile_records(current_day_temp_dir)
            logging.info("SUOMI-NPP: Registros antes de usar URL alterna: {}".format(record_count))
            if record_count == 0:
                logging.warning("SUOMI-NPP: Shapefile descargado tiene 0 registros. Intentando con URL alterna...")
                # Usar URL alterna
                url_vnp = url_vnp_2
                logging.info("Descargando desde URL alterna: {}".format(url_vnp))

                r = requests.get(url_vnp)
                logging.debug(r.status_code)
                logging.debug(r.headers['content-type'])
                with open(vpn_zip_path, 'wb') as f:
                    f.write(r.content)
                with zipfile.ZipFile(vpn_zip_path, 'r') as zip_ref:
                    zip_ref.extractall(current_day_temp_dir)

                # Validar nuevamente
                record_count = count_shapefile_records(current_day_temp_dir)
                logging.info("SUOMI-NPP: Registros después de usar URL alterna: {}".format(record_count))
        except Exception as e:
            logging.debug('No se puede descargar información para suomi, {}'.format(e))

        #######################################################################################
        ## noaa
        #######################################################################################
        # check file
        try:
            logging.debug('check file....')
            r = requests.head(url_noaa)
            logging.debug(r.status_code)
            if r.status_code != 200:
                # logging.debug(r.headers['content-type'])
                logging.debug(r.headers)
                logging.debug('switch to server 2....')
                url_noaa = url_noaa_2
                logging.debug("url_noaa : {} ".format(url_noaa))

            logging.debug('Beginning file download....')
            r = requests.get(url_noaa)

            noaa_zip_path = os.path.join(current_day_temp_dir, "noaa.zip")
            logging.debug("**************")
            logging.debug("noaa_zip_path : {} ".format(noaa_zip_path))
            logging.debug("**************")

            logging.debug(r.status_code)
            logging.debug(r.headers['content-type'])
            # logging.debug(r.encoding)
            with open(noaa_zip_path, 'wb') as f:
                f.write(r.content)
            with zipfile.ZipFile(noaa_zip_path, 'r') as zip_ref:
                zip_ref.extractall(current_day_temp_dir)

            # Validar que el shapefile tenga registros
            record_count = count_shapefile_records(current_day_temp_dir)
            logging.info("NOAA-20: Registros antes de usar URL alterna: {}".format(record_count))
            if record_count == 0:
                logging.warning("NOAA-20: Shapefile descargado tiene 0 registros. Intentando con URL alterna...")
                # Usar URL alterna
                url_noaa = url_noaa_2
                logging.info("Descargando desde URL alterna: {}".format(url_noaa))

                r = requests.get(url_noaa)
                logging.debug(r.status_code)
                logging.debug(r.headers['content-type'])
                with open(noaa_zip_path, 'wb') as f:
                    f.write(r.content)
                with zipfile.ZipFile(noaa_zip_path, 'r') as zip_ref:
                    zip_ref.extractall(current_day_temp_dir)

                # Validar nuevamente
                record_count = count_shapefile_records(current_day_temp_dir)
                logging.info("NOAA-20: Registros después de usar URL alterna: {}".format(record_count))
        except Exception as e:
            logging.debug('No se puede descargar información para noaa, {}'.format(e))

        #######################################################################################
        ## noaa-21
        #######################################################################################
        # check file
        try:
            logging.debug('check file....')
            r = requests.head(url_noaa_21)
            logging.debug(r.status_code)
            if r.status_code != 200:
                # logging.debug(r.headers['content-type'])
                logging.debug(r.headers)
                logging.debug('switch to server 2.... the same')
                logging.debug("url_noaa_21 : {} ".format(url_noaa_21))

            logging.debug('Beginning file download....')
            r = requests.get(url_noaa_21)

            noaa21_zip_path = os.path.join(current_day_temp_dir, "noaa_21.zip")
            logging.debug("**************")
            logging.debug("noaa21_zip_path : {} ".format(noaa21_zip_path))
            logging.debug("**************")

            logging.debug(r.status_code)
            logging.debug(r.headers['content-type'])

            with open(noaa21_zip_path, 'wb') as f:
                f.write(r.content)
            with zipfile.ZipFile(noaa21_zip_path, 'r') as zip_ref:
                zip_ref.extractall(current_day_temp_dir)

            # Validar que el shapefile tenga registros
            record_count = count_shapefile_records(current_day_temp_dir)
            logging.info("NOAA-21: Registros antes de usar URL alterna: {}".format(record_count))
            if record_count == 0:
                logging.warning("NOAA-21: Shapefile descargado tiene 0 registros.")
                logging.info("NOAA-21 no tiene URL alterna configurada. Continuando con archivo vacío.")
        except Exception as e:
            logging.debug('No se puede descargar información para noaa 21, {}'.format(e))

        #######################################################################################
        ## FIND SHPS
        #######################################################################################
        logging.debug("***********************************")
        for shp in glob.iglob(os.path.join(current_day_temp_dir, '*.shp')):
            logging.debug(shp)
            if "MODIS".lower() in shp.lower():
                data['shp_modis'] = shp
            elif "SUOMI_VIIRS".lower() in shp.lower():
                data['shp_vnp'] = shp
            elif "J1_VIIRS".lower() in shp.lower():
                data['shp_noaa'] = shp
            elif "J2_VIIRS".lower() in shp.lower():
                data['shp_noaa_21'] = shp
        #######################################################################################
        #######################################################################################
        logging.debug("***********************************")
    except Exception as e:
        print_error(e)
        # raise e


##################################################################
##################################################################
'''
descargar / reintentar  descarga de los shps de la nasa
'''
def download_shps(data):
    logging.debug("***********************************")
    logging.debug("** download_shps **")
    logging.debug("***********************************")
    max_retries = data['max_retries']
    delay_seconds = data['delay_seconds']
    logging.debug("max_retries : {} ".format(max_retries))
    logging.debug("delay_seconds : {} ".format(delay_seconds))

    data['shp_modis'] = ""
    data['shp_vnp'] = ""
    data['shp_noaa'] = ""
    data['shp_noaa_21'] = ""

    i = 0
    while i < max_retries:
        logging.debug("try download # : {} ".format(i))
        try:
            download_nasa_files(data)
            logging.debug("***********************************")
            logging.debug("shp_modis : {} ".format(data['shp_modis']))
            logging.debug("shp_vnp : {} ".format(data['shp_vnp']))
            logging.debug("shp_noaa : {} ".format(data['shp_noaa']))
            logging.debug("shp_noaa_21 : {} ".format(data['shp_noaa_21']))
            logging.debug("***********************************")

        except Exception as e:
            print_error(e)

        # AVera - 20231211, Se ajusta el siguiente condicional para que intente nuevamento solo cuando fallan los cuatro sensores.
        if data['shp_modis'] == "" and data['shp_vnp'] == "" and data['shp_noaa'] == "" and data['shp_noaa_21'] == "":
            logging.debug("sleep begin...")
            time.sleep(delay_seconds)
            logging.debug("sleep end...")
            i += 1
        else:
            break

    logging.debug("***********************************")
    logging.debug("** Local shps : *")
    logging.debug("***********************************")
    logging.debug("shp_modis : {} ".format(data['shp_modis']))
    logging.debug("shp_vnp : {} ".format(data['shp_vnp']))
    logging.debug("shp_noaa : {} ".format(data['shp_noaa']))
    logging.debug("shp_noaa_21 : {} ".format(data['shp_noaa_21']))
    logging.debug("***********************************")

    # AVera - 20231211, Se ajusta el siguiente condicional para que levante la excepcion solo cuando fallan los cuatro sensores.
    if data['shp_modis'] == "" and data['shp_vnp'] == "" and data['shp_noaa'] == "" and data['shp_noaa_21'] == "":
        raise Exception('ERROR_001 - No se pudo descargar shps de la nasa')
    logging.debug("***********************************")


##################################################################
##################################################################
'''
Enviar email utilizando gmail
'''
def send_email(data, to, subject, message, bcc=[]):
    logging.debug("***********************************")
    logging.debug("** send_email ")
    logging.debug("***********************************")
    logging.debug("subject : {} ".format(subject))
    logging.debug("to : {} ".format(to))
    logging.debug("bcc : {} ".format(bcc))

    gmail_user = data["gmail_user"]
    gmail_password = data["gmail_password"]

    '''
    Importante: a partir de junio de 2020 Google restringe el acceso a gmail por aplicaciones
    no seguras, se debio habilitar factor de atenticacion 2 telefono administrador Andres Diaz,
    y una contrasena especifica para esta la aplicacion aplicacion_focos_smtp, la contrasena es
    'psvyagohgszbyigo', se pone en el campo password del cliente smtplib.SMTP_SSL como se muestra a continuacion
    se debe usar cliente seguro SSL
    '''

    server = smtplib.SMTP_SSL('smtp-relay.gmail.com', 465, timeout=120)
    server.ehlo()
    server.login(gmail_user, gmail_password)

    for email in to:
        logging.debug(email)
        try:
            msg = MIMEMultipart()
            msg['From'] = gmail_user
            msg['To'] = email
            msg['Subject'] = subject
            # msg['Bcc'] = ', '.join(bcc)
            # text = """\n{}\n""".format(message)

            html = """\
            <html>
            <body style="font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;" >
                <p>
                <br/>
                {}
                <br/>
                </p>
                <div>                    
					<table>
					<tr>
					<td>
					<img src="http://siatac.co/image/image_gallery?uuid=b9aa63b4-d60d-4765-9179-f24054d059da&groupId=755&t=1579281579021" height="47" width="68" float="left" class="CToWUd a6T" tabindex="0">
					</td>
					<td>
					<font size = "1">Para conocer la ubicación de los puntos de calor reportados, ingrese al mapa interactivo del sistema de monitoreo (SINCHI)</font>
					<a href="https://experience.arcgis.com/stemapp/ceb7f423780c410389ca35fc0990e7e4" target="_blank" >Aquí.</a> 
					<br/>
					<font size = "1">Para mayor detalle ingrese al sistema de monitoreo de puntos de calor y cicatrices de quema (SINCHI)</font>
					<a href="http://siatac.co/web/guest/productos/monitoreo-fuegos" target="_blank" >Aquí</a>
					</td>					
					</tr>
				</table>
				<table>
				<tr>
				<td style="width: 68px;">
				<img src="http://siatac.co/image/image_gallery?uuid=0d3c9e99-5a22-4ba8-8c6d-485032d8b6d7&groupId=755&t=1579281543068" height="37" width="46" float="left" class="CToWUd a6T" tabindex="0">
				</td>
				<td>
				<font size = "1">Para revisar el sistema de monitoreo de puntos activos de calor en Colombia (IDEAM)</font>
				<a href="http://puntosdecalor.ideam.gov.co/" target="_blank" >Aquí</a><br>
				<font size = "1">Para revisar el sistema de pronostico y alertas en Colombia (IDEAM)</font>
				<a href="http://www.pronosticosyalertas.gov.co/alertabig-portlet/html/alertabig/view.jsp" target="_blank" >Aquí</a><br>
				<td>		
				</tr>
				</table>
				<br/>
                    <font size = "2">Para dejar de recibir este servicio puede enviar un correo a </font>
                    <a href="mailto:siatac@sinchi.org.co" target="_blank">siatac@sinchi.org.co</a>
                <br/><br/><br/>
                    <img src="http://siatac.co:81/images/ui/Logo_Mail.png" 
                height="116" width="589" class="CToWUd a6T" tabindex="0">
                </div>

            </body>
			</html>
                """.format(message)

            # part1 = MIMEText(text, "plain")
            part2 = MIMEText(html, "html")
            # msg.attach(part1)
            msg.attach(part2)

            server.sendmail(
                gmail_user, [email] + bcc, msg.as_string()
            )

            logging.debug("Email sent!")
        except Exception as e:
            print_error(e)
            logging.debug("Something went wrong sending email.")
    server.close()
    logging.debug("***********************************")


##################################################################
##################################################################
'''
Creación de conexiones SDE a las bases de datos
'''
def create_sde_connections(data):
    logging.debug("***********************************")
    try:
        ## Read
        username = data['user_reader']
        password = data['user_reader_pwd']
        instance = data['prod_instance']
        database_name = data['prod_database_name']
        reader_conn = create_conn(data, username, password, instance, database_name)
        data['reader_conn_prod_instance'] = reader_conn

        username = data['pub_user_reader']
        password = data['pub_user_reader_pwd']
        instance = data['pub_instance']
        database_name = data['pub_database_name']
        reader_conn = create_conn(data, username, password, instance, database_name)
        data['reader_conn_pub_instance'] = reader_conn

        ## write
        username = data['prod_user_edit']
        password = data['prod_user_edit_pwd']
        instance = data['prod_edit_instance']
        database_name = data['prod_database_name']
        reader_conn = create_conn(data, username, password, instance, database_name)
        data['edit_conn_prod_instance'] = reader_conn

        username = data['pub_user_edit']
        password = data['pub_user_edit_pwd']
        instance = data['pub_edit_instance']
        database_name = data['pub_database_name']
        reader_conn = create_conn(data, username, password, instance, database_name)
        data['edit_conn_pub_instance'] = reader_conn
    except Exception as e:
        print_error(e)
        raise Exception('ERROR_002 - No se pudo crear las conexiones SDE a las bases de datos')
    logging.debug("***********************************")


##################################################################
##################################################################
'''
Validación datos de entrada SDE
'''
def validate_input_data(data):
    logging.debug("***********************************")
    logging.debug("** validate_input_data **")
    logging.debug("***********************************")
    try:
        ##################################################################
        reader_conn = data['reader_conn_prod_instance']

        layer = data['layer_hidrocarburos']
        if data["is_test"]:
            layer = "\\" + get_last_portion(layer)
        feature_path = reader_conn + layer
        result = int(arcpy.GetCount_management(feature_path)[0])
        logging.debug('{} has {} records'.format(layer, result))
        #if result == 0:
        #    raise Exception("No se encontraron datos en la capa {} ".format(layer))
        data['feature_hidrocarburos'] = feature_path

        layer = data['layer_union_ent_ref']
        if data["is_test"]:
            layer = "\\" + get_last_portion(layer)
        feature_path = reader_conn + layer
        result = int(arcpy.GetCount_management(feature_path)[0])
        logging.debug('{} has {} records'.format(layer, result))
        #if result == 0:
        #    raise Exception("No se encontraron datos en la capa {} ".format(layer))
        data['feature_union_ent_ref'] = feature_path

        layer = data['layer_dlim']
        if data["is_test"]:
            layer = "\\" + get_last_portion(layer)
        feature_path = reader_conn + layer
        result = int(arcpy.GetCount_management(feature_path)[0])
        logging.debug('{} has {} records'.format(layer, result))
        #if result == 0:
        #    raise Exception("No se encontraron datos en la capa {} ".format(layer))
        data['feature_dlim'] = feature_path

        reader_conn = data['reader_conn_pub_instance']

        layer = data['layer_usuarios_emails']
        if data["is_test"]:
            layer = "\\" + get_last_portion(layer)
        feature_path = reader_conn + layer
        result = int(arcpy.GetCount_management(feature_path)[0])
        logging.debug('{} has {} records'.format(layer, result))
        #if result == 0:
        #    raise Exception("No se encontraron datos en la capa {} ".format(layer))
        data['feature_usuarios_emails'] = feature_path

        ##################################################################
        edit_conn = data['edit_conn_prod_instance']

        layer = data['layer_output_prod']
        if data["is_test"]:
            layer = "\\" + get_last_portion(layer)
        feature_path = edit_conn + layer
        result = int(arcpy.GetCount_management(feature_path)[0])
        logging.debug('{} has {} records'.format(layer, result))
        #if result == 0:
        #    raise Exception("No se encontraron datos en la capa {} ".format(layer))
        data['feature_output_prod'] = feature_path
        data['total_fuegos_historicos_prod'] = result

        edit_conn = data['edit_conn_pub_instance']

        layer = data['layer_output_pub']
        if data["is_test"]:
            layer = "\\" + get_last_portion(layer)
        feature_path = edit_conn + layer
        result = int(arcpy.GetCount_management(feature_path)[0])
        logging.debug('{} has {} records'.format(layer, result))
        #if result == 0:
        #    raise Exception("No se encontraron datos en la capa {} ".format(layer))
        data['feature_output_pub'] = feature_path
        data['total_fuegos_historicos_pub'] = result

        layer_sirgas = data['layer_output_pub_sirgas']
        if data["is_test"]:
            layer_sirgas = "\\" + get_last_portion(layer_sirgas)
        feature_path_sirgas = edit_conn + layer_sirgas
        result_sirgas = int(arcpy.GetCount_management(feature_path_sirgas)[0])
        logging.debug('{} has {} records'.format(layer_sirgas, result_sirgas))
        #if result_sirgas == 0:
        #    raise Exception("No se encontraron datos en la capa {} ".format(layer_sirgas))
        data['feature_output_pub_sirgas'] = feature_path_sirgas
        data['total_fuegos_historicos_pub_sirgas'] = result_sirgas
        ##################################################################
    except Exception as e:
        print_error(e)
        raise Exception('ERROR_003 - Error al Validar Datos : {} '.format(e))

    logging.debug("***********************************")

##################################################################
##################################################################
'''
Valida la existencia de un atributo
'''
def field_exists(layer_name, field_name):
    if arcpy.Exists(layer_name):
        return len(arcpy.ListFields(layer_name, field_name)) > 0

##################################################################
##################################################################
'''
Procesamiento de datos
'''
def process_data(data):
    logging.debug("*********************************************")
    logging.debug("*********************************************")
    logging.debug("** process_data **")
    logging.debug("*********************************************")
    logging.debug("*********************************************")

    try:
        current_day_temp_dir = data['current_day_temp_dir']
        feature_hidrocarburos = data['feature_hidrocarburos']
        feature_dlim = data['feature_dlim']
        feature_union_ent_ref = data['feature_union_ent_ref']

        '''
        ##############################################################################
        ##############################################################################
        # Ruta para prueba local
        logging.debug("********************************************************************")
        logging.debug("********************************************************************")
        logging.debug("*** WARNING: COPY TEST DATA ****************************************")
        logging.debug("********************************************************************")
        logging.debug("********************************************************************")
        import glob 
        temp_mod  =  "c:\\temp\\2020-06-04_15-06_data\\MODIS_C6_South_America_24h.shp"
        temp_vnp  =  "c:\\temp\\2020-06-04_15-06_data\\VNP14IMGTDL_NRT_South_America_24h.shp"

        logging.debug(" delete files.......  " )
        fileList = glob.glob( current_day_temp_dir + '//MODIS_C6_South_America_24h.*')
        for filePath in fileList:
            try:
                logging.debug(" filePath : {}  ".format( filePath )  )
                os.remove(filePath)
            except:
                logging.error("Error while deleting file : ", filePath)

        fileList = glob.glob( current_day_temp_dir + '//VNP14IMGTDL_NRT_South_America_24h.*')
        for filePath in fileList:
            try:
                logging.debug(" filePath : {}  ".format( filePath )  )
                os.remove(filePath)
            except:
                logging.error("Error while deleting file : ", filePath)

        logging.debug(" copy files.......  " )
        source_test_files = "C:\\temp\\temp_data\\"
        from os import listdir
        from os.path import isfile, join
        temp_files = [f for f in listdir(source_test_files) if isfile(join(source_test_files, f))]
        logging.debug(temp_files )
        for filename in temp_files:
            filename =   os.path.join( source_test_files , filename)
            logging.debug(" filename : {}  ".format( filename )  )
            shutil.copy(filename, current_day_temp_dir)
        logging.debug("********************************************************************")
        logging.debug("********************************************************************")
        ##############################################################################
        ##############################################################################
        '''

        shp_modis = data['shp_modis']
        shp_vnp = data['shp_vnp']
        shp_nooa = data['shp_noaa']
        shp_nooa_21 = data['shp_noaa_21']

        logging.debug(" shp_modis : {}  ".format(shp_modis))
        logging.debug(" shp_vnp : {}  ".format(shp_vnp))
        logging.debug(" shp_nooa : {}  ".format(shp_nooa))
        logging.debug(" shp_nooa_21 : {}  ".format(shp_nooa_21))

        ## fgdb para almacenamiento temporal de datos durante la ejecución del modelo
        fgdb_name = "Output.gdb"
        temp_fgdb = current_day_temp_dir + '\\' + fgdb_name
        arcpy.CreateFileGDB_management(current_day_temp_dir, fgdb_name)

        # Espacio de trabajo por default para el geoprocesamiento temporal
        env.workspace = temp_fgdb
        '''
        El 23 de febrero de 2022 se solicito incluir el campo confidence,
        el proceso antes de esa fecha eliminaba el atributo usando las siguientes lineas
        de codigo:

        logging.debug("** delete fields... **")

        # Se elimina el atributo CONFIDENCE de Insumo_MODIS
        dropFields = ["CONFIDENCE"]
        if field_exists(shp_modis, "CONFIDENCE"):
            arcpy.DeleteField_management(shp_modis, dropFields)

        # Se elimina el atributo CONFIDENCE de Insumo_VIIRS
        if field_exists(shp_vnp, "CONFIDENCE"):
            arcpy.DeleteField_management(shp_vnp, dropFields)

        # Se elimina el atributo CONFIDENCE de Insumo NOOA
        if field_exists(shp_nooa, "CONFIDENCE"):
            arcpy.DeleteField_management(shp_nooa, dropFields)

        logging.debug("** add fields... **")
        '''
        # Copy shapefiles inputs to workspace fgdb
        shp_modis_1 = "shp_modis_1"
        shp_vnp_1 = "shp_vnp_1"
        shp_nooa_1 = "shp_nooa_1"
        shp_nooa_21_1 = "shp_nooa_21_1"
        merge_list = []

        # AVera - 20231211, Se crea un condicional por cada shp de cada sensor para que si este existe lo tenga analisis para el resto del anlisis
        # ADiaz - 20240301 Se altera la longitud del campo satellite para poder manejar nombres largos de las siglas del satelite que asigna la NASA

        if shp_modis and arcpy.Exists(shp_modis):
            arcpy.Select_analysis(shp_modis, "shp_modis_1")
            # Ajuste campo satellite
            arcpy.AlterField_management(shp_modis_1, "SATELLITE", "SATELLITEOLD")
            arcpy.AddField_management(shp_modis_1, "SATELLITE", "TEXT", "", "", "256", "", "NULLABLE", "NON_REQUIRED",
                                      "")
            arcpy.CalculateField_management(shp_modis_1, "SATELLITE", "!SATELLITEOLD!", "PYTHON3", "")
            arcpy.DeleteField_management(shp_modis_1, "SATELLITEOLD")
            # Se adiciona el atributo INSTRUMENT a  Insumo_MODIS y se le asigna el valor MODIS
            arcpy.AddField_management(shp_modis_1, "INSTRUMENT", "STRING")
            expression = "'MODIS'"
            arcpy.CalculateField_management(shp_modis_1, "INSTRUMENT", expression, "PYTHON3")

            # Se cambia el nombre del atributo confidence a confidence_modis para diferenciar de confidence, donde se
            # almacenara su valor en datos categoricos no numericos
            arcpy.AlterField_management(shp_modis_1, 'CONFIDENCE', 'confidence_modis', 'confidence_modis')

            # Se agrega el campo confidence a modis para almacenar sus valores categoricos
            arcpy.AddField_management(shp_modis_1, "CONFIDENCE", "STRING")

            # Se calcula confidense en MODIS de acuerdo a sus valores categoricos en la documentacion MODIS
            expression = "getClass(!confidence_modis!)"
            codeblock = """def getClass(confidence_modis):
    confidence = confidence_modis
    if confidence < 30:
        return 'low'
    elif confidence >= 30 and confidence < 80:
        return 'nominal'
    elif confidence >= 80 and confidence <= 100:
        return 'high'"""
            arcpy.CalculateField_management(shp_modis_1, "CONFIDENCE", expression, "PYTHON3",
                                            codeblock)
            merge_list.append(shp_modis_1)

        if shp_vnp and arcpy.Exists(shp_vnp):
            arcpy.Select_analysis(shp_vnp, "shp_vnp_1")
            # Ajuste campo satellite
            arcpy.AlterField_management(shp_vnp_1, "SATELLITE", "SATELLITEOLD")
            arcpy.AddField_management(shp_vnp_1, "SATELLITE", "TEXT", "", "", "256", "", "NULLABLE", "NON_REQUIRED", "")
            arcpy.CalculateField_management(shp_vnp_1, "SATELLITE", "!SATELLITEOLD!", "PYTHON3", "")
            arcpy.DeleteField_management(shp_vnp_1, "SATELLITEOLD")

            # Se adiciona el atributo INSTRUMENT a  Insumo_VIIRS y se le asigna el valor VIIRS_SOUMI
            arcpy.AddField_management(shp_vnp_1, "INSTRUMENT", "STRING")
            expression = "'VIIRS_SOUMI'"
            arcpy.CalculateField_management(shp_vnp_1, "INSTRUMENT", expression, "PYTHON3")

            # Se adiciona el atributo confidense_modis a Insumo_VIIRS para ser compatible en el merge posterior
            arcpy.AddField_management(shp_vnp_1, "confidence_modis", "Double")
            merge_list.append(shp_vnp_1)

        if shp_nooa and arcpy.Exists(shp_nooa):
            arcpy.Select_analysis(shp_nooa, "shp_nooa_1")
            # Ajuste campo satellite
            arcpy.AlterField_management(shp_nooa_1, "SATELLITE", "SATELLITEOLD")
            arcpy.AddField_management(shp_nooa_1, "SATELLITE", "TEXT", "", "", "256", "", "NULLABLE", "NON_REQUIRED",
                                      "")
            arcpy.CalculateField_management(shp_nooa_1, "SATELLITE", "!SATELLITEOLD!", "PYTHON3", "")
            arcpy.DeleteField_management(shp_nooa_1, "SATELLITEOLD")

            # Se adiciona el atributo INSTRUMENT a  Insumo_VIIRS y se le asigna el valor VIIRS_NOAA
            arcpy.AddField_management(shp_nooa_1, "INSTRUMENT", "STRING")
            expression = "'VIIRS_NOAA'"
            arcpy.CalculateField_management(shp_nooa_1, "INSTRUMENT", expression, "PYTHON3")

            # Se adiciona el atributo confidense_modis a Insumo_VIIRS para ser compatible en el merge posterior
            arcpy.AddField_management(shp_nooa_1, "confidence_modis", "Double")
            merge_list.append(shp_nooa_1)

        if shp_nooa_21 and arcpy.Exists(shp_nooa_21):
            arcpy.Select_analysis(shp_nooa_21, "shp_nooa_21_1")
            # Ajuste campo satellite
            arcpy.AlterField_management(shp_nooa_21_1, "SATELLITE", "SATELLITEOLD")
            arcpy.AddField_management(shp_nooa_21_1, "SATELLITE", "TEXT", "", "", "256", "", "NULLABLE", "NON_REQUIRED","")
            arcpy.CalculateField_management(shp_nooa_21_1, "SATELLITE", "!SATELLITEOLD!", "PYTHON3", "")
            arcpy.DeleteField_management(shp_nooa_21_1, "SATELLITEOLD")

            # Se adiciona el atributo INSTRUMENT a  Insumo_VIIRS y se le asigna el valor VIIRS_NOAA
            arcpy.AddField_management(shp_nooa_21_1, "INSTRUMENT", "STRING")
            expression = "'VIIRS_NOAA_21'"
            arcpy.CalculateField_management(shp_nooa_21_1, "INSTRUMENT", expression, "PYTHON3")

            # Se adiciona el atributo confidense_modis a Insumo_VIIRS para ser compatible en el merge posterior
            arcpy.AddField_management(shp_nooa_21_1, "confidence_modis", "Double")
            merge_list.append(shp_nooa_21_1)

        # Realiza merge entre Insumo_MODIS, Insumo VIIRS (+ Insumo VIIRS NOAA 21) y lo almacena en el dato intermedio
        # Actividad 2 - Nuevo modelo
        continental_lyr = "continental_lyr"
        arcpy.Merge_management(merge_list, continental_lyr, "")

        # Se reproyecta el merge a Sirgas para poder hacer el clip con la capa de la amazonía
        continental_sirgas_lyr = "continental_sirgas_lyr"
        coordinate_system_sirgas = arcpy.SpatialReference(4170)
        arcpy.Project_management(continental_lyr, continental_sirgas_lyr, coordinate_system_sirgas)

        # Actividad 3 - Nuevo modelo
        # Posterior a ello se hace el corte al límite de la región amazónica entre la capa
        # resultante de la actividad 2  y se filtran los datos para la Amazonia, (continental_dlim)
        amazonia_nasa_lyr = "amazonia_nasa_lyr"
        arcpy.Clip_analysis(continental_sirgas_lyr, feature_dlim, amazonia_nasa_lyr, "")

        #######################################################################################
        ## Modis
        #######################################################################################
        # Modelo Anterior:
        # Se seleccionan los datos de amazonia_nasa_lyr con el criterio INSTRUMENT = 'MODIS'
        # y se almacenan en amazonia_modis_lyr
        # Modelo Anterior: Se remueve de la selección anterior la intersección con POZOS
        # a una distancia de 1000 metros
        amazonia_modis_lyr = "amazonia_modis_lyr"
        arcpy.MakeFeatureLayer_management(amazonia_nasa_lyr, amazonia_modis_lyr, "INSTRUMENT = 'MODIS'")
        arcpy.SelectLayerByLocation_management(amazonia_modis_lyr, "INTERSECT", feature_hidrocarburos,
                                               "1000 Meters", "REMOVE_FROM_SELECTION", "NOT_INVERT")

        # Nuevo modelo: Se copian datos filtrados en el feature amazonia_modis_without_pozos_lyr
        amazonia_modis_without_pozos_lyr = "amazonia_modis_without_pozos_lyr"
        arcpy.CopyFeatures_management(amazonia_modis_lyr, amazonia_modis_without_pozos_lyr, "", "0", "0", "0")

        #######################################################################################
        ## Suomi
        #######################################################################################
        # Se realiza la selección de los datos con el criterio INSTRUMENT = VIIRS_SOUMI,
        # se remueven de dicha selección
        # los pozos a una distancia de 375  metros.
        amazonia_vnp_lyr = "amazonia_vnp_lyr"
        arcpy.MakeFeatureLayer_management(amazonia_nasa_lyr, amazonia_vnp_lyr, "INSTRUMENT = 'VIIRS_SOUMI'")
        arcpy.SelectLayerByLocation_management(amazonia_vnp_lyr, "INTERSECT", feature_hidrocarburos,
                                               "375 Meters", "REMOVE_FROM_SELECTION", "NOT_INVERT")

        # Nuevo modelo: Se copian datos filtrados en el feature amazonia_vpn_without_pozos_lyr
        amazonia_vpn_without_pozos_lyr = "amazonia_vpn_without_pozos_lyr"
        arcpy.CopyFeatures_management(amazonia_vnp_lyr, amazonia_vpn_without_pozos_lyr, "", "0", "0", "0")

        #######################################################################################
        ## NOAA
        #######################################################################################
        # Se realiza la selección de los datos con el criterio INSTRUMENT = VIIRS_NOAA,
        # se remueven de dicha selección
        # los pozos a una distancia de 375  metros.
        amazonia_noaa_lyr = "amazonia_noaa_lyr"
        arcpy.MakeFeatureLayer_management(amazonia_nasa_lyr, amazonia_noaa_lyr, "INSTRUMENT = 'VIIRS_NOAA'")
        arcpy.SelectLayerByLocation_management(amazonia_noaa_lyr, "INTERSECT", feature_hidrocarburos,
                                               "375 Meters", "REMOVE_FROM_SELECTION", "NOT_INVERT")

        # Nuevo modelo: Se copian datos filtrados en el feature amazonia_noaa_without_pozos_lyr
        amazonia_noaa_without_pozos_lyr = "amazonia_noaa_without_pozos_lyr"
        arcpy.CopyFeatures_management(amazonia_noaa_lyr, amazonia_noaa_without_pozos_lyr, "", "0", "0", "0")

        #######################################################################################
        ## NOAA_21
        #######################################################################################
        # Se realiza la selección de los datos con el criterio INSTRUMENT = VIIRS_NOAA_21,
        # se remueven de dicha selección
        # los pozos a una distancia de 375  metros.
        amazonia_noaa_21_lyr = "amazonia_noaa_21_lyr"
        arcpy.MakeFeatureLayer_management(amazonia_nasa_lyr, amazonia_noaa_21_lyr, "INSTRUMENT = 'VIIRS_NOAA_21'")
        arcpy.SelectLayerByLocation_management(amazonia_noaa_lyr, "INTERSECT", feature_hidrocarburos,
                                               "375 Meters", "REMOVE_FROM_SELECTION", "NOT_INVERT")

        # Nuevo modelo: Se copian datos filtrados en el feature amazonia_noaa_without_pozos_lyr
        amazonia_noaa_21_without_pozos_lyr = "amazonia_noaa_21_without_pozos_lyr"
        arcpy.CopyFeatures_management(amazonia_noaa_21_lyr, amazonia_noaa_21_without_pozos_lyr, "", "0", "0", "0")

        #######################################################################################
        #######################################################################################
        # Se realiza merge de las dos selecciones
        # amazonia_modis_without_pozos_lyr, amazonia_vpn_without_pozos_lyr
        # se copia el resultado en amazonia_without_pozos_lyr
        amazonia_without_pozos_lyr = "amazonia_without_pozos_lyr"
        arcpy.Merge_management([amazonia_modis_without_pozos_lyr, amazonia_vpn_without_pozos_lyr,
                                amazonia_noaa_without_pozos_lyr, amazonia_noaa_21_without_pozos_lyr],
                               amazonia_without_pozos_lyr, "")

        # se adiciona el campo FECHA_DESC de tipo string y se le asignan los valores
        # según la siguiente expresión: "def getFecha(): return time.strftime("%d/%m/%Y")"
        # se adiciona el campo FECHA_DATE de tipo date y se le asignan los valores
        # según la siguiente expresión: "def getFecha(): return time.strftime("%d/%m/%Y %H:%M:%S")"
        arcpy.AddField_management(amazonia_without_pozos_lyr, "FECHA_DESC", "STRING")
        arcpy.AddField_management(amazonia_without_pozos_lyr, "FECHA_DATE", "DATE")

        expression = "getFecha()"
        codeblock = """def getFecha():
    return time.strftime("%d/%m/%Y")"""
        arcpy.CalculateField_management(amazonia_without_pozos_lyr, "FECHA_DESC", expression, "PYTHON3", codeblock)

        expression = "getFecha()"
        codeblock = """def getFecha():
    return time.strftime("%d/%m/%Y %H:%M:%S")"""
        arcpy.CalculateField_management(amazonia_without_pozos_lyr, "FECHA_DATE", expression, "PYTHON3", codeblock)

        #########################################################################################

        # Intersección entre la capa de "unión entidades de referencia" y los fuegos encontrados
        fuegos_union_ent_ref_lyr = "fuegos_union_ent_ref_lyr"

        logging.debug("** intersect... {} and {} , output: {}".format(feature_union_ent_ref, amazonia_without_pozos_lyr, fuegos_union_ent_ref_lyr ))

        arcpy.Intersect_analysis([feature_union_ent_ref, amazonia_without_pozos_lyr],
                                 fuegos_union_ent_ref_lyr, "NO_FID", "", "INPUT")

        #########################################################################################
        #########################################################################################
        # convertir fechas y filtrar exclusivamente 24 horas
        '''
        acq_date	timestamp without time zone
        acq_time	character varying

        acq_col	timestamp without time zone
        acq_day_col	integer
        acq_month_col	integer
        acq_year_col	integer
        acq_hour_col	integer
        '''
        arcpy.AddField_management(fuegos_union_ent_ref_lyr, "acq_col", "DATE")
        arcpy.AddField_management(fuegos_union_ent_ref_lyr, "acq_day_col", "SHORT")
        arcpy.AddField_management(fuegos_union_ent_ref_lyr, "acq_month_col", "SHORT")
        arcpy.AddField_management(fuegos_union_ent_ref_lyr, "acq_year_col", "SHORT")
        arcpy.AddField_management(fuegos_union_ent_ref_lyr, "acq_hour_col", "SHORT")

        fields = ['acq_date', 'acq_time', 'acq_col', 'acq_day_col', 'acq_month_col', 'acq_year_col', 'acq_hour_col']

        tz = pytz.timezone('America/Bogota')
        logging.debug(datetime.datetime.now())
        max_hours = 24
        logging.debug("max_hours: {} ".format(max_hours))
        # To get data for specific date use this:
        # specific_date = datetime.datetime(2024, 3, 12, 0, 3)
        # min_date = specific_date  - datetime.timedelta(hours=max_hours, minutes=0)
        min_date = datetime.datetime.now() - datetime.timedelta(hours=max_hours, minutes=0)
        min_date = tz.localize(min_date)
        logging.debug("min_date: {} ".format(min_date))

        with arcpy.da.UpdateCursor(fuegos_union_ent_ref_lyr, fields) as cursor:
            for row in cursor:
                #logging.debug("**********************")
                # logging.debug(row[0])
                # logging.debug(row[1])

                # sensor_date_str =  "{}-{}-{} {}:{}".format( row[0].year, row[0].month, row[0].day ,    row[1][0:2] ,  row[1][2:4]   )
                # logging.debug(sensor_date_str)
                # datetime(year, month, day, hour, minute, second, microsecond)
                sensor_date = datetime.datetime(row[0].year, row[0].month, row[0].day, int(row[1][0:2]),
                                                int(row[1][2:4]), 0, 0)
                #logging.debug("sensor_date: {} ".format(sensor_date))

                col_date = pytz.utc.localize(sensor_date).astimezone(tz)
                # logging.debug(utc_time)
                #logging.debug("col_date: {} ".format(col_date))
                #logging.debug(min_date < col_date)

                if min_date >= col_date:
                    #logging.debug("col_date is older than min_date. Ignore row. ")
                    cursor.deleteRow()
                else:
                    #logging.debug("col_date is newer than min_date. Include row. ")
                    row[2] = col_date
                    row[3] = col_date.day
                    row[4] = col_date.month
                    row[5] = col_date.year
                    row[6] = col_date.hour
                    cursor.updateRow(row)

        #########################################################################################
        #########################################################################################

        data['feature_fuegos'] = fuegos_union_ent_ref_lyr

        #########################################################################################
        #########################################################################################
        # Validar registros duplicados en los datos originales de la NASA
        '''
        Se agrego como campo para verificar duplicados CONFIDENSE, de acuerdo a la peticion
        de agregar ese campo a la base de datos, el 23 febrero 2022
        '''
        logging.debug("**********************************************************")
        logging.debug("** DELETE DUPLICADED ROWS FROM SOURCE DATA (NASA)... ")
        logging.debug("**********************************************************")
        result = int(arcpy.GetCount_management(fuegos_union_ent_ref_lyr)[0])
        logging.debug(' Layer: {}'.format(fuegos_union_ent_ref_lyr))
        logging.debug(' Total rows before deletion of duplicated data:  {} '.format(result))
        data['total_fuegos'] = result

        fields = ['LATITUDE', 'LONGITUDE', 'BRIGHTNESS', 'SCAN', 'TRACK'
            , 'ACQ_DATE', 'ACQ_TIME', 'SATELLITE', 'VERSION'
            , 'BRIGHT_T31', 'FRP', 'DAYNIGHT', 'INSTRUMENT'
            , 'BRIGHT_TI4', 'BRIGHT_TI5']

        # AVera - 20231211, Debido a que cuando pueda falta un sensor algunos campos no estaria disponibles
        # Se crea este proceso para que identifique los campos faltantes y los cree con valor nulo y no se modifique la logica posterior
        field_list = [field.name for field in arcpy.ListFields(fuegos_union_ent_ref_lyr)]
        lista_campos_faltantes = list(set(fields) - set(field_list))
        for field in lista_campos_faltantes:
            arcpy.AddField_management(fuegos_union_ent_ref_lyr, field, "TEXT")

        arcpy.FindIdentical_management(fuegos_union_ent_ref_lyr, "fuegos_union_ent_ref_lyr_duplicated", fields)

        arcpy.DeleteIdentical_management(fuegos_union_ent_ref_lyr, fields)

        result = int(arcpy.GetCount_management(fuegos_union_ent_ref_lyr)[0])
        logging.debug(' Total rows AFTER deletion of duplicated data: : {} '.format(result))
        data['total_fuegos'] = result
        logging.debug("**********************************************************")

        #########################################################################################
        #########################################################################################
        # Validar que los nuevos registros no existan en la bd
        '''
        2020-06-03
        De acuerdo a la revisión que se realizó para detectar registros repetidos en la 
        base de datos les hago llegar los campos que se deben tener en cuenta para armar 
        la llave y comparar contra la base de datos histórica.

        [latitude] & " - "& [longitude] & " - "& [brightness] & " - "& [scan] & " - "& [track] 
        & " - "& [acq_date] & " - "& [acq_time] & " - "& [satellite] & " - "& [version] 
        & " - "& [bright_t31] & " - "& [frp] & " - "& [daynight] & " - "& [instrument] 
        & " - "& [bright_ti4] & " - "& [bright_ti5]   

          [LATITUDE] & " - "& [LONGITUDE] & " - "& [BRIGHTNESS] & " - "& [SCAN] & " - "& [TRACK] 
        & " - "& [ACQ_DATE] & " - "& [ACQ_TIME] & " - "& [SATELLITE] & " - "& [VERSION] 
        & " - "& [BRIGHT_T31] & " - "& [FRP] & " - "& [DAYNIGHT] & " - "& [INSTRUMENT] 
        & " - "& [BRIGHT_TI4] & " - "& [BRIGHT_TI5] 
        '''
        logging.debug("**********************************************************")
        logging.debug("** VALIDATE EXISTING RECORDS... **")
        logging.debug("**********************************************************")

        edit_conn = data['edit_conn_prod_instance']
        table_name = 'e2_modfun.CFgoHis_Car_Mun_Dep_Elt_Pai'

        duplicated_lyr = 'duplicated_lyr'
        feature_output_prod = data['feature_output_prod']
        arcpy.MakeFeatureLayer_management(feature_output_prod, duplicated_lyr)

        fields = ['OBJECTID', 'LATITUDE', 'LONGITUDE', 'BRIGHTNESS', 'SCAN', 'TRACK'
            , 'ACQ_DATE', 'ACQ_TIME', 'SATELLITE', 'VERSION', 'BRIGHT_T31'
            , 'FRP', 'DAYNIGHT', 'INSTRUMENT', 'BRIGHT_TI4', 'BRIGHT_TI5']

        deleted_rows = 0
        #logging.debug("using test data: {}".format(data["is_test"]))
        if not data["is_test"]:
            egdb_conn = arcpy.ArcSDESQLExecute(edit_conn)
        with arcpy.da.UpdateCursor(fuegos_union_ent_ref_lyr, fields) as cursor:
            for row in cursor:
                #logging.debug('{0}, {1}, {2}, {3}, {4}'.format(row[0], row[1], row[2], row[3], row[4]))
                id = row[0]
                aq_date = " TO_DATE ( '" + row[6].strftime("%Y/%m/%d") + "', 'YYYY/MM/DD' ) "
                brigthness = ""
                if row[3] is None:
                    brigthness = " is NULL"
                else:
                    brigthness = " = " + str(row[3])

                bright_t31 = ""
                if row[10] is None:
                    bright_t31 = " is NULL"
                else:
                    bright_t31 = " = " + str(row[10])

                bright_ti4 = ""
                if row[14] is None:
                    bright_ti4 = " is NULL"
                else:
                    bright_ti4 = " = " + str(row[14])

                bright_ti5 = ""
                if row[15] is None:
                    bright_ti5 = " is NULL"
                else:
                    bright_ti5 = " = " + str(row[15])

                where = ''' LATITUDE = {} AND LONGITUDE = {} and BRIGHTNESS  {} and  SCAN = {}  and TRACK = {}
                    and ACQ_DATE =  {}  and ACQ_TIME = '{}' and  SATELLITE = '{}'  and VERSION = '{}'  and  BRIGHT_T31   {} 
                    and FRP = {}  and DAYNIGHT = '{}' and  INSTRUMENT = '{}' and BRIGHT_TI4  {}  and BRIGHT_TI5  {} 
                    '''.format(row[1], row[2], brigthness, row[4], row[5], aq_date, row[7]
                               , row[8], row[9], bright_t31, row[11], row[12], row[13], bright_ti4, bright_ti5)

                #logging.debug(" id: {}   ".format(id))

                sql = '''   
                    SELECT COUNT(*) AS f_count FROM {} where {}   
                    '''.format(table_name, where)
                #logging.debug("sql:    {}  ".format(sql))
                if not data["is_test"]:
                    egdb_return = egdb_conn.execute(sql)
                    #logging.debug(' #  of existing records : {}'.format(egdb_return))
                    if egdb_return > 0:
                        #logging.debug(' Row already exists in DB  id : {} '.format(id))
                        logging.debug("sql:    {}  ".format(sql))
                        cursor.deleteRow()
                        deleted_rows += 1

        logging.debug("Total rows before validation : {} ".format(data['total_fuegos']))
        total_after_validation = int(arcpy.GetCount_management(fuegos_union_ent_ref_lyr)[0])
        logging.debug('Total rows  to append after validation : {}, deleted: {} '.format(total_after_validation, deleted_rows))

        if total_after_validation > 0:
            #########################################################################################
            #########################################################################################
            # Actividad 6:
            # Se adicionan los datos diarios de fuegos a una capa histórica, mediante la herramienta  “Append”.
            # Bd prod y bd publicación
            # Actividad 7 :
            # Se verifica el almacenamiento de la información de la capa resultante en la base de datos, sumándose
            #  al histórico del servicio de fuegos

            logging.debug("** append corp... **")
            fms = get_field_mappings(fuegos_union_ent_ref_lyr)
            if not data["is_test"]:
                arcpy.Append_management([fuegos_union_ent_ref_lyr], feature_output_prod, "NO_TEST", fms)
                expected_new_total_fuegos_prod = data['total_fuegos_historicos_prod'] + total_after_validation

                result = int(arcpy.GetCount_management(feature_output_prod)[0])
                logging.debug('{} has {} records after append'.format(feature_output_prod, result))
                if result != expected_new_total_fuegos_prod:
                    raise Exception("No se pudieron adicionar nuevos registros a {} ".format(feature_output_prod))

            logging.debug("** append pub... **")
            # Se realiza un append a la tabla historica en Web Mercator y en Sirgas

            feature_output_pub = data['feature_output_pub']
            feature_output_pub_sirgas = data['feature_output_pub_sirgas']
            arcpy.Append_management([fuegos_union_ent_ref_lyr], feature_output_pub_sirgas, "NO_TEST", fms)
            #########################################################################################
            #########################################################################################
            # Se reproyecta la capa fuegos_union_ent_ref_lyr a web mercator para hacer un append a la capa historica en el dataset de publicacion web mercator
            output_pub_web_mercator = 'output_pub_web_mercator'
            coordinate_system_web_mercator = arcpy.SpatialReference(3857)
            arcpy.Project_management(fuegos_union_ent_ref_lyr, output_pub_web_mercator, coordinate_system_web_mercator)
            arcpy.Append_management([output_pub_web_mercator], feature_output_pub, "NO_TEST", fms)
            expected_new_total_fuegos_pub = data['total_fuegos_historicos_pub'] + total_after_validation
            result = int(arcpy.GetCount_management(feature_output_pub)[0])
            logging.debug('{} has {} records after append'.format(feature_output_pub, result))

            result_sirgas_out = int(arcpy.GetCount_management(feature_output_pub_sirgas)[0])
            logging.debug('{} has {} records after append'.format(feature_output_pub_sirgas, result_sirgas_out))
            if result != expected_new_total_fuegos_pub:
                raise Exception("No se pudieron adicionar nuevos registros a {} ".format(feature_output_pub))
    except Exception as e:
        print_error(e)
        raise Exception('ERROR_004 - Error al procesar Datos : {} '.format(e))

    logging.debug("***********************************")

def get_field_mappings(lyr):
    logging.debug("** get_field_mappings ")
    fields = arcpy.ListFields(lyr)
    fms = arcpy.FieldMappings()
    try:
        for field in fields:
            # Excluir campos de sistema: OID, Geometry, GlobalID
            if field.type in ['OID', 'Geometry', 'GlobalID']:
                logging.debug("{0} is a type of {1} with a length of {2} - SKIPPED (system field)".format(
                    field.name, field.type, field.length))
                continue

            logging.debug("{0} is a type of {1} with a length of {2}".format(field.name, field.type, field.length))
            fm = arcpy.FieldMap()
            fm.addInputField(lyr, field.name)
            type_name = fm.outputField
            type_name.name = field.name
            fm.outputField = type_name
            fms.addFieldMap(fm)
    except Exception as e:
        print_error(e)
        raise Exception('ERROR_004 - Error al procesar Datos : {} '.format(e))
    return fms


##################################################################
##################################################################
'''
Envío de notificacones

# Actividad 8
# El sistema envía un correo electrónico con el resumen de los fuegos diarios de la Amazonia colombiana 
# con un reporte por departamentos
# y corporaciones a una lista de distribución de correo pre-establecida con usuarios de interés de forma 
# diaria  al terminar el procesamiento.
'''


def send_notifications(data):
    logging.debug("***********************************")
    logging.debug(' send_notifications ')

    try:
        feature_fuegos = data['feature_fuegos']
        total_fuegos = data['total_fuegos']
        feature_usuarios_emails = data['feature_usuarios_emails']

        conteo_car = group_by_count(feature_fuegos, ["car"])
        # logging.debug( ' conteo agrupando por car: {} '.format(conteo_car )  )

        conteo_depto = group_by_count(feature_fuegos, ["departamen"])
        # logging.debug( ' conteo agrupando por departamento: {} '.format(conteo_depto )  )

        conteo_muni = group_by_count(feature_fuegos, ["departamen", "municipio"])
        # logging.debug( ' conteo agrupando por municipio: {} '.format(conteo_muni )  )

        message = '''
			<p hidden>
            <b>Asunto</b>: SIATAC - Resumen diario de puntos de calor en la Amazonia colombiana. <br/>
            <b>Fecha</b>:  {} <br/><br/>
			</p>
            <img src="http://siatac.co/image/image_gallery?uuid=fee74773-e69e-4fac-97ca-6be20777d7fb&groupId=755&t=1579281604474" height="83" width="600" class="CToWUd a6T" tabindex="0" style="text-align: center">
		<br/>
		<br/>
		<p>
        Este servicio hace parte del Sistema de Información Ambiental Territorial de la 
        Amazonia Colombiana – SIAT-AC del Instituto Amazónico de Investigaciones Científicas – SINCHI.<br/> Tomando como fuente los datos de los servicios expuestos por la NASA
		se registra un total de <strong style="color:#BF6830;"> {} </strong>reportes de puntos de calor, desde las 
             6:00 AM 
             del 
             {}   
             a las 6:00 AM 
             del 
             {}   <br/>		
		</p>		
        '''

        table = ''' 
            <h3>Reporte por departamento y municipio </h3>
            <table style="width:30%;padding: 8px; border-collapse: collapse;border: 1px solid #cccccc; ">
            <tr>
                <th  style="border: 1px solid #cccccc;font-weight: bold; text-align: center; padding-top: 1px;  padding-bottom: 1px; background-color: #BF6830;">
                Departamento</th>
                <th  style="border: 1px solid #cccccc;font-weight: bold; text-align: center; padding-top: 1px;  padding-bottom: 1px; background-color: #BF6830;">
                Cantidad</th>
                <th  style="border: 1px solid #cccccc;font-weight: bold; text-align: center; padding-top: 1px;  padding-bottom: 1px; background-color: #BF6830;">
                Municipio*</th>
                <th  style="border: 1px solid #cccccc;font-weight: bold; text-align: center; padding-top: 1px;  padding-bottom: 1px; background-color: #BF6830;">
                Cantidad</th>
            </tr>
            {}
            </table>
			<div>
			<font size = "0">*En la tabla se presenta el municipio que tuvo la mayor cantidad de puntos de calor reportados en el departamento. </font>
			</div>
			'''

        ordered_depto = collections.OrderedDict(sorted(conteo_depto.items()))
        # logging.debug( ' ordered_depto: {} '.format(ordered_depto )  )

        ordered_muni = collections.OrderedDict(sorted(conteo_muni.items()))
        # logging.debug( ' ordered_muni: {} '.format(ordered_muni )  )

        rows_car = ''
        for depto in ordered_depto:
            municipio_max_name = ""
            municipio_max_total = 0

            for muni in ordered_muni:
                # logging.debug( type(muni)  )
                if muni[0] == depto:
                    if conteo_muni[muni] > municipio_max_total:
                        municipio_max_name = muni[1]
                        municipio_max_total = conteo_muni[muni]

            row = '''
            <tr>
                <td style="border: 1px solid #cccccc; border-collapse: collapse; padding: 1px;">{}</td>
                <td style="border: 1px solid #cccccc; border-collapse: collapse; padding: 1px;">{}</td>
                <td style="border: 1px solid #cccccc; border-collapse: collapse; padding: 1px;">{}</td>
                <td style="border: 1px solid #cccccc; border-collapse: collapse; padding: 1px;">{}</td>
            </tr>
            '''.format(depto, conteo_depto[depto],
                       municipio_max_name, municipio_max_total)
            rows_car += row

        message += table.format(rows_car) + "<br/><br/>"

        table = '''
			<h3>Reporte por Corporación Autónoma Regional y de Desarrollo Sostenible</h3>
            <table style="width:30%; padding: 8px; border-collapse: collapse;border: 1px solid #cccccc; ">
            <tr>
            <th style="border: 1px solid #cccccc;font-weight: bold; text-align: center; padding-top: 1px;  padding-bottom: 1px; background-color: #BF6830;" >
            Corporación</th>
            <th style="border: 1px solid #cccccc;font-weight: bold; text-align: center; padding-top: 1px;  padding-bottom: 1px; background-color: #BF6830;" >
            Cantidad</th>
            </tr>
            {}
            </table>'''

        ordered_car = collections.OrderedDict(sorted(conteo_car.items()))
        # logging.debug( ' ordered_car: {} '.format(ordered_car )  )

        rows_car = ''
        for car in ordered_car:
            logging.debug("Este es car: " + str(car))
            logging.debug("Este es conteo_car: " + str(conteo_car[car]))
            row = '''
            <tr>
                <td style="border: 1px solid #cccccc; border-collapse: collapse; padding: 1px;" >{}</td>
                <td style="border: 1px solid #cccccc; border-collapse: collapse; padding: 1px;">{}</td>
            </tr>
            '''.format(car, conteo_car[car])
            rows_car += row

        message += table.format(rows_car) + "<br/>"

        feature_usuarios_emails = data['feature_usuarios_emails']
        email_batch_size = data['email_batch_size']
        fields = ['correo']
        logging.debug("email_batch_size : {} ".format(email_batch_size))

        recipients = []
        with arcpy.da.SearchCursor(feature_usuarios_emails, fields) as cursor:
            for row in cursor:
                recipients.append(row)

        # logging.debug( recipients  )
        n = int(email_batch_size)
        recipients_by_chunks = [recipients[i * n:(i + 1) * n] for i in range((len(recipients) + n - 1) // n)]
        logging.debug("recipients_by_chunks : {} ".format(recipients_by_chunks))

        if data["is_test"]:
            recipients_by_chunks = ["edwin.piragauta@gmail.com"]
        previous_day = (datetime.date.today() - datetime.timedelta(1)).strftime("%Y/%m/%d")
        for recipients in recipients_by_chunks:
            gmail_user = data["gmail_user"]

            body = message.format(time.strftime("%Y/%m/%d - %H:%M"),
                                  total_fuegos,
                                  previous_day,
                                  time.strftime("%Y/%m/%d"))

            # logging.debug( body )
            subject = "SIATAC - Resumen diario de puntos de calor en la Amazonia colombiana. Fecha: {} .".format(
                time.strftime("%Y/%m/%d - %H:%M"))
            try:
                send_email(data, [gmail_user], subject, body, recipients)
                time.sleep(40)
            except Exception as e:
                print_error(e)
                raise Exception('ERROR_005 - Error al enviar notificación : {} '.format(e))

    except Exception as e:
        print_error(e)
        raise Exception('ERROR_004 - Error al procesar Datos : {} '.format(e))
    logging.debug("***********************************")

'''
    Retorna el texto a la derecha despues del ultimo punto
'''
def get_last_portion(text):
  parts = text.split('.')
  return parts[-1]


'''
    Borra registros de la capa para una fecha en particular
'''
def deleteRows(feature_class, fecha_campo, fecha_obj):
    logging.debug("deleting rows in {}... field: {}, date: {}".format(feature_class, fecha_campo, fecha_obj.strftime("%Y-%m-%d")))

    sql_expr = "{} = date '{}-{}-{} 00:00:00'".format(
        arcpy.AddFieldDelimiters(feature_class, fecha_campo),
        fecha_obj.year,
        str(fecha_obj.month).zfill(2),
        str(fecha_obj.day).zfill(2)
    )

    try:
        # Iniciar una sesión de edición
        edit = arcpy.da.Editor(arcpy.env.workspace)
        edit.startEditing(True)  # True para iniciar con operación exclusiva
        edit.startOperation()

        # Seleccionar los registros que cumplen el criterio de fecha
        selection = arcpy.SelectLayerByAttribute_management(
            in_layer_or_view=feature_class,
            selection_type="NEW_SELECTION",
            where_clause=sql_expr
        )

        # Obtener el conteo de registros seleccionados
        count = int(arcpy.GetCount_management(selection)[0])
        print("Se seleccionaron {} registros para ser eliminados.".format(count))

        if count > 0:
            # Eliminar los registros seleccionados
            arcpy.DeleteRows_management(selection)
            print("Se eliminaron {} registros con fecha igual o anterior a {}.".format(
                count, fecha))
        else:
            print("No se encontraron registros que cumplan con el criterio de fecha.")

            # Completar la operación y terminar la sesión de edición
            edit.stopOperation()
            edit.stopEditing(True)  # True para guardar los cambios

    except arcpy.ExecuteError:
        # Si ocurre un error en arcpy, imprimir el mensaje
        print(arcpy.GetMessages())
        # Si estamos en una sesión de edición, detener sin guardar cambios
        if 'edit' in locals():
            edit.stopOperation()
            edit.stopEditing(False)  # False para no guardar los cambios

    except Exception as e:
        # Para cualquier otro error
        print("Error: {}".format(str(e)))
        # Si estamos en una sesión de edición, detener sin guardar cambios
        if 'edit' in locals():
            edit.stopOperation()
            edit.stopEditing(False)  # False para no guardar los cambios


##################################################################
##################################################################
'''
Programa principal que coordina la ejecución de los diferentes pasos.
'''
def main(data):
    ##################################################################
    ##################################################################
    print("main ***")
    # Python 3 uses UTF-8 by default, no need for reload/setdefaultencoding
    temp_dir = data['temp_dir']
    print(temp_dir)
    logFormat = '%(asctime)-10s %(name)-12s %(levelname)-6s %(message)s'
    # logFormat =  '%(message)s'
    current_day = '{:%Y-%m-%d_%H-%M}'.format(datetime.datetime.now())
    logfile = os.path.join(temp_dir, 'fuegos_{}.log'.format(current_day))
    data['logfile'] = logfile
    print(logfile)
    ##################################################################
    ##################################################################
    logging.basicConfig(level=logging.DEBUG, format=logFormat, filename=logfile, filemode='w')
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.info("************************************************************************************************")
    logging.info("Inicio Programa")
    logging.debug("***********************************")
    logging.debug("current_day : {} ".format(current_day))
    current_day_temp_dir = os.path.join(temp_dir, current_day)
    logging.debug("current_day_temp_dir : {} ".format(current_day_temp_dir))
    if os.path.isdir(current_day_temp_dir):
        # os.rmdir(current_day_temp_dir)
        shutil.rmtree(current_day_temp_dir)
    os.mkdir(current_day_temp_dir)
    data['current_day_temp_dir'] = current_day_temp_dir
    ##################################################################
    ##################################################################

    ##################################################################
    ##################################################################
    try:
        download_shps(data)
    except Exception as e:
        print_error(e)
        to = list(data["admin_emails"])
        subject = "SIATAC - Procesamiento de Fuegos - Error - {} ".format(current_day)
        body = '''
            <H2>SIATAC - Procesamiento de Fuegos - Error al descargar archivos shps de la NASA.</H2><br/><br/>
            <b>Mensaje:</b><br/><br/>
            No se pudo descargar los archivos shapefile del servidor de la NASA.<br/><br/>
            Ver archivo de log en el servidor en la ruta <i> {} </i> <br/><br/>
            <b>Error:</b> {} <br/><br/>'''.format(logfile, e)
        send_email(data, to, subject, body)
        raise e
    ##################################################################
    ##################################################################

    ##################################################################
    ##################################################################
    logging.debug("verifying if connect to sde or using filegdb...")
    layer_output_prod = data['layer_output_prod']
    layer_output_pub = data['layer_output_pub']
    layer_output_pub_sirgas = data['layer_output_pub_sirgas']
    try:
        if not data["is_test"]:
            create_sde_connections(data)
        else:
            logging.debug("testing with local data")
            data['reader_conn_prod_instance'] = data["local_gdb"]
            data['reader_conn_pub_instance'] = data["local_gdb"]
            data['edit_conn_prod_instance'] = data["local_gdb"]
            data['edit_conn_pub_instance'] = data["local_gdb"]
            env.workspace = data["local_gdb"]
            layer_output_prod = "\\" + get_last_portion(layer_output_prod)
            layer_output_pub = "\\" + get_last_portion(layer_output_pub)
            layer_output_pub_sirgas = "\\" + get_last_portion(layer_output_pub_sirgas)

        fecha_actual = datetime.datetime.now()
        fecha_anterior = fecha_actual - datetime.timedelta(days=1)
        deleteRows(layer_output_prod, "acq_date", fecha_anterior)
        deleteRows(layer_output_pub, "acq_date", fecha_anterior)
        deleteRows(layer_output_pub_sirgas, "acq_date", fecha_anterior)
    except Exception as e:
        print_error(e)
        to = list(data["admin_emails"])
        subject = "SIATAC - Procesamiento de Fuegos - Error - {} ".format(current_day)
        body = '''
            <H2>SIATAC - Procesamiento de Fuegos - Error al crear conexiones a SDE.</H2><br/><br/>
            <b>Mensaje:</b><br/><br/>
            Error al crear conexiones a SDE.<br/><br/>
            Ver archivo de log en el servidor en la ruta <i> {} </i> <br/><br/>
            <b>Error:</b> {} <br/><br/>'''.format(logfile, e)
        send_email(data, to, subject, body)
        raise e
    ##################################################################
    ##################################################################

    ##################################################################
    ##################################################################
    try:
        validate_input_data(data)
    except Exception as e:
        print_error(e)
        to = list(data["admin_emails"])
        subject = "SIATAC - Procesamiento de Fuegos - Error - {} ".format(current_day)
        body = '''
            <H2>SIATAC - Procesamiento de Fuegos - Error al validar datos de  SDE.</H2><br/><br/>
            <b>Mensaje:</b><br/><br/>
            Error al validar datos de  SDE.<br/><br/>
            Ver archivo de log en el servidor en la ruta <i> {} </i> <br/><br/>
            <b>Error:</b> {} <br/><br/>'''.format(logfile, e)
        send_email(data, to, subject, body)
        raise e
    ##################################################################
    ##################################################################

    ##################################################################
    ##################################################################
    try:
        process_data(data)
        # El proceso de enviar correo de notificación ahora se ejecuta de manera independiente
        # send_notifications(data)
    except Exception as e:
        print_error(e)
        to = list(data["admin_emails"])
        subject = "SIATAC - Procesamiento de Fuegos - Error - {} ".format(current_day)
        body = '''
            <H2>SIATAC - Procesamiento de Fuegos - Error al procesar datos.</H2><br/><br/>
            <b>Mensaje:</b><br/><br/>
            Error al procesar los datos del modelo para monitoreo de fuegos.<br/><br/>
            Ver archivo de log en el servidor en la ruta <i> {} </i> <br/><br/>
            <b>Error:</b> {} <br/><br/>'''.format(logfile, e)
        send_email(data, to, subject, body)
        raise e
    ##################################################################
    ##################################################################


################################################################################
################################################################################
if __name__ == "__main__":
    try:
        ################################################################################
        data = {}
        # print (os.path.realpath(__file__) )
        # print('sys.argv[0] =', sys.argv[0])
        pathname = os.path.dirname(sys.argv[0])
        basepath = os.path.abspath(pathname)
        # print('path =', pathname)
        print('full path =', basepath)
        config_path = os.path.join(basepath, 'config', 'config.json')
        print(config_path)
        with open(config_path) as config_file:
            data = json.load(config_file)
        main(data)
    except Exception as e:
        print_error(e)
    finally:
        ##################################################################
        ##################################################################
        logging.debug("***********************************")
        # if 'current_day_temp_dir' in data.keys():
        #     try:
        #         logging.debug("deleting temp dir {} ".format(data['current_day_temp_dir']))
        #         shutil.rmtree(data['current_day_temp_dir'])
        #     except Exception as e:
        #         ## No se pudo eliminar la carpeta temporal debido a que todavía hay procesos
        #         ## abiertos que bloquean los archivos
        #         ## Se debe programar una tarea de sistema operativo que limpie los teporales de forma periódica.
        #         print_error(e)
        logging.debug("***********************************")
        logging.debug("Detalles del entorno:")
        logging.debug(json.dumps(data, sort_keys=True, indent=2, separators=(',', ': ')))
        logging.debug("***********************************")
        logging.info("**************************************************************************************")
        logging.info("Fin Programa")
        logging.info("**************************************************************************************")
        ##################################################################
        ##################################################################
################################################################################
################################################################################