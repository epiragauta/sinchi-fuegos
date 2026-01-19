# -*- coding: utf-8 -*-
"""
Procesamiento diario de datos de fuegos

- author juanmendez@gkudos.com
- require: python 2.7 Arcgis Desktop
"""

import logging, os, sys, traceback, json, glob, shutil, time, zipfile, smtplib
import arcpy
import requests
import datetime, collections
import mysql.connector
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
    no seguras, se debio habilitar factor de autenticacion 2 telefono administrador Andres Diaz,
    y una contrasena especifica para esta la aplicacion aplicacion_focos_smtp, la contrasena es
    'psvyagohgszbyigo', se pone en el campo password del cliente smtplib.SMTP_SSL como se muestra a continuacion
    se debe usar cliente seguro SSL
    '''

    server = smtplib.SMTP_SSL('smtp-relay.gmail.com', 465, timeout=120)
    #server = smtplib.SMTP_SSL('smtp.gmail.com', 465, timeout=120)
    #server = smtplib.SMTP_SSL('smtp.gmail.com', 587, timeout=120)
    server.set_debuglevel(1)
    server.ehlo()
    server.login(gmail_user, gmail_password)

    for email in to:
        #logging.debug(email)
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
					<img src="https://aplicaciones.siatac.co/monitoreo_ambiental/region/media/logo_sinchi_siatac.jpg" height="47" width="68" float="left" class="CToWUd a6T" tabindex="0">
					</td>
					<td>
					<font size = "1">Para conocer la ubicación de los puntos de calor reportados, ingrese al mapa interactivo del sistema de monitoreo (SINCHI)</font>
					<a href="https://experience.arcgis.com/experience/ceb7f423780c410389ca35fc0990e7e4" target="_blank" >Aquí.</a> 
					<br/>
					<font size = "1">Para mayor detalle ingrese al sistema de monitoreo de puntos de calor y cicatrices de quema (SINCHI)</font>
					<a href="https://siatac.co/puntos-de-calor/" target="_blank" >Aquí</a>
					</td>					
					</tr>
				</table>
				<table>
				<tr>
				<td style="width: 68px;">
				<img src="https://aplicaciones.siatac.co/monitoreo_ambiental/region/media/ideam_logo.png" height="37" width="46" float="left" class="CToWUd a6T" tabindex="0">
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
                    <img src="https://aplicaciones.siatac.co/monitoreo_ambiental/region/media/logo_direccion_siatac.png" 
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


def get_last_portion(text):
  parts = text.split('.')
  return parts[-1]


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
        # fecha_desc   "16/04/2020"
        current_day = '{:%d/%m/%Y}'.format(datetime.datetime.now())
        logging.debug(' current_day : {} '.format(current_day))

        edit_conn = data['edit_conn_pub_instance']
        feature_path = edit_conn + data['layer_output_pub']
        reader_conn = data['reader_conn_pub_instance']
        if data["is_test"]:
            reader_conn = data["local_gdb"]
            feature_path = reader_conn + "\\" + get_last_portion(data['layer_output_pub'])

        # Filtrar los datos del día de hoy
        selection_lyr = "selection_lyr"
        filter_query = ' "fecha_desc" = \'{}\' '.format(current_day)
        logging.debug(' filter_query : {} '.format(filter_query))

        arcpy.MakeFeatureLayer_management(feature_path, selection_lyr)
        arcpy.SelectLayerByAttribute_management(selection_lyr, "NEW_SELECTION", filter_query)

        total_fuegos = int(arcpy.GetCount_management(selection_lyr)[0])
        logging.debug(' selection_lyr  has {} records'.format(total_fuegos))

        conteo_car = group_by_count(selection_lyr, ["car"])
        # logging.debug( ' conteo agrupando por car: {} '.format(conteo_car )  )

        conteo_depto = group_by_count(selection_lyr, ["departamen"])
        # logging.debug( ' conteo agrupando por departamento: {} '.format(conteo_depto )  )

        conteo_muni = group_by_count(selection_lyr, ["departamen", "municipio"])
        # logging.debug( ' conteo agrupando por municipio: {} '.format(conteo_muni )  )

        conteo_cuencas = group_by_count(selection_lyr, ["nomzh"])

        conteo_subcuencas = group_by_count(selection_lyr, ["nomzh", "nomszh"])

        conteo_nucleos = group_by_count(selection_lyr, ["nombre_uer"])

        message = '''
			<p hidden>
            <b>Asunto</b>: SIATAC - Resumen diario de puntos de calor en la Amazonia colombiana. <br/>
            <b>Fecha</b>:  {} <br/><br/>
			</p>
            <p>
            Este servicio hace parte del Sistema de Información Ambiental Territorial de la 
            Amazonia Colombiana – SIAT-AC del Instituto Amazónico de Investigaciones Científicas – SINCHI.<br>
            </p>
            <img src="https://aplicaciones.siatac.co/monitoreo_ambiental/region/media/correo_encabezado_puntos_calor.jpg" height="83" width="600" class="CToWUd a6T" tabindex="0" style="text-align: center">
		<br/>
		<br/>
		<p>
        Tomando como fuente los datos de los servicios expuestos por la NASA
		se registra un total de <strong style="color:#BF6830;"> {} </strong> puntos de calor, desde las 
             12:00 AM 
             del 
             {}   
             hasta las 11:59 PM 
             del 
             {}  
             (Hora de Colombia UTC-5)
              <br/>		
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
                if muni[0] == depto:
                    if conteo_muni[muni] > municipio_max_total:
                        municipio_max_name = muni[1].encode('utf-8')
                        municipio_max_total = conteo_muni[muni]

            row = '''
            <tr>
                <td style="border: 1px solid #cccccc; border-collapse: collapse; padding: 1px;">{}</td>
                <td style="border: 1px solid #cccccc; border-collapse: collapse; padding: 1px;">{}</td>
                <td style="border: 1px solid #cccccc; border-collapse: collapse; padding: 1px;">{}</td>
                <td style="border: 1px solid #cccccc; border-collapse: collapse; padding: 1px;">{}</td>
            </tr>
            '''.format(depto.encode('utf-8'), conteo_depto[depto],
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
            </table>            

            '''

        ordered_car = collections.OrderedDict(sorted(conteo_car.items()))

        ordered_cuencas = collections.OrderedDict(sorted(conteo_cuencas.items()))

        ordered_subcuencas = collections.OrderedDict(sorted(conteo_subcuencas.items()))

        ordered_nucleos = collections.OrderedDict(sorted(conteo_nucleos.items()))
        # logging.debug( ' ordered_car: {} '.format(ordered_car )  )

        rows_car = ''
        for car in ordered_car:
            row = '''
            <tr>
                <td style="border: 1px solid #cccccc; border-collapse: collapse; padding: 1px;" >{}</td>
                <td style="border: 1px solid #cccccc; border-collapse: collapse; padding: 1px;">{}</td>
            </tr>
            '''.format(car, conteo_car[car])
            rows_car += row

        message += table.format(rows_car) + "<br/>"

        style_th = 'style="border: 1px solid #cccccc;font-weight: bold; text-align: center; padding-top: 1px;  padding-bottom: 1px; background-color: #BF6830;"'
        style_td = 'style="border: 1px solid #cccccc; border-collapse: collapse; padding: 1px;"'
        table_html_cuencas = ''' 
            <h3>Reporte por cuencas hidrográficas</h3>    
            <table style="width:50%;padding: 8px; border-collapse: collapse;border: 1px solid #cccccc; ">
            <tr>
                <th {}>Cuenca hidrográfica</th>
                <th {}>Cantidad</th>        
                <th {}>Subcuenca hidrográfica</th>
                <th {}>Cantidad</th>
            </tr>
            {}
            </table>    
            <p><b>Cuenca Hidrográfica</b>: Entiéndase por cuenca u hoya hidrográfica el área de aguas superficiales o subterráneas, que vierten a una red natural con 
            uno o varios cauces naturales, de caudal continuo o intermitente, que confluyen en un curso mayor que, a su vez, puede desembocar en un río principal, 
            en un depósito natural de aguas, en un pantano o directamente en el mar. (Fuente. Decreto 1729 de 2002, Art 1) la tabla muestra las cuencas determinadas 
            por el IDEAM como zonas hidrográficas subzonas hidrográficas o cuencas de tercer orden </p>
            <p><font size = "0">*En la tabla se presenta la subcuenca  que tuvo la mayor cantidad de puntos de calor reportados en la cuenca. </font></p>            
            '''
        rows_cuencas = ''
        for cuenca in ordered_cuencas:
            subszh_max_name = ""
            subszh_max_total = 0

            for subszh in ordered_subcuencas:
                if subszh[0] == cuenca:
                    if conteo_subcuencas[subszh] > subszh_max_total:
                        subszh_max_name = subszh[1].encode('utf-8')
                        subszh_max_total = conteo_subcuencas[subszh]

            row = '''
            <tr>
                <td {}>{}</td>
                <td {}>{}</td>
                <td {}>{}</td>
                <td {}>{}</td>
            </tr>
            '''.format(style_td, cuenca.encode('utf-8'),
                       style_td, conteo_cuencas[cuenca],
                       style_td, subszh_max_name,
                       style_td, subszh_max_total)

            rows_cuencas += row

        message += table_html_cuencas.format(style_th, style_th, style_th, style_th, rows_cuencas) + "<br/>"

        table_cndf = ''' 
                    <h3>Reporte por núcleos de desarrollo forestal y de la biodiversidad</h3>    
                    <table style="width:50%;padding: 8px; border-collapse: collapse;border: 1px solid #cccccc; ">
                    <tr>
                        <th {}>Núcleos de desarrollo forestal</th>
                        <th {}>Cantidad</th>        
                    </tr>
                    {}
                    </table>
                    <p><b>Núcleo de Desarrollo Forestal y de la Biodiversidad NDFyB</b>: Es una área identificada por el Sistema de Monitoreo de Bosques y Carbono SMByC 
                    como Núcleo Activo de Deforestación NAD, que además cuenta con una oferta natural de superficie de bosque y que ha sido priorizada por 
                    el Ministerio de Ambiente y Desarrollo Sostenible para que las comunidades locales, con sus saberes y con el acompañamiento del Estado, 
                    implementen acciones de manejo sostenible de los bosques y de la biodiversidad, generando transformación social, económica y ambiental del territorio. 
                    (Fuente. Resolución 057 de 2025. Ministerio del Medio Ambiente. Art. 2 Definiciones)</p>    
                    '''
        rows_cndf = ''
        for nucleo in ordered_nucleos:
            if not nucleo is None:
                logging.debug("Este es nucleo: " + str(nucleo))
                logging.debug("Este es conteo_nucleo: " + str(conteo_nucleos[nucleo]))
                row = '''
                        <tr>
                            <td {} >{}</td>
                            <td {}>{}</td>
                        </tr>
                        '''.format(style_td, nucleo.encode('utf-8'), style_td, conteo_nucleos[nucleo])
                rows_cndf += row

        message += table_cndf.format(style_th, style_th, rows_cndf) + "<br/>"

        informative_note = '''
                        <p>
                            Un punto de calor se define como una anomalía térmica sobre el terreno, que en realidad es una aproximación a incendios o puntos potenciales de fuego 
                            (Di Bella, y otros, 2008). Se detectan con imágenes de los sensores MODIS y VIIRS que viajan sobre los satélites Terra, Aqua y Suomi-NPP, utilizando 
                            un algoritmo contextual que capta la fuerte emisión de radiación infrarroja media de los posibles incendios (Nasa, 2020). El algoritmo examina cada píxel y asigna a cada uno, 
                            diferentes categorías entre ellas, puntos de calor (Di Bella, y otros, 2008). 
                            </p>
                            <p>
                            Para descargar el informe de puntos de calor de los últimos 7 días haga clic 
                            <a href="https://aplicaciones.siatac.co/jasperserver/rest_v2/reports/reports/puntos_calor/CFgoHis_Sem.pdf?&j_username=fuegos_consulta&j_password=fuegos_consulta" target="_blank" >Aquí.</a> 
                            Tenga en cuenta que si no se detectan puntos de calor en un día, este día no aparecerá en las estadísticas.
                        </p>        
                        '''
        message += informative_note

        email_batch_size = data['email_batch_size']
        fields = ['correo']
        logging.debug("email_batch_size : {} ".format(email_batch_size))

        #####################################################
        ## Obtener destinatarios de correo

        recipients = []
        config = {
            'user': data['mysql_user'],
            'password': data['mysql_password'],
            'host': data['mysql_host'],
            'database': data['mysql_database'],
            'raise_on_warnings': True,
            'ssl_ca': data['mysql_ssl_ca'],
            'ssl_cert': data['mysql_ssl_cert'],
            'ssl_key': data['mysql_ssl_key']
        }

        logging.debug("config : {} ".format(config))
        user_table_name = data['mysql_table_name']
        logging.debug("uncommenting mysql block")
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()

        query = ("SELECT email  FROM {} order by email asc  ".format(user_table_name))
        print("query : {} ".format(query))
        logging.debug("query : {} ".format(query))
        cursor.execute(query)

        for email in cursor:
            print("{}".format(email))
            recipients.append(email)
            logging.debug("{}".format(email))

        cursor.close()
        cnx.close()
        logging.debug("recipients : {} ".format(recipients))
        print("recipients : {} ".format(recipients))
        #####################################################

        n = int(email_batch_size)
        recipients_by_chunks = [recipients[i * n:(i + 1) * n] for i in range((len(recipients) + n - 1) // n)]
        if data["is_test"]:
            recipients_by_chunks = [["edwin.piragauta@skaphe.com"]]
        #recipients_by_chunks = [["edwin.piragauta@skaphe.com"]] #
        logging.debug("recipients_by_chunks : {} ".format(recipients_by_chunks))

        previous_day = (datetime.date.today() - datetime.timedelta(1)).strftime("%Y/%m/%d")
        for recipients in recipients_by_chunks:
            gmail_user = data["gmail_user"]

            body = message.format(time.strftime("%Y/%m/%d - %H:%M"),
                                  total_fuegos,
                                  previous_day,
                                  previous_day
                                  )

            # logging.debug( body )
            subject = "SIATAC - Resumen diario de puntos de calor en la Amazonia colombiana. Fecha: {} .".format(
                time.strftime("%Y/%m/%d - %H:%M"))
            logging.debug("recipients : {} ".format(recipients))
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


##################################################################
##################################################################
'''
Programa principal que coordina la ejecución de los diferentes pasos.
'''


def main(data):
    print("main")
    ##################################################################
    ##################################################################
    reload(sys)
    sys.setdefaultencoding('utf-8')
    temp_dir = data['temp_dir']
    # print(temp_dir)
    logFormat = '%(asctime)-10s %(name)-12s %(levelname)-6s %(message)s'
    # logFormat =  '%(message)s'
    current_day = '{:%Y-%m-%d_%H-%M}'.format(datetime.datetime.now())
    logfile = os.path.join(temp_dir, 'email_fuegos_{}.log'.format(current_day))
    data['logfile'] = logfile
    # print(logfile)
    ##################################################################
    ##################################################################
    logging.basicConfig(level=logging.INFO, format=logFormat, filename=logfile, filemode='w')
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
        create_sde_connections(data)
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
        send_notifications(data)
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
        print (os.path.realpath(__file__) )
        print('sys.argv[0] =', sys.argv[0])
        pathname = os.getcwd()
        if len(sys.argv) > 0:
            pathname = os.path.dirname(sys.argv[0])
        basepath = os.path.abspath(pathname)
        print('path =', pathname)
        print('full path =', basepath)
        config_path = os.path.join(basepath, 'config\config.json')
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
        #logging.debug("Detalles del entorno:")
        #logging.debug(json.dumps(data, sort_keys=True, indent=2, separators=(',', ': ')))
        #logging.debug("***********************************")
        logging.info("**************************************************************************************")
        logging.info("Fin Programa")
        logging.info("**************************************************************************************")
        ##################################################################
        ##################################################################
################################################################################
################################################################################