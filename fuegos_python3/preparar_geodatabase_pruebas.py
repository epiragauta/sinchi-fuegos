# -*- coding: utf-8 -*-
"""
Script para preparar geodatabase de pruebas para el sistema de monitoreo de incendios
Requiere: Python 3 + ArcGIS Pro

Este script:
1. Crea una file geodatabase local
2. Copia las capas de referencia desde SDE (con datos)
3. Crea la estructura vacía de las capas de salida (sin datos)

Autor: Sistema SIATAC - Instituto SINCHI
"""

import arcpy
import os
import sys
import json
import logging
from datetime import datetime

def get_layer_name_from_path(layer_path):
    """Extrae el nombre de la capa del path completo (texto después del último punto)"""
    # Ejemplo: "\\labsigysr_corp.e1b_geodata_co.ANH\\labsigysr_corp.e1b_geodata_co.CPzp2010"
    # Debe retornar: "CPzp2010"
    if not layer_path:
        return None

    # Dividir por backslash y tomar la última parte
    parts = layer_path.split('\\')
    last_part = parts[-1] if parts else layer_path

    # Dividir por punto y tomar la última parte
    name_parts = last_part.split('.')
    layer_name = name_parts[-1] if name_parts else last_part

    return layer_name

def setup_logging(output_dir):
    """Configura el sistema de logging"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    log_file = os.path.join(output_dir, f'preparar_gdb_{datetime.now():%Y%m%d_%H%M%S}.log')

    # Configurar formato
    log_format = '%(asctime)s - %(levelname)s - %(message)s'

    # Handler para archivo con encoding UTF-8
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format))

    # Handler para consola (sin caracteres especiales)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format))

    # Configurar logger raíz
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return log_file

def create_sde_connection(temp_dir, username, password, instance, database_name, conn_name):
    """Crea una conexión SDE temporal"""
    logging.info(f"Creando conexión SDE: {conn_name}")
    try:
        authType = "DATABASE_AUTH"
        saveUserInfo = "SAVE_USERNAME"
        versionName = "SDE.DEFAULT"

        out_workspace = arcpy.CreateDatabaseConnection_management(
            temp_dir,
            conn_name,
            "POSTGRESQL",
            instance,
            authType,
            username,
            password,
            saveUserInfo,
            database_name,
            "#",
            "TRANSACTIONAL",
            versionName
        )

        conn_path = out_workspace.getOutput(0)
        logging.info(f"Conexión creada: {conn_path}")
        return conn_path
    except Exception as e:
        logging.error(f"Error creando conexión {conn_name}: {str(e)}")
        raise

def create_test_geodatabase(gdb_path):
    """Crea la file geodatabase de pruebas"""
    logging.info("=" * 80)
    logging.info("CREANDO FILE GEODATABASE DE PRUEBAS")
    logging.info("=" * 80)

    gdb_dir = os.path.dirname(gdb_path)
    gdb_name = os.path.basename(gdb_path)

    # Crear directorio si no existe
    if not os.path.exists(gdb_dir):
        logging.info(f"Creando directorio: {gdb_dir}")
        os.makedirs(gdb_dir)

    # Eliminar geodatabase si ya existe
    if arcpy.Exists(gdb_path):
        logging.warning(f"Geodatabase ya existe. Eliminando: {gdb_path}")
        arcpy.Delete_management(gdb_path)

    # Crear geodatabase
    logging.info(f"Creando geodatabase: {gdb_path}")
    arcpy.CreateFileGDB_management(gdb_dir, gdb_name)
    logging.info("Geodatabase creada exitosamente")

    return gdb_path

def copy_reference_layer(source_conn, layer_path, gdb_path, output_name):
    """Copia una capa de referencia con todos sus datos"""
    logging.info("-" * 80)
    logging.info(f"Copiando capa de referencia: {output_name}")

    try:
        # Construir ruta completa de origen
        source_path = source_conn + layer_path
        output_path = os.path.join(gdb_path, output_name)

        # Verificar que la capa de origen existe
        if not arcpy.Exists(source_path):
            logging.error(f"ADVERTENCIA: Capa de origen no existe: {source_path}")
            return False

        # Obtener conteo de registros
        count = int(arcpy.GetCount_management(source_path)[0])
        logging.info(f"Capa origen: {source_path}")
        logging.info(f"Registros a copiar: {count}")

        # Copiar la capa
        logging.info("Copiando datos...")
        arcpy.FeatureClassToFeatureClass_conversion(source_path, gdb_path, output_name)

        # Verificar copia
        output_count = int(arcpy.GetCount_management(output_path)[0])
        logging.info(f"Registros copiados: {output_count}")

        if output_count == count:
            logging.info(f"[OK] Capa '{output_name}' copiada exitosamente")
            return True
        else:
            logging.warning(f"[ADVERTENCIA] Conteo de registros no coincide ({count} vs {output_count})")
            return True

    except Exception as e:
        logging.error(f"[ERROR] Error copiando capa '{output_name}': {str(e)}")
        return False

def create_empty_output_layer(source_conn, layer_path, gdb_path, output_name):
    """Crea una capa de salida con la estructura pero sin datos"""
    logging.info("-" * 80)
    logging.info(f"Creando estructura vacía: {output_name}")

    try:
        # Construir ruta completa de origen
        source_path = source_conn + layer_path
        output_path = os.path.join(gdb_path, output_name)

        # Verificar que la capa de origen existe
        if not arcpy.Exists(source_path):
            logging.error(f"ADVERTENCIA: Capa de origen no existe: {source_path}")
            return False

        logging.info(f"Capa origen: {source_path}")

        # Crear capa vacía con la misma estructura
        logging.info("Creando estructura de capa...")

        # Obtener descripción de la capa de origen
        desc = arcpy.Describe(source_path)

        # Crear feature class vacío
        arcpy.CreateFeatureclass_management(
            gdb_path,
            output_name,
            desc.shapeType,
            source_path,  # Usar como template
            "SAME_AS_TEMPLATE",
            "SAME_AS_TEMPLATE",
            desc.spatialReference
        )

        # Verificar creación
        if arcpy.Exists(output_path):
            count = int(arcpy.GetCount_management(output_path)[0])
            logging.info(f"Registros: {count} (debe ser 0)")
            logging.info(f"[OK] Estructura de '{output_name}' creada exitosamente")
            return True
        else:
            logging.error(f"[ERROR] No se pudo crear la capa '{output_name}'")
            return False

    except Exception as e:
        logging.error(f"[ERROR] Error creando estructura de '{output_name}': {str(e)}")
        return False

def main():
    """Función principal"""
    print("=" * 80)
    print("SCRIPT DE PREPARACIÓN DE GEODATABASE DE PRUEBAS")
    print("Sistema de Monitoreo de Incendios Forestales - SIATAC")
    print("=" * 80)
    print()

    # Leer configuración
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config', 'config.json')

    print(f"Leyendo configuración: {config_path}")

    if not os.path.exists(config_path):
        print(f"ERROR: Archivo de configuración no encontrado: {config_path}")
        sys.exit(1)

    with open(config_path) as f:
        config = json.load(f)

    # Configurar logging
    log_dir = os.path.join(script_dir, 'logs')
    log_file = setup_logging(log_dir)

    logging.info("=" * 80)
    logging.info("INICIO DEL PROCESO")
    logging.info("=" * 80)
    logging.info(f"Archivo de log: {log_file}")

    try:
        # Obtener ruta de la geodatabase
        gdb_path = config['local_gdb']
        logging.info(f"Geodatabase destino: {gdb_path}")

        # Crear directorio temporal para conexiones SDE
        temp_dir = os.path.join(script_dir, 'temp_connections')
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)

        # Crear geodatabase
        create_test_geodatabase(gdb_path)

        # Crear conexiones SDE
        logging.info("")
        logging.info("=" * 80)
        logging.info("CREANDO CONEXIONES SDE")
        logging.info("=" * 80)

        # Conexión a base de datos de producción (lectura)
        prod_conn = create_sde_connection(
            temp_dir,
            config['user_reader'],
            config['user_reader_pwd'],
            config['prod_instance'],
            config['prod_database_name'],
            'prod_reader'
        )

        # Conexión a base de datos de publicación (lectura)
        pub_conn = create_sde_connection(
            temp_dir,
            config['pub_user_reader'],
            config['pub_user_reader_pwd'],
            config['pub_instance'],
            config['pub_database_name'],
            'pub_reader'
        )

        # Conexión a base de datos de producción (escritura) - para capas de salida
        prod_edit_conn = create_sde_connection(
            temp_dir,
            config['prod_user_edit'],
            config['prod_user_edit_pwd'],
            config['prod_edit_instance'],
            config['prod_database_name'],
            'prod_edit'
        )

        # Conexión a base de datos de publicación (escritura) - para capas de salida
        pub_edit_conn = create_sde_connection(
            temp_dir,
            config['pub_user_edit'],
            config['pub_user_edit_pwd'],
            config['pub_edit_instance'],
            config['pub_database_name'],
            'pub_edit'
        )

        # Copiar capas de referencia (CON DATOS)
        logging.info("")
        logging.info("=" * 80)
        logging.info("COPIANDO CAPAS DE REFERENCIA (CON DATOS)")
        logging.info("=" * 80)

        reference_layers = [
            {
                'conn': prod_conn,
                'layer': config['layer_hidrocarburos'],
                'name': get_layer_name_from_path(config['layer_hidrocarburos']),
                'description': 'Pozos de hidrocarburos'
            },
            {
                'conn': prod_conn,
                'layer': config['layer_dlim'],
                'name': get_layer_name_from_path(config['layer_dlim']),
                'description': 'Límite región amazónica'
            },
            {
                'conn': prod_conn,
                'layer': config['layer_union_ent_ref'],
                'name': get_layer_name_from_path(config['layer_union_ent_ref']),
                'description': 'Unión de entidades de referencia (país, car, departamento, municipio)'
            },
            {
                'conn': pub_conn,
                'layer': config['layer_usuarios_emails'],
                'name': get_layer_name_from_path(config['layer_usuarios_emails']),
                'description': 'Usuarios para distribución de correos'
            }
        ]

        copied_count = 0
        for layer_info in reference_layers:
            logging.info(f"\nCapa: {layer_info['description']}")
            if copy_reference_layer(
                layer_info['conn'],
                layer_info['layer'],
                gdb_path,
                layer_info['name']
            ):
                copied_count += 1

        # Crear capas de salida (SOLO ESTRUCTURA, SIN DATOS)
        logging.info("")
        logging.info("=" * 80)
        logging.info("CREANDO CAPAS DE SALIDA (SOLO ESTRUCTURA)")
        logging.info("=" * 80)

        output_layers = [
            {
                'conn': prod_edit_conn,
                'layer': config['layer_output_prod'],
                'name': get_layer_name_from_path(config['layer_output_prod']),
                'description': 'Histórico de fuegos - Producción'
            },
            {
                'conn': pub_edit_conn,
                'layer': config['layer_output_pub'],
                'name': get_layer_name_from_path(config['layer_output_pub']),
                'description': 'Histórico de fuegos - Publicación Web Mercator'
            },
            {
                'conn': pub_edit_conn,
                'layer': config['layer_output_pub_sirgas'],
                'name': get_layer_name_from_path(config['layer_output_pub_sirgas']),
                'description': 'Histórico de fuegos - Publicación SIRGAS'
            }
        ]

        created_count = 0
        for layer_info in output_layers:
            logging.info(f"\nCapa: {layer_info['description']}")
            if create_empty_output_layer(
                layer_info['conn'],
                layer_info['layer'],
                gdb_path,
                layer_info['name']
            ):
                created_count += 1

        # Resumen final
        logging.info("")
        logging.info("=" * 80)
        logging.info("RESUMEN")
        logging.info("=" * 80)
        logging.info(f"Geodatabase creada: {gdb_path}")
        logging.info(f"Capas de referencia copiadas: {copied_count}/{len(reference_layers)}")
        logging.info(f"Capas de salida creadas: {created_count}/{len(output_layers)}")

        if copied_count == len(reference_layers) and created_count == len(output_layers):
            logging.info("")
            logging.info("[OK] PROCESO COMPLETADO EXITOSAMENTE")
            logging.info("")
            logging.info("Proximos pasos:")
            logging.info("1. Editar config.json y cambiar 'is_test' a true")
            logging.info("2. Ejecutar Fuegos.py en modo de prueba")
            logging.info("3. Verificar que los datos se procesen correctamente")
        else:
            logging.warning("")
            logging.warning("[ADVERTENCIA] PROCESO COMPLETADO CON ADVERTENCIAS")
            logging.warning("Revisar los errores anteriores")

        # Limpiar conexiones temporales
        logging.info("")
        logging.info("Limpiando conexiones temporales...")
        if os.path.exists(temp_dir):
            import shutil
            shutil.rmtree(temp_dir)

    except Exception as e:
        logging.error("=" * 80)
        logging.error("ERROR CRÍTICO")
        logging.error("=" * 80)
        logging.error(str(e))
        import traceback
        logging.error(traceback.format_exc())
        sys.exit(1)

    finally:
        logging.info("")
        logging.info("=" * 80)
        logging.info("FIN DEL PROCESO")
        logging.info("=" * 80)
        logging.info(f"Log guardado en: {log_file}")

if __name__ == "__main__":
    main()
