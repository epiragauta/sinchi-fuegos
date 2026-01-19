# Sistema de Monitoreo de Incendios Forestales SIATAC

- **Versión:** Python 3 - ArcGIS Pro 3.x
- Instituto Amazónico de Investigaciones Científicas SINCHI
- SIATAC (Sistema de Información Ambiental Territorial de la Amazonia Colombiana)

## Descripción

Sistema automatizado para el procesamiento diario de datos satelitales de puntos de calor (incendios forestales) en la región amazónica colombiana. Descarga datos de NASA FIRMS, realiza geoprocesamiento y distribuye reportes diarios por correo electrónico.

## Migración Python 2 → Python 3

Esta versión ha sido migrada desde Python 2.7/ArcGIS Desktop 10.8 a Python 3.9+/ArcGIS Pro 3.x.

### Cambios Principales

| Aspecto | Python 2.7 (Desktop) | Python 3 (Pro) |
|---------|---------------------|----------------|
| Python | 2.7 | 3.9+ |
| ArcGIS | Desktop 10.8 | Pro 3.x |
| Encoding | Manual (`reload`, `setdefaultencoding`) | Automático (UTF-8) |
| ArcPy CalculateField | `PYTHON_9.3` | `PYTHON3` |
| String encoding | `.encode('utf-8')` necesario | Automático |

## Requisitos del Sistema

### Software Necesario

- **ArcGIS Pro 3.x** (con licencia válida)
- **Python 3.9+** (incluido con ArcGIS Pro)
- **PostgreSQL** (servidor de base de datos)
- **MySQL** (para lista de distribución de correos)
- **Conexión a Internet** (para descarga de datos NASA)

### Librerías Python

**Incluidas con ArcGIS Pro:**
- `arcpy` (ArcGIS Python API)
- `requests`
- `pytz`
- `smtplib` (Python estándar)
- `datetime` (Python estándar)

**⚠️ REQUIERE INSTALACIÓN MANUAL:**
- `mysql-connector-python` (ver sección de Instalación de Dependencias más abajo)

## Estructura de Archivos

```
fuegos_python3/
├── Fuegos.py                    # Script principal de procesamiento
├── Enviar_Email_Fuegos.py       # Script de envío de correos
├── fuegos.bat                   # Ejecutor Windows
├── Correos_nuevo.ps1            # Orquestador PowerShell
├── config/
│   └── config.json             # Archivo de configuración
└── README.md                    # Este archivo
```

## Configuración

### 1. Configurar config.json

Editar `config/config.json` con las credenciales y rutas de su entorno:

```json
{
  "temp_dir": "C:\\temp\\fuegos",
  "user_reader": "su_usuario_lectura",
  "user_reader_pwd": "su_password",
  "prod_instance": "servidor:5432",
  "gmail_user": "su_correo@gmail.com",
  "gmail_password": "contraseña_aplicacion_gmail",
  ...
}
```

**Importante:**
- Para Gmail, usar **contraseña de aplicación**, no la contraseña de la cuenta
- Habilitar autenticación de 2 factores en Gmail
- Generar contraseña de aplicación en: https://myaccount.google.com/apppasswords

### 2. Configurar Rutas

Actualizar las rutas en los archivos según su entorno:

**fuegos.bat:**
```batch
cd /d "C:\ruta\a\fuegos_python3"
"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" "C:\ruta\a\fuegos_python3\Fuegos.py"
```

**Correos_nuevo.ps1:**
```powershell
$ARCH_LOG = "C:\ruta\logs\fuegos_$FECH_ACT*.log"
& "C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" "C:\ruta\a\fuegos_python3\Enviar_Email_Fuegos.py"
```

### 3. Instalar Dependencias

⚠️ **IMPORTANTE**: Antes de ejecutar los scripts, debe instalar `mysql-connector-python`.

#### Opción 1: Usando el Script de Instalación (Recomendado)

Ejecutar como **Administrador**:

```batch
instalar_mysql_connector.bat
```

#### Opción 2: Instalación Manual con pip

Abrir **PowerShell como Administrador** y ejecutar:

```powershell
& "C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" -m pip install mysql-connector-python
```

#### Opción 3: Python Package Manager de ArcGIS Pro

1. Abrir **ArcGIS Pro**
2. Ir a: **Settings** → **Python** → **Manage Environments**
3. Clonar el ambiente `arcgispro-py3` (recomendado para no modificar el original)
4. Activar el ambiente clonado
5. Buscar y agregar el paquete: `mysql-connector-python`
6. Actualizar los scripts .bat y .ps1 para usar el nuevo ambiente

#### Verificar Instalación

```batch
"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" -c "import mysql.connector; print('mysql-connector-python instalado correctamente. Version:', mysql.connector.__version__)"
```

Si aparece el mensaje con la versión, la instalación fue exitosa.

## Uso

### Ejecución Manual

#### Procesamiento de Datos

```batch
cd C:\ws\sinchi\ws\fuegos_python3
fuegos.bat
```

O directamente con Python:

```batch
"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" Fuegos.py
```

#### Envío de Correos

```batch
"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" Enviar_Email_Fuegos.py
```

#### Ejecución Completa (Orquestada)

```powershell
powershell -ExecutionPolicy Bypass -File Correos_nuevo.ps1
```

### Ejecución Automática (Tarea Programada)

#### Windows Task Scheduler

1. Abrir **Programador de tareas**
2. Crear tarea básica
3. Configurar trigger diario (ej. 7:00 AM)
4. Acción: Ejecutar `fuegos.bat`
5. Crear segunda tarea para `Correos_nuevo.ps1` (ej. 8:00 AM)

## Modo de Prueba

Para probar el sistema sin afectar la base de datos de producción:

### 1. Preparar Geodatabase de Pruebas (AUTOMATIZADO)

El proyecto incluye un script que prepara automáticamente la geodatabase de pruebas:

#### Opción A: Ejecutar con batch (Recomendado)

```batch
preparar_geodatabase_pruebas.bat
```

#### Opción B: Ejecutar directamente con Python

```batch
"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" preparar_geodatabase_pruebas.py
```

**Este script:**
- ✅ Crea la file geodatabase local en la ruta configurada
- ✅ Copia las capas de referencia desde SDE (con todos los datos):
  - `CPzp2010` - Pozos de hidrocarburos (layer_hidrocarburos)
  - `DLim2014` - Límite región amazónica (layer_dlim)
  - `CPai2014_CCar2014_CDep2014_CMun2014_CElt2019` - Entidades territoriales (layer_union_ent_ref)
  - `Usuarios_Siatac` - Lista de distribución de correos (layer_usuarios_emails)
- ✅ Crea la estructura vacía de las capas de salida (sin datos):
  - `CFgoHis_Car_Mun_Dep_Elt_Pai` - Producción (layer_output_prod)
  - `CFgoHis_Car_Mun_Dep_Elt_Pai_WM` - Publicación Web Mercator (layer_output_pub)
  - `CFgoHis_Car_Mun_Dep_Elt_Pai` - Publicación SIRGAS (layer_output_pub_sirgas)
- ✅ Genera log detallado en `logs/preparar_gdb_*.log`

**Nota:** Los nombres de las capas se extraen automáticamente del archivo `config.json`, tomando la porción después del último punto del path completo de cada capa.

**Requisitos:**
- Conexión a las bases de datos de producción y publicación
- Credenciales configuradas en `config/config.json`
- Permisos de lectura en las capas de SDE

**Tiempo estimado:** 5-15 minutos (dependiendo de la cantidad de datos)

**Salida esperada del script:**
```
================================================================================
PREPARACION DE GEODATABASE DE PRUEBAS
Sistema de Monitoreo de Incendios Forestales - SIATAC
================================================================================

Creando geodatabase: C:\temp\test_data\fuegos_test.gdb
[OK] Geodatabase creada exitosamente

CREANDO CONEXIONES SDE
[OK] Conexion prod_reader creada
[OK] Conexion pub_reader creada
[OK] Conexion prod_edit creada
[OK] Conexion pub_edit creada

COPIANDO CAPAS DE REFERENCIA (CON DATOS)
Capa: Pozos de hidrocarburos
  [OK] Capa 'CPzp2010' copiada exitosamente - 1,234 registros

Capa: Limite region amazonica
  [OK] Capa 'DLim2014' copiada exitosamente - 1 registro

Capa: Union de entidades de referencia
  [OK] Capa 'CPai2014_CCar2014_CDep2014_CMun2014_CElt2019' copiada exitosamente - 567 registros

Capa: Usuarios para distribucion de correos
  [OK] Capa 'Usuarios_Siatac' copiada exitosamente - 89 registros

CREANDO CAPAS DE SALIDA (SOLO ESTRUCTURA)
Capa: Historico de fuegos - Produccion
  [OK] Estructura de 'CFgoHis_Car_Mun_Dep_Elt_Pai' creada exitosamente - 0 registros

Capa: Historico de fuegos - Publicacion Web Mercator
  [OK] Estructura de 'CFgoHis_Car_Mun_Dep_Elt_Pai_WM' creada exitosamente - 0 registros

Capa: Historico de fuegos - Publicacion SIRGAS
  [OK] Estructura de 'CFgoHis_Car_Mun_Dep_Elt_Pai' creada exitosamente - 0 registros

RESUMEN
Geodatabase creada: C:\temp\test_data\fuegos_test.gdb
Capas de referencia copiadas: 4/4
Capas de salida creadas: 3/3

[OK] PROCESO COMPLETADO EXITOSAMENTE

Proximos pasos:
1. Editar config.json y cambiar 'is_test' a true
2. Ejecutar Fuegos.py en modo de prueba
3. Verificar que los datos se procesen correctamente
```

### 2. Configurar Modo de Prueba

Editar `config/config.json`:

```json
{
  "is_test": true,
  "local_gdb": "C:\\temp\\test_data\\fuegos_test.gdb"
}
```

**Importante:** La ruta `local_gdb` debe coincidir con la geodatabase creada en el paso 1.

### 3. Ejecutar en Modo Prueba

```batch
"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" Fuegos.py
```

Los datos se procesarán pero se guardarán en el geodatabase local en lugar de la BD de producción.

### 4. Verificar Resultados

Después de ejecutar en modo prueba:

1. **Revisar logs:**
   ```batch
   type D:\proceso_ptos_calor_produccion\producccion_archivos_procesados\fuegos_*.log
   ```

2. **Verificar datos en geodatabase:**
   - Abrir `C:\temp\test_data\fuegos_test.gdb` en ArcGIS Pro
   - Verificar que las capas de salida tienen datos procesados
   - Verificar que los registros nuevos fueron agregados

3. **Probar envío de correos (opcional):**
   ```batch
   "C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" Enviar_Email_Fuegos.py
   ```

### Solución de Problemas - Modo Prueba

**Error: No se puede conectar a SDE al preparar geodatabase**
- Verificar que tiene acceso a la red donde están los servidores PostgreSQL
- Verificar credenciales en `config/config.json`
- Verificar que los puertos 5432 están abiertos

**Error: Capa de origen no existe**
- Verificar que los nombres de las capas en `config/config.json` son correctos
- Algunas capas pueden haber sido renombradas en SDE
- Contactar al administrador de la base de datos

**Geodatabase incompleta**
- Revisar el log `logs/preparar_gdb_*.log` para identificar qué capas fallaron
- Intentar copiar manualmente las capas faltantes usando ArcGIS Pro

**Error de encoding UTF-8 (ya corregido en la versión actual)**
- El script usa encoding UTF-8 para archivos de log
- Los caracteres especiales se muestran correctamente en consola usando [OK], [ERROR], [ADVERTENCIA]

## Flujo de Procesamiento

### Fuegos.py

1. **Descarga** de shapefiles NASA FIRMS (4 sensores):
   - MODIS (Terra/Aqua)
   - VIIRS Suomi-NPP (VNP) - *actualmente inactivo*
   - NOAA-20 (J1_VIIRS)
   - NOAA-21 (J2_VIIRS)

2. **Geoprocesamiento**:
   - Merge de sensores
   - Reproyección a SIRGAS 4170
   - Clip por límite amazónico
   - Buffer de exclusión (pozos hidrocarburos)
   - Intersección con entidades territoriales

3. **Validación**:
   - Filtrado temporal (24 horas)
   - Deduplicación de registros NASA
   - Verificación contra histórico en BD

4. **Carga a BD**:
   - Append a capas históricas
   - Múltiples proyecciones (SIRGAS, Web Mercator)

### Enviar_Email_Fuegos.py

1. **Consulta** datos del día actual
2. **Agregación** por:
   - Departamento y municipio
   - Corporación autónoma regional
   - Cuencas hidrográficas
   - Núcleos de desarrollo forestal

3. **Generación** de reporte HTML
4. **Envío** de correos masivos (por lotes)

## Sensores Satelitales

### Activos

- **MODIS (Terra/Aqua)**: Resolución ~1km, buffer exclusión 1000m
- **NOAA-20 (J1_VIIRS)**: Resolución 375m, buffer exclusión 375m
- **NOAA-21 (J2_VIIRS)**: Resolución 375m, buffer exclusión 375m

### Inactivos

- **VIIRS Suomi-NPP (VNP)**: Dejó de operar desde agosto 2022
  - Se usa archivo vacío de respaldo: `SUOMI_VIIRS_C2_South_America_24h_vacio.zip`

## Logs y Monitoreo

Los logs se generan en `temp_dir` con el formato:

```
fuegos_AAAA-MM-DD_HH-MM.log
email_fuegos_AAAA-MM-DD_HH-MM.log
```

### Revisar Logs

```batch
type C:\temp\fuegos\fuegos_2025-12-30_07-00.log
```

### Verificar Éxito

El log debe contener al final:

```
**************************************************************************************
Fin Programa
**************************************************************************************
```

## Solución de Problemas

### Error: No se pudo descargar shps de la nasa

**Causa:** Servidores NASA no disponibles o problema de red

**Solución:**
- Verificar conexión a Internet
- El sistema intenta servidor secundario automáticamente
- Reintentos configurables en `max_retries` y `delay_seconds`

### Error: No se pudo crear las conexiones SDE

**Causa:** Credenciales incorrectas o servidor PostgreSQL no disponible

**Solución:**
- Verificar credenciales en `config.json`
- Verificar conectividad al servidor: `ping servidor_produccion`
- Verificar puerto: `telnet servidor_produccion 5432`

### Error: PYTHON_9.3 no reconocido

**Causa:** Código Python 2 ejecutado en Python 3

**Solución:**
- Asegurar que está usando los scripts de `fuegos_python3/`
- Verificar que `fuegos.bat` apunta a Python de ArcGIS Pro

### Error de encoding UTF-8

**Causa:** Posible código antiguo sin migrar

**Solución:**
- Verificar que el archivo Python tiene `# -*- coding: utf-8 -*-` al inicio
- Python 3 maneja UTF-8 automáticamente, no debería haber problemas

## Diferencias con Versión Python 2

### Código Eliminado

```python
# YA NO SE NECESITA:
reload(sys)
sys.setdefaultencoding('utf-8')
texto.encode('utf-8')  # En la mayoría de casos
```

### Código Actualizado

```python
# ANTES (Python 2):
arcpy.CalculateField_management(layer, "field", expr, "PYTHON_9.3", code)

# AHORA (Python 3):
arcpy.CalculateField_management(layer, "field", expr, "PYTHON3", code)
```

### Comportamiento Automático

- **Strings**: Siempre Unicode (UTF-8)
- **Print**: Función, no statement
- **División**: `/` devuelve float, `//` para enteros

## Mantenimiento

### Actualización de URLs NASA

Si NASA cambia las URLs de descarga, actualizar en `config.json`:

```json
{
  "url_modis": "nueva_url_modis.zip",
  "url_modis_2": "nueva_url_modis_servidor2.zip"
}
```

### Reactivación Sensor VNP

Si el sensor VIIRS Suomi-NPP vuelve a operar, actualizar:

```json
{
  "url_vnp": "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/shapes/zips/SUOMI_VIIRS_C2_South_America_24h.zip"
}
```

## Contacto y Soporte

**Instituto SINCHI**
Sistema SIAT-AC
Email: siatac@sinchi.org.co

## Notas Adicionales

### Compatibilidad

- ✅ Compatible con ArcGIS Pro 3.x
- ✅ Python 3.9, 3.10, 3.11
- ❌ No compatible con ArcGIS Desktop (usar versión `fuegos_python2/`)

### Rendimiento

- Procesamiento típico: 5-15 minutos (dependiendo de cantidad de datos)
- Envío de correos: ~40 segundos por lote de 50 destinatarios

### Seguridad

- Nunca commitear `config.json` con credenciales reales a git
- Usar `.gitignore` para excluir archivos de configuración
- Rotar contraseñas regularmente
- Usar contraseñas de aplicación para Gmail, no contraseña principal

---

**Última actualización:** 2025-12-30
**Versión:** 1.0.0 (Python 3)
