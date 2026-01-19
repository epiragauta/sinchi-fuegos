# Cambios en la Migración Python 2 → Python 3

## Resumen de Correcciones

Este documento registra todos los cambios realizados durante la migración del sistema de monitoreo de incendios forestales de Python 2.7/ArcGIS Desktop 10.8 a Python 3/ArcGIS Pro 3.x.

---

## 1. Encoding UTF-8

### Problema
Python 2 requería configuración manual de encoding:
```python
reload(sys)
sys.setdefaultencoding('utf-8')
```

### Solución
Eliminado - Python 3 usa UTF-8 por defecto.

**Archivos afectados:**
- `Fuegos.py` (línea 1407)
- `Enviar_Email_Fuegos.py` (línea 580)

---

## 2. String Encoding en HTML

### Problema
Python 2 requería `.encode('utf-8')` para strings en HTML:
```python
depto.encode('utf-8')
municipio_max_name = muni[1].encode('utf-8')
```

### Solución
Eliminado - Python 3 maneja strings unicode automáticamente.

**Archivos afectados:**
- `Fuegos.py` (líneas 1245, 1255)
- `Enviar_Email_Fuegos.py` (líneas 352, 362, 430, 440, 474)

---

## 3. ArcPy CalculateField - PYTHON_9.3 → PYTHON3

### Problema
ArcGIS Desktop usaba `PYTHON_9.3` como lenguaje de expresión:
```python
arcpy.CalculateField_management(layer, "field", expr, "PYTHON_9.3", code)
```

### Solución
Cambiado a `PYTHON3`:
```python
arcpy.CalculateField_management(layer, "field", expr, "PYTHON3", code)
```

**Archivos afectados:**
- `Fuegos.py` (11 ocurrencias - líneas 698, 703, 724, 732, 738, 750, 756, 767, 772, 877, 882)

---

## 4. Separadores de Ruta en Windows

### Problema
Uso de backslash literal en rutas:
```python
config_path = os.path.join(basepath, 'config\config.json')
```

### Solución
Uso de separador portable:
```python
config_path = os.path.join(basepath, 'config', 'config.json')
```

**Archivos afectados:**
- `Fuegos.py` (línea 1539)
- `Enviar_Email_Fuegos.py` (línea 660)

---

## 5. datetime.now() → datetime.datetime.now()

### Problema
Error en modo de prueba:
```python
fecha_actual = datetime.now()  # ❌ Error en Python 3
```

### Solución
Uso correcto del módulo:
```python
fecha_actual = datetime.datetime.now()  # ✅ Correcto
```

**Archivos afectados:**
- `Fuegos.py` (línea 1466)

---

## 6. Indentación en Codeblocks de CalculateField

### Problema
Codeblocks con indentación inicial causan error en Python 3:
```python
codeblock = """
            def getClass(confidence_modis):  # ❌ Indentación inválida
                if confidence < 30:
                    return 'low'"""
```

Error: `IndentationError: unexpected indent`

### Solución
Codeblocks sin indentación inicial:
```python
codeblock = """def getClass(confidence_modis):  # ✅ Correcto
    if confidence < 30:
        return 'low'"""
```

**Diferencia Python 2 vs Python 3:**
- Python 2 (`PYTHON_9.3`): Toleraba indentación inicial en strings
- Python 3 (`PYTHON3`): Requiere que el código comience sin espacios antes de `def`

**Archivos afectados:**
- `Fuegos.py` - 3 codeblocks:
  - Línea 714: Clasificación CONFIDENCE para MODIS
  - Línea 874: Cálculo FECHA_DESC
  - Línea 879: Cálculo FECHA_DATE

---

## 7. AlterField con Longitud Inválida

### Problema
AlterField intentaba reducir longitud de campo a 1 carácter:
```python
arcpy.AlterField_management(layer, "SATELLITE", "SATELLITEOLD", "", "TEXT", "1", "NULLABLE", "false")
# ❌ Error: No se puede reducir longitud si hay datos
```

### Solución
Solo renombrar el campo sin cambiar tipo:
```python
arcpy.AlterField_management(layer, "SATELLITE", "SATELLITEOLD")
# ✅ Correcto: Solo cambia el nombre
```

**Contexto:**
Este cambio permite aumentar la longitud del campo SATELLITE de su tamaño original a 256 caracteres para manejar nombres largos de satélites NASA.

**Proceso correcto:**
1. Renombrar `SATELLITE` → `SATELLITEOLD` (mantener tipo original)
2. Crear nuevo campo `SATELLITE` TEXT(256)
3. Copiar valores de `SATELLITEOLD` → `SATELLITE`
4. Eliminar `SATELLITEOLD`

**Archivos afectados:**
- `Fuegos.py` - 4 sensores satelitales:
  - Línea 695: `shp_modis_1` (MODIS)
  - Línea 730: `shp_vnp_1` (VIIRS Suomi-NPP)
  - Línea 747: `shp_nooa_1` (VIIRS NOAA-20)
  - Línea 765: `shp_nooa_21_1` (VIIRS NOAA-21)

---

## 8. Exclusión de Campos de Sistema en FieldMappings

### Problema
La función `get_field_mappings` intentaba agregar campos de sistema al FieldMap:
```python
for field in fields:
    if field.name == "shape":  # ❌ Solo excluye "shape" en minúsculas
        continue
    fm.addInputField(lyr, field.name)  # Error con OBJECTID, Shape, etc.
```

**Error:**
```
FieldMap: Error in adding input field to field map
OBJECTID is a type of OID
Shape is a type of Geometry
```

### Solución
Excluir campos basándose en su **tipo**, no en su nombre:
```python
for field in fields:
    # Excluir campos de sistema: OID, Geometry, GlobalID
    if field.type in ['OID', 'Geometry', 'GlobalID']:  # ✅ Correcto
        continue
    fm.addInputField(lyr, field.name)
```

**Campos de sistema excluidos:**
- `OID`: OBJECTID, FID, etc.
- `Geometry`: Shape, SHAPE, etc.
- `GlobalID`: Identificador global único

**Por qué:**
Los campos de sistema son gestionados automáticamente por ArcGIS y no deben ser agregados manualmente a FieldMaps.

**Archivos afectados:**
- `Fuegos.py` (línea 1136-1158: función `get_field_mappings`)

---

## 9. Script de Preparación de Geodatabase

### Problema Original
Nombres de capas genéricos y error de encoding UTF-8 en logs.

### Solución
- **Extracción automática de nombres:** Función `get_layer_name_from_path()` extrae el nombre correcto del config.json
- **Logging UTF-8:** Handler de archivo con `encoding='utf-8'`
- **Caracteres ASCII:** Reemplazados símbolos especiales (✓→[OK], ✗→[ERROR])

**Archivo afectado:**
- `preparar_geodatabase_pruebas.py`

---

## Archivos Nuevos Creados

### Scripts de Ejecución
- `fuegos.bat` - Ejecutor para ArcGIS Pro
- `Correos_nuevo.ps1` - Orquestador PowerShell actualizado
- `preparar_geodatabase_pruebas.py` - Preparación de ambiente de pruebas
- `preparar_geodatabase_pruebas.bat` - Ejecutor del script de preparación

### Configuración y Documentación
- `config/config.json` - Configuración actualizada con credenciales reales
- `config/config.json.example` - Plantilla de ejemplo (versionable)
- `.gitignore` - Protección de archivos sensibles
- `README.md` - Documentación completa
- `CAMBIOS_MIGRACION.md` - Este archivo

---

## Compatibilidad

### Librerías Python
Todas las librerías son compatibles con Python 3:
- ✅ `arcpy` (ArcGIS Pro)
- ✅ `requests` (incluido en ArcGIS Pro)
- ✅ `mysql.connector` ⚠️ **REQUIERE INSTALACIÓN MANUAL**
- ✅ `smtplib` (incluido en Python estándar)
- ✅ `datetime` (incluido en Python estándar)
- ✅ `pytz` (incluido en ArcGIS Pro)

### Instalación de Librerías Adicionales

**IMPORTANTE**: El módulo `mysql-connector-python` NO viene preinstalado en ArcGIS Pro y debe instalarse manualmente.

**Opción 1 - Usando pip (Recomendado)**:
```batch
# Ejecutar como Administrador
"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" -m pip install mysql-connector-python
```

O usar el script incluido:
```batch
instalar_mysql_connector.bat
```

**Opción 2 - Usando Python Package Manager de ArcGIS Pro**:
1. Abrir ArcGIS Pro
2. Settings → Python → Manage Environments
3. Clonar el ambiente `arcgispro-py3` (recomendado)
4. Agregar el paquete "mysql-connector-python"

**Verificar instalación**:
```python
python -c "import mysql.connector; print(mysql.connector.__version__)"
```

### Funciones ArcPy Sin Cambios
Estas funciones funcionan igual en ArcGIS Desktop y Pro:
- `arcpy.CreateDatabaseConnection_management`
- `arcpy.da.SearchCursor` / `UpdateCursor`
- `arcpy.Clip_analysis`
- `arcpy.Project_management`
- `arcpy.Merge_management`
- `arcpy.Append_management`
- Spatial references (4170, 3857)

---

## Resumen Estadístico

| Categoría | Cantidad |
|-----------|----------|
| Archivos migrados | 2 (Fuegos.py, Enviar_Email_Fuegos.py) |
| Archivos nuevos | 9 |
| Líneas modificadas | ~40 |
| Cambios PYTHON_9.3 | 11 |
| Cambios .encode() | 7 |
| Cambios codeblock indentación | 3 |
| Cambios AlterField | 4 |
| Cambios FieldMappings | 1 |
| Cambios datetime | 1 |

---

## Estado de la Migración

✅ **COMPLETADA** - 2025-01-02

El sistema ha sido completamente migrado a Python 3/ArcGIS Pro 3.x y está listo para pruebas.

---

## Próximos Pasos

1. **Instalar dependencias requeridas**:
   ```batch
   instalar_mysql_connector.bat
   ```

2. Ejecutar `preparar_geodatabase_pruebas.bat` para crear ambiente de prueba

3. Configurar `is_test: true` en `config/config.json`

4. Ejecutar `Fuegos.py` en modo prueba

5. Verificar resultados en geodatabase local

6. Ejecutar `Enviar_Email_Fuegos.py` en modo prueba (enviará correo solo a dirección de prueba)

7. Si las pruebas son exitosas, configurar `is_test: false` y ejecutar en producción

---

**Fecha de migración:** 2025-01-02
**Migrado por:** Claude Code (Anthropic)
**Versión Python:** 2.7 → 3.9+
**Versión ArcGIS:** Desktop 10.8 → Pro 3.x
