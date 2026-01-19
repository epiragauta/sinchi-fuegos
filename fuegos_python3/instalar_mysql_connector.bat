@echo off
REM Script para instalar mysql-connector-python en el ambiente de ArcGIS Pro
REM Ejecutar como Administrador

echo ========================================
echo Instalando mysql-connector-python
echo ========================================
echo.

REM Ruta al pip de ArcGIS Pro
set PYTHON_ARCGIS="C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"

REM Verificar version de Python
echo Verificando version de Python...
%PYTHON_ARCGIS% --version
echo.

REM Instalar mysql-connector-python
echo Instalando mysql-connector-python...
%PYTHON_ARCGIS% -m pip install mysql-connector-python
echo.

REM Verificar instalacion
echo Verificando instalacion...
%PYTHON_ARCGIS% -c "import mysql.connector; print('mysql-connector-python version:', mysql.connector.__version__)"
echo.

echo ========================================
echo Instalacion completada
echo ========================================
pause
