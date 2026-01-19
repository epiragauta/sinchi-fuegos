@echo off
REM Script para preparar geodatabase de pruebas
REM Requiere ArcGIS Pro 3.x

echo ================================================================================
echo PREPARACION DE GEODATABASE DE PRUEBAS
echo Sistema de Monitoreo de Incendios Forestales - SIATAC
echo ================================================================================
echo.

cd /d "C:\ws\sinchi\ws\fuegos_python3"

echo Ejecutando script de preparacion...
echo.

"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" preparar_geodatabase_pruebas.py

echo.
echo ================================================================================
echo Proceso finalizado
echo ================================================================================
echo.
echo Revisar el archivo de log en: logs\preparar_gdb_*.log
echo.

pause
