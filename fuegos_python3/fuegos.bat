@echo off
REM Script para ejecutar Fuegos.py con Python de ArcGIS Pro 3.x
REM Migrado de Python 2.7 (ArcGIS Desktop 10.8) a Python 3 (ArcGIS Pro)

cd /d "C:\ws\sinchi\ws\fuegos_python3"
"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" "C:\ws\sinchi\ws\fuegos_python3\Fuegos.py"
