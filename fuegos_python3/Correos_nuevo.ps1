Set-PSDebug -Off

# Obtener la fecha actual en formato AAAA-MM-DD
$FECH_ACT = Get-Date -Format "yyyy-MM-dd"

# Archivos a revisar
# NOTA: Actualizar estas rutas según la ubicación de los logs en producción
$ARCH_LOG = "D:\proceso_ptos_calor_produccion\producccion_archivos_procesados\fuegos_$FECH_ACT*.log"
$ARCH_ENV = "D:\proceso_ptos_calor_produccion\producccion_archivos_procesados\email_fuegos_$FECH_ACT*.log"

# Verificar si el archivo email no existe
if (-Not (Test-Path $ARCH_ENV)) {
    Write-Output "El archivo $ARCH_ENV no se ha generado. Ejecutando el script..."

    # Verificar si el archivo de fuegos existe
    if (Test-Path $ARCH_LOG) {
        Write-Output "Archivo de fuegos encontrado: $ARCH_LOG"

        # Buscar la línea "Fin Programa" en el archivo
        $ARCH_FIND = Get-ChildItem -Path "D:\proceso_ptos_calor_produccion\producccion_archivos_procesados" -Filter "fuegos_$FECH_ACT*.log"
        $CONTE = Get-Content $ARCH_FIND.FullName
        if ($CONTE -match "Fin Programa") {
            Write-Output "Línea encontrada, iniciando el proceso..."
            # Ejecutar script de envío de correos con Python de ArcGIS Pro 3.x
            & "C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" "C:\ws\sinchi\ws\fuegos_python3\Enviar_Email_Fuegos.py"
        } else {
            Write-Output "Línea no encontrada, el proceso no se ejecutará."
        }
    } else {
        Write-Output "Archivo de fuegos no encontrado con la fecha: $FECH_ACT"
    }
} else {
    Write-Output "El archivo $ARCH_ENV ya ha sido generado. No se ejecutará el script."
}
