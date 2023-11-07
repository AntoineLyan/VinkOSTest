import dotenv
import os
import time
from helpers.functions import (check_layout, read_files_to_client, create_bkp, conect_db, insert_errors, process_estadistica,
                                process_validate_visitante, insert_bitacora)


dotenv.load_dotenv('variables.env')
user = os.environ.get("user")
pwd = os.environ.get("pass")
pass_db = os.environ.get("pass_db")
port = 3000

        
def main():
    # Obtener el tiempo inicial
    time_inicial = time.time()
    #Leer archivos y pasarlos al local.
    files_found, report_files  = read_files_to_client(user=user, pwd=pwd, port=port)
    #Hacer backup
    create_bkp(files=report_files)
    #Checar Layout
    errores, archivos_validos = check_layout()
    # archivos_validos = ['report_8.txt']
    # files_found = 3
    #Conexión a DB
    conn = conect_db(pass_db=pass_db)
    #Insertar Errores
    insert_errors(conn=conn, df=errores)
    #Inserta tabla en estadisticas con validaciones
    process_estadistica(conn=conn, archivos_validados=archivos_validos)
    #Valida fechas y email , inserta a visitante.
    process_validate_visitante(conn=conn , files=archivos_validos)
    # Obtener el tiempo final
    time_final = time.time()
    # Calcular el tiempo total
    time_total = time_final - time_inicial
    insert_bitacora(conn=conn, list_data=[files_found, len(archivos_validos), time_total])
    conn.close()

if __name__ == '__main__':
    main()
