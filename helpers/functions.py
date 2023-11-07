import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import datetime
from email_validator import validate_email, EmailNotValidError
import zipfile
import pysftp
import os

def conect_db(pass_db):
    try:
        connection = mysql.connector.connect(host='localhost',
                                            database='vinkosdb',
                                            user='root',
                                            password=pass_db)
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            return connection


    except Error as e:
        print("Error while connecting to MySQL", e)

def read_files_to_client(user, pwd, port):
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # Deshabilita la comprobación de clave de host, no debe hacerse en productivo. 

    with pysftp.Connection('localhost', username=user, password=pwd, port=port, cnopts=cnopts) as sftp:

        with sftp.cd('archivosVisitas'):           
            # Obtén una lista de todos los archivos en el servidor FTP
            files = sftp.listdir()
            #print(files)

            # Filtra la lista para que solo contenga archivos .txt que empiecen con "report"
            report_files = [file for file in files if file.endswith(".txt") and file.startswith("report")]
            #print(report_files)

            for file in report_files:
                sftp.get(file, "archivostxt/" + file)
                #! Elimino los archivos que estan en carpeta del cliente.
                #sftp.remove(file)
    found_files  = len(report_files)
    return found_files, report_files

def create_bkp(files : list):
    date = datetime.now()
    date_str = date.strftime('%Y-%m-%d-%H-%M-%S')

    archivo_zip = zipfile.ZipFile(f'visitas/bckp/{date_str}.zip', 'w')

    for archivo in files:
        archivo_zip.write('archivostxt/' + archivo, arcname=archivo)

    archivo_zip.close()

#Funcion para leer los archivos, comprobar layout, registros de errores.
def check_layout():
    files = os.listdir("./archivostxt")
    data = {}
    error_data = []
    valid_data = []
    type_expect_cols = ["object", "float64", "object", "object", "object", "object"
                    , "int64", "int64", "object", "int64", "int64", "object", "object", "object", "object" ]
    for file in files:
        data[file] = pd.read_csv("./archivostxt/" + file)

    for file, df in data.items():
        valid_file = True
        # Validar la cantidad de columnas
        if len(df.columns) != 15:
            error_data.append({
                "Fecha": pd.Timestamp("now"),
                "Archivo": file,
                "Error": "El archivo no tiene 14 columnas"
            })
            valid_file = False
        
        
        # Validar el tipo de dato de cada columna
        errors = []
        for col_name, type_cols, expected_type in zip(df.columns, df.dtypes, type_expect_cols):
            if type_cols != expected_type:
                errors.append(f"El tipo de dato de la columna {col_name} no es válido.")

        if errors:
            error_data.append({
                "Fecha": pd.Timestamp("now"),
                "Archivo": file,
                "Error": ", ".join(errors)
            })
            valid_file = False
    
        # Validar el orden y nombres de las columnas
        expected_columns = ['email', 'jyv', 'Badmail', 'Baja', 'Fecha envio', 'Fecha open', 'Opens',
                            'Opens virales', 'Fecha click', 'Clicks', 'Clicks virales', 'Links',
                            'IPs', 'Navegadores', 'Plataformas']
        #!Es jyv no jk
        error_columns = [col for col in expected_columns if col not in df.columns]
        if error_columns:
            error_data.append({
                "Fecha": pd.Timestamp("now"),
                "Archivo": file,
                "Error": f"Ops error en columnas. Columnas faltantes: {', '.join(error_columns)}"
            })
            valid_file = False
        
        if valid_file:
            # Si todas las validaciones pasaron, agregamos el archivo a 'valid_data'
            valid_data.append(file)

        # Validar email
        for row_number, email in enumerate(df['email']):
            df.at[row_number, 'valid_email'] = validate_email_column(email, row_number, error_data, file)     
        #invalid_emails = df[df['valid_email'] != 'Correcto']
        #print(invalid_emails.head(5))

    error_df = pd.DataFrame(error_data)
    return error_df, valid_data

def insert_errors(conn,df):
    for _ , row in df.iterrows():
        query = ("INSERT INTO vinkosdb.errores (nameRegister, messageError)"
        "VALUES (%s, %s)")
        data = (row['Archivo'], row['Error'])
        cursor = conn.cursor()
        cursor.execute(query, data)
        conn.commit()
        cursor.close()

def insert_estadistica(conn, df):
    for _ , row in df.iterrows():
        query = ("INSERT INTO vinkosdb.estadistica (email, jyv, badmail, baja, fechaEnvio, fechaOpen, opens,"
        "opensVirales, fechaClick, clicks, clicksVirales, links, IPs, Navegadores, Plataformas)"
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        data = (row['email'], row['jyv'], row['Badmail'], row['Baja'], row['Fecha envio'], row['Fecha open'],
                row['Opens'], row['Opens virales'], row['Fecha click'], row['Clicks'], row['Clicks virales'],
                row['Links'], row['IPs'], row['Navegadores'], row['Plataformas'])
        cursor = conn.cursor()
        cursor.execute(query, data)
        conn.commit()
        cursor.close()

def convertir_valor(valor):
    if valor in ('-', 'NaN'):
        return None
    return valor

def formateo_NaN(df):
    # Reemplazar NaN en columnas de tipo string por None
    string_columns = df.select_dtypes(include=[object]).columns
    for columna in string_columns:
        df[columna] = df[columna].apply(convertir_valor)

    df[string_columns] = df[string_columns].where(df[string_columns].notna(), None)

    # Llena NaN en columnas de tipo int con 0
    int_columns = df.select_dtypes(include=[int]).columns
    df[int_columns] = df[int_columns].fillna(0)

    # Llena NaN en columnas de tipo float con 0.0
    float_columns = df.select_dtypes(include=[float]).columns
    df[float_columns] = df[float_columns].fillna(0.0)
    return df

# Función para convertir el formato de fecha
def convertir_fecha(fecha_str):
    if fecha_str != None:
        fecha_obj = datetime.strptime(fecha_str, '%d/%m/%Y %H:%M')
        return fecha_obj.strftime('%Y-%m-%d %H:%M:%S')
    else:
        return None

def convertir_columns_df(df):
    # Aplica la conversión a las columnas de fecha en tu DataFrame
    columnas_de_fecha = ['Fecha envio', 'Fecha open', 'Fecha click']
    for columna in columnas_de_fecha:
        df[columna] = df[columna].apply(convertir_fecha)
    return df


def process_estadistica(conn, archivos_validados : list):
    for file in archivos_validados:
        df = pd.read_csv('archivostxt/'+ file, sep=',')
        df = formateo_NaN(df)
        df = convertir_columns_df(df)
        insert_estadistica(conn=conn, df=df)

def validate_email_column_simple(email):
    try:
        validate_email(email)
        return True
    except EmailNotValidError as e:
        return False

def validate_email_column(email, row_number, error_data, file):
    try:
        validate_email(email)
        return 'Correcto'
    except EmailNotValidError as e:
        error_message = f"Fila {row_number + 1 }, error de correo: {str(e)}"
        error_data.append({
            "Fecha": pd.Timestamp("now"),
            "Archivo": file,
            "Error": error_message
        })
        return error_message

def process_validate_visitante(conn, files : list ):
    final_result = pd.DataFrame()
    for file in files: 
        df = pd.read_csv('archivostxt/'+ file, sep=',')

        df['Fecha envio'] = pd.to_datetime(df['Fecha envio'], format='%d/%m/%Y %H:%M', errors='coerce')
        df['Fecha open'] = pd.to_datetime(df['Fecha open'], format='%d/%m/%Y %H:%M', errors='coerce')
        df['Fecha click'] = pd.to_datetime(df['Fecha click'], format='%d/%m/%Y %H:%M', errors='coerce')

        # Calcula la fecha de última visita y primera
        df['fechaUltimaVisita'] = df[['Fecha envio', 'Fecha open', 'Fecha click']].max(axis=1)
        df['fechaPrimeraVisita'] = df[['Fecha envio', 'Fecha open', 'Fecha click']].min(axis=1)

        # Cuenta el número total de visitas
        visitas_totales = df.groupby('email').size().reset_index(name='visitasTotales')

        # Filtra las filas correspondientes al año y mes actual
        fecha_actual = datetime.now()
        mes_actual = fecha_actual.month
        anio_actual = fecha_actual.year
        visitas_anio_actual = df[(df['Fecha envio'].dt.year == anio_actual)].groupby('email').size().reset_index(name='visitasAnioActual')
        visitas_mes_actual = df[df['Fecha envio'].dt.month == mes_actual].groupby('email').size().reset_index(name='visitasMesActual')

        resultados = visitas_totales.merge(df[['email', 'fechaPrimeraVisita', 'fechaUltimaVisita']], on='email', how='left')
        resultados = resultados.merge(visitas_anio_actual, on='email', how='left')
        resultados = resultados.merge(visitas_mes_actual, on='email', how='left')

        resultados.fillna(0, downcast="int64", inplace=True)

        final_result = pd.concat([final_result, resultados])

        final_result['email_valid'] = final_result['email'].apply(validate_email_column_simple)
        final_result['fechaPrimeraVisita'] = final_result.groupby('email')['fechaPrimeraVisita'].transform('min')
        final_result['fechaUltimaVisita'] = final_result.groupby('email')['fechaUltimaVisita'].transform('max')
        final_result.drop_duplicates(subset='email', keep='last', inplace=True)

        filter_by_email_valid = final_result[final_result['email_valid'] == True]

        insert_to_visitantes(conn=conn, df=filter_by_email_valid)


def insert_to_visitantes(conn, df):
    for _, row in df.iterrows():
        cursor = conn.cursor()
        # Consulta SQL para obtener los valores actuales
        query_select = "SELECT visitasTotales, visitasAnioActual, visitasMesActual FROM vinkosdb.visitante WHERE email = %s"
        data = (row['email'],)
        cursor.execute(query_select, data)
        current_data = cursor.fetchone()
        
        visitasTotales = row['visitasTotales']
        visitasAnioActual = row['visitasAnioActual']
        visitasMesActual = row['visitasMesActual']

        if current_data:
            # Si el correo electrónico ya existe en la tabla, obtén los valores actuales
            current_visitasTotales, current_visitasAnioActual, current_visitasMesActual = current_data

            # Suma las nuevas visitas a los valores actuales
            visitasTotales += current_visitasTotales
            visitasAnioActual += current_visitasAnioActual
            visitasMesActual += current_visitasMesActual

        # Consulta SQL para insertar o actualizar registros
        query = (
            "INSERT INTO visitante (email, fechaPrimeraVisita, fechaUltimaVisita, visitasTotales, visitasAnioActual, visitasMesActual) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "fechaUltimaVisita = VALUES(fechaUltimaVisita), "
            "visitasTotales = VALUES(visitasTotales), "
            "visitasAnioActual = VALUES(visitasAnioActual), "
            "visitasMesActual = VALUES(visitasMesActual)"
        )
        data = (row['email'],row['fechaPrimeraVisita'],row['fechaUltimaVisita'],
                visitasTotales, visitasAnioActual, visitasMesActual)

        cursor.execute(query, data)
        conn.commit()
        cursor.close()

def insert_bitacora(conn, list_data : list):
    query = ("INSERT INTO vinkosdb.bitacora (registersFounds, registersCorrects,processingTime)"
    "VALUES (%s, %s, %s)")
    data = (list_data[0], list_data[1], list_data[2])
    cursor = conn.cursor()
    cursor.execute(query, data)
    conn.commit()
    cursor.close()

