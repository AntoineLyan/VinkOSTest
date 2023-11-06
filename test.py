import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import datetime


def conect_db():
    try:
        connection = mysql.connector.connect(host='localhost',
                                            database='vinkosdb',
                                            user='root',
                                            password='Medeli1997')
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            return connection


    except Error as e:
        print("Error while connecting to MySQL", e)


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

def insert_errors(conn,df):
    for _ , row in df.iterrows():
        query = ("INSERT INTO vinkosdb.errores (nameRegister, messageError)"
        "VALUES (%s, %s)")
        data = (row['Archivo'], row['Error'])
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
    conn.close()