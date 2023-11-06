import pandas as pd
from datetime import datetime
from test import conect_db
from email_validator import validate_email, EmailNotValidError

def validate_email_column(email):
    try:
        validate_email(email)
        return True
    except EmailNotValidError as e:
        return False

def process_validate_visitante(files : list ):
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

        final_result['email_valid'] = final_result['email'].apply(validate_email_column)
        final_result['fechaPrimeraVisita'] = final_result.groupby('email')['fechaPrimeraVisita'].transform('min')
        final_result['fechaUltimaVisita'] = final_result.groupby('email')['fechaUltimaVisita'].transform('max')
        final_result.drop_duplicates(subset='email', keep='last', inplace=True)

        filter_by_email_valid = final_result[final_result['email_valid'] == True]



    return filter_by_email_valid #visitas 2


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

files_list = ['report_7.txt']
conn = conect_db()
df = process_validate_visitante(files=files_list)
#print(df[df['email'] == 'alexxa_1203@yahoo.com'])
insert_to_visitantes(conn=conn, df=df)