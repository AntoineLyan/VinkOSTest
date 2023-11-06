import pysftp
import dotenv
import os
import pandas as pd
from email_validator import validate_email, EmailNotValidError

dotenv.load_dotenv('variables.env')
user = os.environ.get("user")
pwd = os.environ.get("pass")

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None  # Deshabilita la comprobación de clave de host

# with pysftp.Connection('localhost', username=user, password=pwd, port=3000, cnopts=cnopts) as sftp:

#     with sftp.cd('archivosVisitas'):           
#         # Obtén una lista de todos los archivos en el servidor FTP
#         files = sftp.listdir()
#         #print(files)

#         # Filtra la lista para que solo contenga archivos .txt que empiecen con "reporte"
#         report_files = [file for file in files if file.endswith(".txt") and file.startswith("report")]
#         #print(report_files)

#         for file in report_files:
#             sftp.get(file, "archivostxt/" + file)

#funcion de emails

def validate_email_column(email):
    try:
        validate_email(email)
        return 'Correcto'
    except EmailNotValidError as e:
        return str(e)

#Funcion para leer los archivos y comprobar layout
files = os.listdir("./archivostxt")
data = {}
type_columns = ["object", "float64", "object", "object", "object", "object"
                , "int64", "int64", "object", "int64", "int64", "object", "object", "object", "object" ]
for file in files:
    data[file] = pd.read_csv("archivostxt/" + file)

for file, df in data.items():
    # Validar la cantidad de columnas
    if len(df.columns) != 15:
        raise ValueError(f"El archivo {file} no tiene 14 columnas.")

    # Validar el tipo de dato de cada columna
    i=0
    for col_name, type_cols in zip(df.columns, df.dtypes):
        if type_cols != type_columns[i]:
            raise ValueError(f"El tipo de dato de la columna {col_name} no es válido del archivo: {file}.")
        i+=1
    
    # Validar el orden y nombres de las columnas
    expected_columns = ['email', 'jk', 'Badmail', 'Baja', 'Fecha envio', 'Fecha open', 'Opens',
                        'Opens virales', 'Fecha click', 'Clicks', 'Clicks virales', 'Links',
                        'IPs', 'Navegadores', 'Plataformas']
    #!Es jyv no jk
    if list(df.columns) != expected_columns:
        raise ValueError(f"El orden de las columnas en el archivo {file} no es el correcto.")
    
    #Validar email
    df['valid_email'] = df['email'].apply(validate_email_column)
    #invalid_emails = df[df['valid_email'] != 'Correcto']
    #print(invalid_emails.head(5))

    #Cambiando formato al requerido de las fechas
    df['Fecha envio'] = pd.to_datetime(df['Fecha envio'], format='%d/%m/%Y %H:%M')
    df['Fecha envio'] = df['Fecha envio'].dt.strftime('%d/%m/%Y %H:%M')
    print(df.head(5))