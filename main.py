import pysftp
import dotenv
import os
import pandas as pd
from email_validator import validate_email, EmailNotValidError


def read_files_to_client():
    dotenv.load_dotenv('variables.env')
    user = os.environ.get("user")
    pwd = os.environ.get("pass")

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # Deshabilita la comprobación de clave de host, no debe hacerse en productivo. 

    with pysftp.Connection('localhost', username=user, password=pwd, port=3000, cnopts=cnopts) as sftp:

        with sftp.cd('archivosVisitas'):           
            # Obtén una lista de todos los archivos en el servidor FTP
            files = sftp.listdir()
            #print(files)

            # Filtra la lista para que solo contenga archivos .txt que empiecen con "report"
            report_files = [file for file in files if file.endswith(".txt") and file.startswith("report")]
            #print(report_files)

            for file in report_files:
                sftp.get(file, "archivostxt/" + file)

#funcion de emails
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

#Funcion para leer los archivos y comprobar layout
def check_layout():
    files = os.listdir("./archivostxt")
    data = {}
    error_data = []
    valid_data = []
    type_expect_cols = ["object", "float64", "object", "object", "object", "object"
                    , "int64", "int64", "object", "int64", "int64", "object", "object", "object", "object" ]
    for file in files:
        data[file] = pd.read_csv("archivostxt/" + file)

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
        expected_columns = ['email', 'jk', 'Badmail', 'Baja', 'Fecha envio', 'Fecha open', 'Opens',
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

        #Cambiando formato al requerido de las fechas
        # df['Fecha envio'] = pd.to_datetime(df['Fecha envio'], format='%d/%m/%Y %H:%M')
        # df['Fecha envio'] = df['Fecha envio'].dt.strftime('%d/%m/%Y %H:%M')
        # print(df.head(5))

    error_df = pd.DataFrame(error_data)


    return error_df, valid_data
        

errores, archivos_validos = check_layout()

print(errores['Error'])