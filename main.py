import pysftp
import dotenv
import os

dotenv.load_dotenv('variables.env')
user = os.environ.get("user")
pwd = os.environ.get("pass")

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None  # Deshabilita la comprobación de clave de host

with pysftp.Connection('localhost', username=user, password=pwd, port=3000, cnopts=cnopts) as sftp:

    with sftp.cd('archivosVisitas'):           
        # Obtén una lista de todos los archivos en el servidor FTP
        files = sftp.listdir()
        #print(files)

        # Filtra la lista para que solo contenga archivos .txt que empiecen con "reporte"
        report_files = [file for file in files if file.endswith(".txt") and file.startswith("report")]
        #print(report_files)

        for file in report_files:
            sftp.get(file, "archivostxt/" + file)