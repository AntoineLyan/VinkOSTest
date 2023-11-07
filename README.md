
# Prueba de VinkOS (Desarrollo)

Se realizara el reporte del flujo através del readme. Se considararon los puntos que se compartieron en la carpeta y por email. 


## Validaciones
Las validaciones a seguir fueron las siguientes:
- Validacion del layout
    - Numero de columnas igual a 15.
    - Nombre de las columnas
    - Orden de las columnas
    - Tipo de dato

- Validar el formato fecha dd/mm/yyyy hh:mm
- Validar el email sea correcto (email_validator)
- Limpieza de datos nulos
## Flujo del proceso

    1. Conexion al servidor remoto por SFTP.
    2. Lectura, extracción de txt y eliminación de archivos.
    3. Creacion del backup en un zip en carpeta "visitas/bckp".
    4. Comprobación del layout.
    5. Ingesta de errores encontrados.
    6. Validaciones de datos, ingesta de datos en estadistica. 
    7. Flujo de ingesta en visitantes (incluye comprobación).
    8. Ingesta de datos en tabla bitacora. 
## Deployment

Para correr el programa se debe tener instalado python 3.8 o 3.9, se debe tomar en cuenta crear un entorno virtual con virtualenv o pyenv, en mi caso se utilizo virtualenv. 

Línea de comando para env:

```bash
  python -m venv .env
```

Para instalar los requerimientos se utiliza lo siguiente:
```bash
   pip install -r requirements.txt
```
Tambien se debe crear un archivo de variables de entorno para contraseñas y usuarios aunque sea para un poco de seguridad. Las mías las coloque en el gitingore por lo tanto no se veran. También utilice docker para crear un contenedor que simular el servidor del cliente el cual le inserte en un sistema ubuntu los archivos txt dados. 

Si se tiene todo el ambiente configurado se debe correr el siguiente comando: 

```bash
   python main.py
```
## Ambiente Productivo

En caso de necesitar que corra de manera automatica cada día se puede colocar en la computadora dentro de un procesador de tareas en windows. 

Las tablas generadas se pueden procesar en powerBI o Tableu para tener un tablero con la información de una manera mas entendible que se actualice diarío para que así el área de soporte pueda estar al pendiente de cualquier fallo o problema que se necesite resolver. 

Es importante recalcar que en la tabla de errores, se veran cualquier problema que se tengan de los datos. Lo ideal sería crear este proceso en alguna nube para que se puedan observar problemas del proceso de código para que si ocurre algo, se levante un email a la persona encargada de operaciones. 


## Big data Impala

Los principales cambios en el flujo del proceso son los siguientes:

- El código Python debe usar la biblioteca PyImpala en lugar de la biblioteca MySQL-Connector.
- El código Python debe formatear los datos para que coincidan con el esquema de las tablas de Impala.

Los esquemas es lo mas importante en cambios por ejemplo esta es una tabla de lo que puede cambiar:


| MySql Types | Impala Types    | 
| :-------- | :------- | 
| `int` | `int64` |
| `varchar` | `string` |
| `float` | `double` |
| `bit` | `boolean` |

