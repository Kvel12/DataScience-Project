# CS_etl_py
Python etl for a health care database 
## Requirements installation 
 **if not exists environment create one**
```
python3 -m venv my_env
source my_env/bin/activate  
```
your terminal should look like
```
(my_env) $
```
here you can install the packages by doing 
```
pip install -r requirements.txt
```
estructura del config.yml 
```
nombre_conexion:
  drivername: postgresql  
  user: postgres
  password : valor_privado
  port: 5432 # pordefecto 
  host: localhost
  dbname: colombia_saludable #nombre de la base de datos
```


# Guía para ejecutar el ETL

Este proyecto realiza un proceso ETL (Extract, Transform, Load) para extraer datos desde una base de datos fuente, transformarlos y cargarlos en una base de datos de destino (Warehouse). A continuación, se detallan los pasos necesarios para ejecutar el proyecto correctamente.

## Pasos para ejecutar el ETL

### 1. Crear un entorno virtual

Para asegurarte de que las dependencias del proyecto no interfieran con otros proyectos, se recomienda crear un entorno virtual de Python, si ya no se cuenta con uno.

#### En Windows:

1. Abre una terminal o línea de comandos.
2. Ejecuta el siguiente comando para crear un entorno virtual:
    ```bash
    python -m venv my_env
    ```
3. Activa el entorno virtual usando:
    ```bash
    my_env\Scripts\activate
    ```
4. Tu terminal debería verse similar a esto:

    ```bash
    (my_env) $
    ```

#### En Linux/MacOS:

1. Abre una terminal.
2. Ejecuta el siguiente comando para crear un entorno virtual:
    ```bash
    python3 -m venv my_env
    ```
3. Activa el entorno virtual:
    ```bash
    source my_env/bin/activate
    ```
4. Tu terminal debería verse similar a esto:

    ```bash
    (my_env) $
    ```

### 2. Instalar las dependencias

Una vez que el entorno virtual esté activado, deberás instalar todas las dependencias necesarias para ejecutar el proyecto. Las dependencias están listadas en el archivo `requirements.txt`.

1. Con el entorno virtual activado, instala las dependencias con el siguiente comando:

    ```bash
    pip install -r requirements.txt
    ```


### 3. Configurar el archivo `config.yml`

Despues de todo esto, se debe de configurar el archivo `config.yml`. Este archivo contiene las credenciales y configuraciones necesarias para conectar las bases de datos de origen (fuente) y destino (bodega). 

1. Abre el archivo `config.yml`
2. Configura las credenciales de las bases de datos de la siguiente manera:

    ```yaml
    fuente:
        drivername: <postgresql>
        user: <tu_usuario>
        password: <tu_contraseña>
        port: <puerto_fuente>
        host: <host_servidor_fuente>
        dbname: <nombre_base_datos_fuente>
    
    bodega:
        drivername: <postgresql>
        user: <tu_usuario>
        password: <tu_contraseña>
        port: <puerto_bodega>
        host: <host_servidor_bodega>
        dbname: <nombre_base_datos_bodega>
    ```

   - **fuente**: Aqui estarán las credenciales de la base de datos de origen.
   - **bodega**: Aqui estarán las credenciales de la base de datos de destino donde se cargarán los datos transformados.

### 4. Ejecutar el proceso ETL

Una vez que hayas configurado las bases de datos y las credenciales, ya puedes ejecutar el script principal para ejecutar el proceso ETL.

1. En la terminal, con el entorno virtual activado, ejecuta el siguiente comando para correr el script principal (`main.py`):

    ```bash
    python main.py
    ```

2. El proceso comenzará a ejecutarse y, dependiendo del volumen de datos, puede tardar mas o menos tiempo. Una vez completado, los datos transformados estarán disponibles en la base de datos de destino.

### 5. Verificación

Una vez que el proceso ETL se haya ejecutado correctamente, verifica que los datos se hayan cargado correctamente en la base de datos de destino (bodega).
---
Para comprobar que la bodega de datos está funcionando correctamente, puedes ejecutar las consultas SQL que se encuentran al final de cada uno de los notebooks correspondientes a las tablas de hechos.
   
1. Estas consultas SQL deben ejecutarse en el entorno donde se esté corriendo la bodega (es decir, en la base de datos de destino). Puedes hacerlo a través de un cliente SQL, como `pgAdmin`, `DBeaver`, o directamente desde un terminal si estás usando PostgreSQL.
   
Con los resultados arrojados por la sconsultas, se puede verificar si la bodega responde correctamente a las preguntas del proyecto.

---

## Notas adicionales

- Si tienes problemas de conexión con las bases de datos, asegúrate de que las credenciales en `config.yml` sean correctas.
- Si el entorno virtual no se activa correctamente, asegúrate de que Python esté correctamente instalado en tu sistema.
- Este proyecto requiere que tengas instaladas las librerías necesarias como `pandas`, `sqlalchemy`, `psycopg2`, entre otras, que están listadas en el archivo `requirements.txt`.

---
