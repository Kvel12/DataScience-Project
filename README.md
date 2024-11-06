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