import pandas as pd
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy import inspect
import yaml
from etl import extract, transform, load
import psycopg2
import time

def create_connections(config):
    """
    Crea las conexiones a las bases de datos fuente y destino
    """
    source_engine = create_engine(
        f"postgresql://{config['fuente']['user']}:{config['fuente']['password']}@"
        f"{config['fuente']['host']}:{config['fuente']['port']}/{config['fuente']['dbname']}"
    )
    
    target_engine = create_engine(
        f"postgresql://{config['bodega']['user']}:{config['bodega']['password']}@"
        f"{config['bodega']['host']}:{config['bodega']['port']}/{config['bodega']['dbname']}"
    )
    
    return source_engine, target_engine

def drop_all_tables(config):
    """
    Elimina todas las tablas existentes para empezar desde cero
    """
    print("Eliminando todas las tablas existentes...")
    conn = psycopg2.connect(
        dbname=config['bodega']['dbname'],
        user=config['bodega']['user'],
        password=config['bodega']['password'],
        host=config['bodega']['host'],
        port=config['bodega']['port']
    )
    
    # Primero eliminar los hechos (que tienen foreign keys)
    drop_facts = """
    DROP TABLE IF EXISTS hecho_novedades_servicio CASCADE;
    DROP TABLE IF EXISTS hecho_entrega_servicio_diaria CASCADE;
    DROP TABLE IF EXISTS hecho_entrega_servicio_hora CASCADE;
    DROP TABLE IF EXISTS hecho_entrega_acumulado CASCADE;
    """
    
    # Luego eliminar las dimensiones
    drop_dims = """
    DROP TABLE IF EXISTS dim_fecha CASCADE;
    DROP TABLE IF EXISTS dim_hora CASCADE;
    DROP TABLE IF EXISTS dim_cliente CASCADE;
    DROP TABLE IF EXISTS dim_mensajero CASCADE;
    DROP TABLE IF EXISTS dim_sede CASCADE;
    DROP TABLE IF EXISTS dim_novedad CASCADE;
    DROP TABLE IF EXISTS dim_estado CASCADE;
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(drop_facts)
            cur.execute(drop_dims)
        conn.commit()
        print("Todas las tablas fueron eliminadas exitosamente")
    except Exception as e:
        print(f"Error eliminando tablas: {e}")
        conn.rollback()
    finally:
        conn.close()

def create_tables(config):
    """
    Crea las tablas en la base de datos bodega usando sqlscripts.yml
    """
    print("Creando tablas en la base de datos...")
    conn = psycopg2.connect(
        dbname=config['bodega']['dbname'],
        user=config['bodega']['user'],
        password=config['bodega']['password'],
        host=config['bodega']['host'],
        port=config['bodega']['port']
    )

    with open('sqlscripts.yml', 'r', encoding='utf-8') as f:
        scripts = yaml.safe_load(f)

    try:
        with conn.cursor() as cur:
            for table_name, script in scripts.items():
                print(f"Creando tabla {table_name}...")
                cur.execute(script)
            conn.commit()
            print("Todas las tablas fueron creadas exitosamente")
    except Exception as e:
        print(f"Error creando tablas: {e}")
        conn.rollback()
    finally:
        conn.close()

def verify_tables_exist(engine, required_tables):
    """
    Verifica que todas las tablas necesarias existan
    """
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    missing_tables = [table for table in required_tables if table not in existing_tables]
    
    if missing_tables:
        print(f"Tablas faltantes: {missing_tables}")
        return False
    return True

def process_dimensions(source_engine, target_engine):
    """
    Procesa las dimensiones del proceso ETL
    Proceso de Carga (Load)
    """
    print("\nProcesando dimensiones...")
    
    try:
        # Procesar dimensión fecha
        print("Procesando dimensión fecha...")
        dim_fecha = transform.transform_dim_fecha()
        load.load(dim_fecha, target_engine, 'dim_fecha', True)
        print("dim_fecha procesada exitosamente")
        time.sleep(2)

        # Procesar dimensión hora
        print("Procesando dimensión hora...")
        dim_hora = transform.transform_dim_hora()
        load.load(dim_hora, target_engine, 'dim_hora', True)
        print("dim_hora procesada exitosamente")
        time.sleep(2)

        # Procesar dimensión cliente
        print("Procesando dimensión cliente...")
        dim_cliente = extract.extract_dim_cliente(source_engine)
        dim_cliente = transform.transform_dim_cliente(dim_cliente)
        load.load(dim_cliente, target_engine, 'dim_cliente', True)
        print("dim_cliente procesada exitosamente")
        time.sleep(2)

        # Procesar dimensión mensajero
        print("Procesando dimensión mensajero...")
        dim_mensajero = extract.extract_dim_mensajero(source_engine)
        dim_mensajero = transform.transform_dim_mensajero(dim_mensajero)
        load.load(dim_mensajero, target_engine, 'dim_mensajero', True)
        print("dim_mensajero procesada exitosamente")
        time.sleep(2)

        # Procesar dimensión sede
        print("Procesando dimensión sede...")
        dim_sede = extract.extract_dim_sede(source_engine)
        dim_sede = transform.transform_dim_sede(dim_sede)
        load.load(dim_sede, target_engine, 'dim_sede', True)
        print("dim_sede procesada exitosamente")
        time.sleep(2)

        # Procesar dimensión novedad
        print("Procesando dimensión novedad...")
        dim_novedad = extract.extract_dim_novedad(source_engine)
        dim_novedad = transform.transform_dim_novedad(dim_novedad)
        load.load(dim_novedad, target_engine, 'dim_novedad', True)
        print("dim_novedad procesada exitosamente")
        time.sleep(2)

        # Procesar dimensión estado
        print("Procesando dimensión estado...")
        dim_estado = extract.extract_dim_estado(source_engine)
        dim_estado = transform.transform_dim_estado(dim_estado)
        load.load(dim_estado, target_engine, 'dim_estado', True)
        print("dim_estado procesada exitosamente")
        time.sleep(2)
        
        print("Todas las dimensiones fueron procesadas exitosamente")
        
    except Exception as e:
        print(f"Error procesando dimensiones: {e}")
        raise

def process_facts(source_engine, target_engine):
    """
    Procesa los hechos del proceso ETL
    """
    print("\nProcesando hechos...")
    
    # Verificar que todas las dimensiones necesarias existan
    required_tables = ['dim_fecha', 'dim_hora', 'dim_cliente', 'dim_mensajero', 
                      'dim_sede', 'dim_novedad', 'dim_estado']
    
    if not verify_tables_exist(target_engine, required_tables):
        raise Exception("Faltan tablas de dimensiones requeridas")

    try:
        # Procesar hecho acumulado
        print("Procesando hecho acumulado...")
        df_servicios, dim_fecha, dim_cliente, dim_mensajero, dim_hora = extract.extract_hecho_acumulado(source_engine, target_engine)
        hecho_acumulado = transform.transform_hecho_acumulado(df_servicios, dim_fecha, dim_cliente, dim_mensajero, dim_hora)
        load.load(hecho_acumulado, target_engine, 'hecho_entrega_acumulado', True)
        print("hecho_entrega_acumulado procesado exitosamente")
        time.sleep(2)

        # Procesar hecho servicio hora
        print("Procesando hecho servicio hora...")
        df_servicio_hora = extract.extract_hecho_servicio_hora(target_engine)
        hecho_hora = transform.transform_hecho_servicio_hora(df_servicio_hora)
        load.load(hecho_hora, target_engine, 'hecho_entrega_servicio_hora', True)
        print("hecho_entrega_servicio_hora procesado exitosamente")
        time.sleep(2)

        # Procesar hecho servicio diaria
        print("Procesando hecho servicio diaria...")
        df_servicio_dia = extract.extract_hecho_servicio_diaria(target_engine)
        hecho_dia = transform.transform_hecho_servicio_diaria(df_servicio_dia)
        load.load(hecho_dia, target_engine, 'hecho_entrega_servicio_diaria', True)
        print("hecho_entrega_servicio_diaria procesado exitosamente")
        time.sleep(2)

        print("Procesando hecho novedades...")
        df_novedades, dim_fecha, dim_cliente, dim_novedad = extract.extract_hecho_novedades(source_engine, target_engine)
        hecho_novedades = transform.transform_hecho_novedades(
            df_novedades,
            dim_fecha,
            dim_cliente,
            dim_novedad
        )
        load.load(hecho_novedades, target_engine, 'hecho_novedades_servicio', True)
        print("hecho_novedades_servicio procesado exitosamente")
        time.sleep(2)
        
        print("Todos los hechos fueron procesados exitosamente")
        
    except Exception as e:
        print(f"Error procesando hechos: {e}")
        raise

def main():
    # Cargar configuración
    with open('config.yml', 'r') as f:
        config = yaml.safe_load(f)

    # Crear conexiones
    source_engine, target_engine = create_connections(config)

    try:
        # Eliminar todas las tablas existentes
        drop_all_tables(config)
        
        # Crear todas las tablas desde cero
        create_tables(config)
        
        # Procesar dimensiones
        process_dimensions(source_engine, target_engine)
        
        # Procesar hechos
        process_facts(source_engine, target_engine)
        
        print("\n¡Proceso ETL completado exitosamente!")
        
    except Exception as e:
        print(f"\nError durante el proceso ETL: {e}")
        print("El proceso ETL falló")

if __name__ == "__main__":
    main()