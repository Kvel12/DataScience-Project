import pandas as pd
from sqlalchemy.engine import Engine

def extract_dim_cliente(con: Engine) -> pd.DataFrame:
    """
    Extrae los datos de cliente junto con su tipo y ciudad de la base de datos fuente
    Args:
        con: Conexión a la base de datos fuente
    Returns:
        pd.DataFrame: DataFrame con los datos de cliente
    """
    query = """
    SELECT 
        c.cliente_id,
        c.nombre,
        c.nit_cliente,
        tc.nombre as tipo_cliente,
        c.sector,
        c.email,
        c.telefono,
        c.direccion,
        c.nombre_contacto,
        ci.nombre as ciudad
    FROM cliente c
    LEFT JOIN tipo_cliente tc ON c.tipo_cliente_id = tc.tipo_cliente_id
    LEFT JOIN ciudad ci ON c.ciudad_id = ci.ciudad_id
    """
    return pd.read_sql(query, con)


def extract_dim_mensajero(con: Engine) -> pd.DataFrame:
    """
    Extrae los datos de mensajero junto con su ciudad de operación
    Args:
        con: Conexión a la base de datos fuente
    Returns:
        pd.DataFrame: DataFrame con los datos de mensajero
    """
    query = """
    SELECT 
        m.id as mensajero_id,
        m.fecha_entrada,
        m.fecha_salida,
        c.nombre as ciudad_operacion,
        m.activo
    FROM clientes_mensajeroaquitoy m
    LEFT JOIN ciudad c ON m.ciudad_operacion_id = c.ciudad_id
    """
    return pd.read_sql(query, con)


def extract_dim_sede(con: Engine) -> pd.DataFrame:
    """
    Extrae los datos de sedes junto con su información de ubicación
    Args:
        con: Conexión a la base de datos fuente
    Returns:
        pd.DataFrame: DataFrame con los datos de sedes
    """
    query = """
    SELECT 
        sede.sede_id,
        sede.nombre AS nombre_sede,
        sede.direccion AS direccion_sede,
        ciudad.nombre AS ciudad_sede,
        departamento.nombre AS departamento_sede
    FROM sede 
    JOIN ciudad ON sede.ciudad_id = ciudad.ciudad_id
    JOIN departamento ON ciudad.departamento_id = departamento.departamento_id
    """
    return pd.read_sql(query, con)


def extract_dim_novedad(con: Engine) -> pd.DataFrame:
    """
    Extrae los datos de novedades del servicio
    Args:
        con: Conexión a la base de datos fuente
    Returns:
        pd.DataFrame: DataFrame con los datos de novedades
    """
    query = """
    SELECT 
        id as novedad_id,
        tipo_novedad_id,
        descripcion 
    FROM 
        mensajeria_novedadesservicio 
    """
    return pd.read_sql(query, con)

def extract_dim_estado(con: Engine) -> pd.DataFrame:
    """
    Extrae los datos de estados del servicio
    Args:
        con: Conexión a la base de datos fuente
    Returns:
        pd.DataFrame: DataFrame con los datos de estados
    """
    query = """
    SELECT 
        id as estado_id,
        nombre as nombre_estado,
        descripcion
    FROM mensajeria_estado
    """
    return pd.read_sql(query, con)


def extract_hecho_acumulado(source_engine: Engine, target_engine: Engine) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Extrae los datos necesarios para el hecho acumulado
    Args:
        source_engine: Conexión a la base de datos fuente
        target_engine: Conexión a la base de datos bodega
    Returns:
        tuple: (df_servicios, dim_fecha, dim_cliente, dim_mensajero, dim_hora)
    """
    # Consulta principal desde la fuente
    query = """
    SELECT 
        s.id as servicio_id,
        s.cliente_id,
        s.mensajero_id as mensajero_inicial_id,
        es.estado_id,
        es.fecha as fecha_estado,
        es.hora as hora_estado
    FROM mensajeria_servicio s
    JOIN mensajeria_estadosservicio es ON s.id = es.servicio_id
    ORDER BY s.id, es.fecha, es.hora
    """
    
    # Extraer datos principales de la fuente
    df_servicios = pd.read_sql(query, source_engine)
    
    # Extraer dimensiones de la bodega
    dim_fecha = pd.read_sql_table('dim_fecha', target_engine)
    dim_cliente = pd.read_sql_table('dim_cliente', target_engine)
    dim_mensajero = pd.read_sql_table('dim_mensajero', target_engine)
    dim_hora = pd.read_sql_table('dim_hora', target_engine)
    
    return df_servicios, dim_fecha, dim_cliente, dim_mensajero, dim_hora


def extract_hecho_servicio_hora(con: Engine) -> pd.DataFrame:
    """
    Extrae los datos para el hecho de servicios por hora desde el hecho acumulado
    Args:
        con: Conexión a la base de datos bodega
    Returns:
        pd.DataFrame: DataFrame con los datos de servicios por hora
    """
    query = """
    SELECT 
        ha.servicio_id,
        ha.key_dim_fecha,
        ha.key_dim_cliente,
        ha.key_dim_mensajero,
        ha.key_dim_hora,
        ds.key_dim_sede,
        EXTRACT(HOUR FROM ha.hora_iniciado::time) as hora_servicio
    FROM hecho_entrega_acumulado ha
    JOIN dim_cliente dc ON ha.key_dim_cliente = dc.key_dim_cliente
    JOIN dim_sede ds ON dc.cliente_id = ds.sede_id
    WHERE ha.hora_iniciado IS NOT NULL
    """
    return pd.read_sql(query, con)


def extract_hecho_servicio_diaria(con: Engine) -> pd.DataFrame:
    """
    Extrae los datos para el hecho de servicios por día desde el hecho acumulado
    Args:
        con: Conexión a la base de datos bodega
    Returns:
        pd.DataFrame: DataFrame con los datos de servicios por día
    """
    query = """
    SELECT 
        ha.servicio_id,
        ha.key_dim_fecha,
        ha.key_dim_cliente,
        ha.key_dim_mensajero,
        ds.key_dim_sede,
        df.dia_semana
    FROM hecho_entrega_acumulado ha
    JOIN dim_cliente dc ON ha.key_dim_cliente = dc.key_dim_cliente
    JOIN dim_sede ds ON dc.cliente_id = ds.sede_id
    JOIN dim_fecha df ON ha.key_dim_fecha = df.key_dim_fecha
    WHERE ha.fecha_iniciado IS NOT NULL
    """
    return pd.read_sql(query, con)


def extract_hecho_novedades(con_fuente: Engine, con_bodega: Engine) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Extrae los datos necesarios para el hecho de novedades
    Args:
        con_fuente: Conexión a la base de datos fuente
        con_bodega: Conexión a la base de datos bodega
    Returns:
        tuple: (df_novedades, dim_fecha, dim_cliente, dim_novedad)
    """
    # Consulta para datos de la fuente
    query = """
    SELECT 
        n.id AS novedad_id,
        n.fecha_novedad AS fecha_hora_novedad,
        s.cliente_id,
        n.mensajero_id,
        n.tipo_novedad_id,
        n.descripcion 
    FROM 
        mensajeria_novedadesservicio n
        JOIN mensajeria_servicio s ON n.id = s.cliente_id
    """
    
    # Extraer datos de la fuente y dimensiones de la bodega
    df_novedades = pd.read_sql(query, con_fuente)
    dim_fecha = pd.read_sql_table('dim_fecha', con_bodega)
    dim_cliente = pd.read_sql_table('dim_cliente', con_bodega)
    dim_novedad = pd.read_sql_table('dim_novedad', con_bodega)
    
    return df_novedades, dim_fecha, dim_cliente, dim_novedad