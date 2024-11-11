import datetime
from datetime import date

import numpy as np
import pandas as pd
from pandas import DataFrame

def transform_dim_fecha() -> pd.DataFrame:
    """
    Genera la dimensión fecha para el período especificado
    Returns:
        pd.DataFrame: Dimensión fecha con todos sus atributos
    """
    # Generar fechas
    fechas = pd.date_range(start='2023-01-01', end='2024-12-31', freq='D')
    dim_fecha = pd.DataFrame({'fecha': fechas})
    
    # Convertir a timestamp sin hora (solo fecha)
    dim_fecha['fecha'] = dim_fecha['fecha'].dt.strftime('%Y-%m-%d')
    dim_fecha['fecha'] = pd.to_datetime(dim_fecha['fecha'])
    
    # Extraer componentes de la fecha
    dim_fecha['año'] = dim_fecha['fecha'].dt.year
    dim_fecha['mes'] = dim_fecha['fecha'].dt.month
    dim_fecha['dia'] = dim_fecha['fecha'].dt.day
    dim_fecha['dia_semana'] = dim_fecha['fecha'].dt.dayofweek  # 0 = Lunes, 6 = Domingo
    
    # Agregar fecha de carga
    dim_fecha['saved'] = date.today()
    
    return dim_fecha


def get_periodo_dia(hora: int) -> str:
    """
    Determina el periodo del día según la hora
    Args:
        hora: Hora del día (0-23)
    Returns:
        str: Periodo del día (Madrugada, Mañana, Tarde, Noche)
    """
    if 0 <= hora < 6:
        return 'Madrugada'
    elif 6 <= hora < 12:
        return 'Mañana'
    elif 12 <= hora < 18:
        return 'Tarde'
    else:
        return 'Noche'

def transform_dim_hora() -> pd.DataFrame:
    """
    Genera la dimensión hora con las 24 horas del día y sus periodos
    Returns:
        pd.DataFrame: Dimensión hora con todos sus atributos
    """
    horas = []
    for hora in range(24):
        horas.append({
            'hora': hora,
            'periodo_dia': get_periodo_dia(hora)
        })
        
    # Crear DataFrame
    dim_hora = pd.DataFrame(horas)
    
    # Agregar fecha de carga
    dim_hora['saved'] = date.today()
    
    return dim_hora


def transform_dim_cliente(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma los datos de la dimensión cliente
    Args:
        df: DataFrame con los datos extraídos de cliente
    Returns:
        pd.DataFrame: DataFrame transformado
    """
    # Por ahora solo agregamos la fecha de carga
    # Aquí podrías agregar más transformaciones si son necesarias en el futuro
    df['saved'] = date.today()
    return df


def transform_dim_mensajero(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma los datos de la dimensión mensajero
    Args:
        df: DataFrame con los datos extraídos de mensajero
    Returns:
        pd.DataFrame: DataFrame transformado
    """
    # Reemplazar valores nulos
    df = df.replace({
        None: 'No especificado', 
        '': 'No especificado',
        pd.NaT: None  # Para fechas nulas
    })
    
    # Asegurarse de que las fechas estén en el formato correcto
    df['fecha_entrada'] = pd.to_datetime(df['fecha_entrada']).dt.date
    df['fecha_salida'] = pd.to_datetime(df['fecha_salida']).dt.date
    
    # Agregar fecha de carga
    df['saved'] = date.today()
    
    return df

def transform_dim_sede(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma los datos de la dimensión sede
    Args:
        df: DataFrame con los datos extraídos de sede
    Returns:
        pd.DataFrame: DataFrame transformado
    """
    # Agregar fecha de carga
    df['saved'] = date.today()
    return df


def transform_dim_novedad(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma los datos de la dimensión novedad
    Args:
        df: DataFrame con los datos extraídos de novedades
    Returns:
        pd.DataFrame: DataFrame transformado
    """
    # Agregar fecha de carga
    df['saved'] = date.today()
    return df

def transform_dim_estado(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma los datos de la dimensión estado
    Args:
        df: DataFrame con los datos extraídos de estados
    Returns:
        pd.DataFrame: DataFrame transformado
    """
    # Agregar fecha de carga
    df['saved'] = date.today()
    return df

def calcular_tiempo_entre_estados(fecha1, hora1, fecha2, hora2):
    """
    Calcula el tiempo transcurrido entre dos estados
    """
    try:
        if pd.isna(fecha1) or pd.isna(fecha2) or pd.isna(hora1) or pd.isna(hora2):
            return "00:00:00"
        
        timestamp1 = pd.to_datetime(f"{fecha1} {hora1}")
        timestamp2 = pd.to_datetime(f"{fecha2} {hora2}")
        
        if timestamp2 < timestamp1:
            timestamp1, timestamp2 = timestamp2, timestamp1
            
        segundos = (timestamp2 - timestamp1).total_seconds()
        hours = int(segundos // 3600)
        minutes = int((segundos % 3600) // 60)
        seconds = int(segundos % 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    except:
        return "00:00:00"

def transform_hecho_acumulado(df: pd.DataFrame, dim_fecha: pd.DataFrame, 
                            dim_cliente: pd.DataFrame, dim_mensajero: pd.DataFrame,
                            dim_hora: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma los datos para crear el hecho acumulado
    """
    # Limpiar formato de hora
    df['hora_estado'] = df['hora_estado'].apply(lambda x: str(x).split('.')[0] if '.' in str(x) else str(x))
    
    # Procesar estado 3 (novedades)
    estado_3 = df[df['estado_id'] == 3].groupby('servicio_id').agg({
        'fecha_estado': list,
        'hora_estado': list,
        'cliente_id': 'first',
        'mensajero_inicial_id': 'first'
    }).reset_index()
    
    # Crear DataFrame de novedades
    df_novedades = pd.DataFrame({'servicio_id': df['servicio_id'].unique()})
    df_novedades['tiene_novedad'] = df_novedades['servicio_id'].isin(estado_3['servicio_id'])
    df_novedades['cantidad_novedades'] = 0
    
    # Actualizar novedades
    servicios_con_novedades = estado_3.copy()
    servicios_con_novedades['cantidad_novedades'] = servicios_con_novedades['fecha_estado'].str.len()
    df_novedades.update(
        servicios_con_novedades[['servicio_id', 'cantidad_novedades']].set_index('servicio_id')
    )
    
    # Procesar todos los estados
    df_estados = pd.DataFrame()
    for estado in [1, 2, 3, 4, 5, 6]:
        if estado == 3:
            estado_data = estado_3.copy()
            estado_data['fecha_primera_novedad'] = estado_data['fecha_estado'].apply(lambda x: x[0] if x else None)
            estado_data['hora_primera_novedad'] = estado_data['hora_estado'].apply(lambda x: x[0] if x else None)
            estado_data['fecha_ultima_novedad'] = estado_data['fecha_estado'].apply(lambda x: x[-1] if x else None)
            estado_data['hora_ultima_novedad'] = estado_data['hora_estado'].apply(lambda x: x[-1] if x else None)
        else:
            estado_data = df[df['estado_id'] == estado].groupby('servicio_id').agg({
                'fecha_estado': 'first',
                'hora_estado': 'first',
                'cliente_id': 'first',
                'mensajero_inicial_id': 'first'
            }).reset_index()
        
        nombre_estado = {1: 'iniciado', 2: 'asignado', 3: 'novedad',
                        4: 'recogido', 5: 'entregado', 6: 'cerrado'}[estado]
        
        if estado == 3:
            columnas_rename = {
                'fecha_primera_novedad': f'fecha_{nombre_estado}',
                'hora_primera_novedad': f'hora_{nombre_estado}',
                'fecha_ultima_novedad': f'fecha_ultima_{nombre_estado}',
                'hora_ultima_novedad': f'hora_ultima_{nombre_estado}'
            }
        else:
            columnas_rename = {
                'fecha_estado': f'fecha_{nombre_estado}',
                'hora_estado': f'hora_{nombre_estado}'
            }
        
        estado_data = estado_data.rename(columns=columnas_rename)
        
        if len(df_estados) == 0:
            df_estados = estado_data
            df_estados = df_estados.merge(df_novedades[['servicio_id', 'cantidad_novedades']], 
                                        on='servicio_id', how='left')
        else:
            df_estados = df_estados.merge(estado_data, 
                                        on=['servicio_id', 'cliente_id', 'mensajero_inicial_id'],
                                        how='outer')
    
    # Calcular tiempos entre estados
    df_estados['tiempo_asignacion'] = df_estados.apply(
        lambda row: calcular_tiempo_entre_estados(
            row['fecha_iniciado'], row['hora_iniciado'],
            row['fecha_asignado'], row['hora_asignado']
        ), axis=1
    )
    
    df_estados['tiempo_total_novedades'] = df_estados.apply(
        lambda row: calcular_tiempo_entre_estados(
            row['fecha_novedad'], row['hora_novedad'],
            row['fecha_ultima_novedad'], row['hora_ultima_novedad']
        ) if pd.notna(row['fecha_novedad']) else "00:00:00",
        axis=1
    )
    
    df_estados['tiempo_recogida'] = df_estados.apply(
        lambda row: calcular_tiempo_entre_estados(
            row['fecha_asignado'], row['hora_asignado'],
            row['fecha_recogido'], row['hora_recogido']
        ) if pd.isna(row.get('fecha_novedad')) else
        calcular_tiempo_entre_estados(
            row['fecha_ultima_novedad'], row['hora_ultima_novedad'],
            row['fecha_recogido'], row['hora_recogido']
        ), axis=1
    )
    
    df_estados['tiempo_entrega'] = df_estados.apply(
        lambda row: calcular_tiempo_entre_estados(
            row['fecha_recogido'], row['hora_recogido'],
            row['fecha_entregado'], row['hora_entregado']
        ), axis=1
    )
    
    df_estados['tiempo_cierre'] = df_estados.apply(
        lambda row: calcular_tiempo_entre_estados(
            row['fecha_entregado'], row['hora_entregado'],
            row['fecha_cerrado'], row['hora_cerrado']
        ), axis=1
    )
    
    # Preparar para merge con dimensiones
    df_estados['fecha_iniciado'] = pd.to_datetime(df_estados['fecha_iniciado']).dt.date
    dim_fecha['fecha'] = pd.to_datetime(dim_fecha['fecha']).dt.date
    df_estados['hora_del_dia'] = df_estados['hora_iniciado'].apply(
        lambda x: int(x.split(':')[0]) if pd.notna(x) else None
    )
    
    # Realizar merges con dimensiones
    hecho_acumulado = df_estados.merge(
        dim_fecha[['key_dim_fecha', 'fecha']], 
        left_on='fecha_iniciado', 
        right_on='fecha',
        how='left'
    ).merge(
        dim_cliente[['key_dim_cliente', 'cliente_id']], 
        on='cliente_id',
        how='left'
    ).merge(
        dim_mensajero[['key_dim_mensajero', 'mensajero_id']], 
        left_on='mensajero_inicial_id',
        right_on='mensajero_id',
        how='left'
    ).merge(
        dim_hora[['key_dim_hora', 'hora']], 
        left_on='hora_del_dia',
        right_on='hora',
        how='left'
    )

    # Seleccionar y ordenar columnas finales
    columnas_finales = [
        'servicio_id',
        'key_dim_fecha',
        'key_dim_cliente',
        'key_dim_mensajero',
        'key_dim_hora',
        'fecha_iniciado',
        'hora_iniciado',
        'fecha_asignado',
        'hora_asignado',
        'fecha_novedad',
        'hora_novedad',
        'fecha_ultima_novedad',
        'hora_ultima_novedad',
        'fecha_recogido',
        'hora_recogido',
        'fecha_entregado',
        'hora_entregado',
        'fecha_cerrado',
        'hora_cerrado',
        'tiempo_asignacion',
        'tiempo_total_novedades',
        'tiempo_recogida',
        'tiempo_entrega',
        'tiempo_cierre',
        'cantidad_novedades'
    ]
    
    # Agregar fecha de carga
    hecho_acumulado['saved'] = date.today()
    
    # Asegurarse que todas las columnas necesarias existen
    for col in columnas_finales:
        if col not in hecho_acumulado.columns:
            hecho_acumulado[col] = None
            
    # Retornar solo las columnas necesarias en el orden correcto
    columnas_finales.append('saved')
    return hecho_acumulado[columnas_finales]


def transform_hecho_servicio_hora(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma los datos para el hecho de servicios por hora
    Args:
        df: DataFrame con los datos extraídos
    Returns:
        pd.DataFrame: DataFrame transformado con la agregación por hora
    """
    # Agrupar por las dimensiones relevantes
    hecho_hora = df.groupby([
        'key_dim_fecha',
        'key_dim_cliente',
        'key_dim_sede',
        'key_dim_mensajero',
        'key_dim_hora',
        'servicio_id'
    ]).size().reset_index(name='cantidad_servicios')
    
    # Agregar fecha de carga
    hecho_hora['saved'] = date.today()
    
    # Asegurar que todos los campos numéricos sean del tipo correcto
    numeric_columns = ['key_dim_fecha', 'key_dim_cliente', 'key_dim_sede', 
                      'key_dim_mensajero', 'key_dim_hora', 'servicio_id', 
                      'cantidad_servicios']
    
    for col in numeric_columns:
        if col in hecho_hora.columns:
            hecho_hora[col] = pd.to_numeric(hecho_hora[col], errors='coerce')
    
    return hecho_hora


def transform_hecho_servicio_diaria(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma los datos para el hecho de servicios por día
    Args:
        df: DataFrame con los datos extraídos
    Returns:
        pd.DataFrame: DataFrame transformado con la agregación por día
    """
    # Agrupar por las dimensiones relevantes
    hecho_dia = df.groupby([
        'key_dim_fecha',
        'key_dim_cliente',
        'key_dim_sede',
        'key_dim_mensajero',
        'servicio_id'
    ]).size().reset_index(name='cantidad_servicios_dia')
    
    # Agregar fecha de carga
    hecho_dia['saved'] = date.today()
    
    # Asegurar que todos los campos numéricos sean del tipo correcto
    numeric_columns = ['key_dim_fecha', 'key_dim_cliente', 'key_dim_sede', 
                      'key_dim_mensajero', 'servicio_id', 'cantidad_servicios_dia']
    
    for col in numeric_columns:
        if col in hecho_dia.columns:
            hecho_dia[col] = pd.to_numeric(hecho_dia[col], errors='coerce')
    
    return hecho_dia


def transform_hecho_novedades(df: pd.DataFrame, dim_fecha: pd.DataFrame, 
                            dim_cliente: pd.DataFrame, dim_novedad: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma los datos para el hecho de novedades
    Args:
        df: DataFrame con los datos de novedades
        dim_fecha: Dimensión fecha
        dim_cliente: Dimensión cliente
        dim_novedad: Dimensión novedad
    Returns:
        pd.DataFrame: DataFrame transformado con las novedades
    """
    # Convertir fecha_novedad a datetime y extraer componentes
    df['fecha_hora_novedad'] = pd.to_datetime(df['fecha_hora_novedad']).dt.date
    dim_fecha['fecha'] = pd.to_datetime(dim_fecha['fecha']).dt.date
    
    # Realizar los merges con las dimensiones
    hecho_novedades = df.merge(
        dim_fecha[['key_dim_fecha', 'fecha']], 
        left_on='fecha_hora_novedad', 
        right_on='fecha',
        how='left'
    ).merge(
        dim_cliente[['key_dim_cliente', 'cliente_id']], 
        on='cliente_id',
        how='left'
    ).merge(
        dim_novedad[['key_dim_novedad', 'novedad_id']], 
        on='novedad_id',
        how='left'
    )
    
    # Seleccionar columnas finales
    columnas_finales = [
        'key_dim_fecha',
        'key_dim_cliente',
        'key_dim_novedad',
        'fecha_hora_novedad',
        'descripcion'
    ]
    
    hecho_novedades = hecho_novedades[columnas_finales]
    
    # Agregar fecha de carga
    hecho_novedades['saved'] = date.today()
    
    return hecho_novedades