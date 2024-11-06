import pandas as pd
from sqlalchemy.engine import Engine


def extract(tables : list,con: Engine)-> pd.DataFrame:
    """
    :param con: the connection to the database
    :param tables: the tables to extract
    :return: a list of tables in df format
    """
    a = []
    for i in tables:
        aux = pd.read_sql_table(i, con)
        a.append(aux)
    return a

def extract_ips(con: Engine):
    """
    Extract data from database where the conexion established
    :param con:
    :return:
    """
    dim_ips = pd.read_sql_table('ips', con)
    return dim_ips
def extract_medico(con: Engine):
    dim_medico = pd.read_sql_table('medico', con)
    return dim_medico

def extract_persona(con: Engine):
    beneficiarios = pd.read_sql_table("beneficiario", con)
    cotizantes = pd.read_sql_table("cotizante", con)
    cot_ben = pd.read_sql_table("cotizante_beneficiario", con)
    return [beneficiarios, cotizantes, cot_ben]

def extract_trans_servicio(con: Engine):
    df_citas = pd.read_sql_table('citas_generales', con)
    df_urgencias = pd.read_sql_table('urgencias', con)
    df_hosp = pd.read_sql_table('hospitalizaciones', con)
    return [df_citas, df_urgencias, df_hosp]

def extract_hecho_atencion(con: Engine):
    df_diag = pd.read_sql_table('dim_diag', con)
    df_demo = pd.read_sql_table('dim_demografia', con)
    df_trans = pd.read_sql_table('trans_servicio', con)
    dim_persona = pd.read_sql_table('dim_persona', con)
    dim_medico = pd.read_sql_table('dim_medico', con)
    dim_servicio = pd.read_sql_table('dim_servicio', con)
    dim_ips = pd.read_sql_table('dim_ips', con)
    dim_fecha = pd.read_sql_table('dim_fecha', con)
    return [df_trans,dim_persona,dim_medico,dim_servicio,dim_ips,dim_fecha,df_diag,df_demo]#editar para anadir diag y demo

def extract_medicamentos():
    df_medicamentos = pd.read_excel('sources/medicamentos.xls')
    df_medicamentos.rename(columns={'Código':'codigo', 'Nombre Genérico':'nombre','Forma Farmacéutica':'forma',
                                    'Laboratorio y Registro':'laboratorio', 'Tipo Medicamento':'tipo'}, inplace=True)
    return df_medicamentos
def extract_receta(con:Engine):
    df_receta = pd.read_sql_query('''select codigo_formula , id_medico, id_usuario, fecha, 
    medicamentos_recetados as medicamentos from formulas_medicas''',con)
    return df_receta
def extract_demografia(con: Engine):
    df_benco= pd.read_sql_table('cotizante_beneficiario', con)
    df_cotizantes = pd.read_sql_query(
        '''select cedula as numero_identificacion, tipo_cotizante, estado_civil, sexo, fecha_nacimiento,
            nivel_escolaridad, estracto, proviene_otra_eps,salario_base,tipo_discapacidad,id_ips from cotizante''', con)
    df_beneficiarios = pd.read_sql_query(
        '''select id_beneficiario as numero_identificacion, fecha_nacimiento, sexo, estado_civil,
         tipo_discapacidad from beneficiario ''', con)
    df_ips = pd.read_sql_query('select id_ips,municipio,departamento from ips', con )
    empresa = pd.read_sql_query('select nit , nombre as empresa from empresa', con)
    empcot = pd.read_sql_query('select empresa as nit, cotizante as numero_identificacion from empresa_cotizante', con)
    return [df_benco,df_cotizantes,df_beneficiarios,df_ips, empresa, empcot]

def extract_enfermedades(con : Engine):
    urgencias = pd.read_sql_query('select id_usuario, diagnostico,fecha_atencion from urgencias', con)
    hospitalizaciones = pd.read_sql_query('select id_usuario, diagnostico, fecha_atencion  from hospitalizaciones', con)
    citas_generales = pd.read_sql_query('select id_usuario, diagnostico,fecha_atencion  from citas_generales', con)
    remisiones = pd.read_sql_query('select id_usuario, diagnostico, fecha_atencion  from remisiones', con)

    return [urgencias, citas_generales, hospitalizaciones, remisiones]

def extract_paymetns(con: Engine):
    df = pd.read_sql_query('select * from pagos', con)
    return df

def extract_retiros(con: Engine,con_etl):
    df_retiros = pd.read_sql_query('select * from retiros', con)
    df_pagos = pd.read_sql_query('select * from pagos', con)
    df_per = pd.read_sql_table('dim_persona', con_etl)
    df_dom = pd.read_sql_query('select * from dim_demografia', con_etl)
    df_fecha = pd.read_sql_query('select * from dim_fecha', con_etl)
    return [df_pagos, df_retiros,df_per, df_dom,df_fecha]

def extract_hecho_entrega(source, etl):
    df_med = extract_medicamentos()
    df_form = extract_receta(source)
    df_per = pd.read_sql_table('dim_persona', etl)
    df_doc = pd.read_sql_table('dim_medico', etl)
    df_fecha = pd.read_sql_table('dim_fecha',etl)
    return [df_med,df_form,df_per, df_doc,df_fecha]