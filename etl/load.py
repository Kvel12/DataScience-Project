from pandas import DataFrame
from sqlalchemy.engine import Engine
from sqlalchemy import text

def load(table: DataFrame, etl_conn: Engine, tname: str, replace: bool = False):
    """
    Carga un DataFrame en la base de datos
    Args:
        table: DataFrame a cargar
        etl_conn: Conexión a la base de datos
        tname: Nombre de la tabla destino
        replace: Si es True, elimina los datos existentes antes de cargar
    """
    if replace:
        with etl_conn.connect() as conn:
            conn.execute(text(f'Delete from {tname}'))
            conn.commit()
        table.to_sql(tname, etl_conn, if_exists='append', index=False)
    else:
        table.to_sql(tname, etl_conn, if_exists='append', index=False)