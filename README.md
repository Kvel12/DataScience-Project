# Rapidos y Furiosos - ETL

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

1.  Con el entorno virtual activado, instala las dependencias con el siguiente comando:

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

2. El proceso comenzará a ejecutarse y, dependiendo del volumen de datos, puede tardar mas o menos tiempo. El proceso puede demorarsee alrededor de 5 minutos. Una vez completado, los datos transformados estarán disponibles en la base de datos de destino.

### 5. Verificación

Una vez que el proceso ETL se haya ejecutado correctamente, verifica que los datos se hayan cargado correctamente en la base de datos de destino (bodega).

Para comprobar que la bodega de datos está funcionando correctamente, puedes ejecutar las consultas SQL que se encuentran al final de cada uno de los notebooks correspondientes a las tablas de hechos.
   
Estas consultas SQL deben ejecutarse en el entorno donde se esté corriendo la bodega (es decir, en la base de datos de destino). Puedes hacerlo a través de un cliente SQL, como `pgAdmin`, `DBeaver`, o directamente desde un terminal si estás usando PostgreSQL.
   
Con los resultados arrojados por las consultas, se puede verificar si la bodega responde correctamente a las preguntas del proyecto.

---

### 6. Consultas SQL

A continuación se especificarán las preguntas a las que responde este ETL, y así mismo su consulta SQL:

1. Pregunta 1: ¿En qué meses del año los clientes solicitan más servicios?

    ```sql
    SELECT 
    EXTRACT(YEAR FROM df.fecha)::integer as año,
    CASE EXTRACT(MONTH FROM df.fecha)::integer
        WHEN 1 THEN 'Enero'
        WHEN 2 THEN 'Febrero'
        WHEN 3 THEN 'Marzo'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Mayo'
        WHEN 6 THEN 'Junio'
        WHEN 7 THEN 'Julio'
        WHEN 8 THEN 'Agosto'
        WHEN 9 THEN 'Septiembre'
        WHEN 10 THEN 'Octubre'
        WHEN 11 THEN 'Noviembre'
        WHEN 12 THEN 'Diciembre'
    END as mes,
    COUNT(h.servicio_id) as total_servicios,
    ROUND((COUNT(h.servicio_id)::numeric / SUM(COUNT(h.servicio_id)) OVER (PARTITION BY EXTRACT(YEAR FROM df.fecha))) * 100, 2) || '%' as porcentaje_anual
    FROM hecho_entrega_servicio_diaria h
    JOIN dim_fecha df ON h.key_dim_fecha = df.key_dim_fecha
    GROUP BY 
    EXTRACT(YEAR FROM df.fecha),
    EXTRACT(MONTH FROM df.fecha)
    ORDER BY año, 
    EXTRACT(MONTH FROM df.fecha);
    ```

2. Pregunta 2: ¿Cuáles son los días donde más solicitudes hay?

    ```sql
    SELECT 
    CASE df.dia_semana
        WHEN 0 THEN 'Lunes'
        WHEN 1 THEN 'Martes'
        WHEN 2 THEN 'Miércoles'
        WHEN 3 THEN 'Jueves'
        WHEN 4 THEN 'Viernes'
        WHEN 5 THEN 'Sábado'
        WHEN 6 THEN 'Domingo'
    END as dia,
    COUNT(h.servicio_id) as total_servicios,
    ROUND((COUNT(h.servicio_id)::numeric / SUM(COUNT(h.servicio_id)) OVER ()) * 100, 2) || '%' as porcentaje_total,
    ROUND(AVG(h.cantidad_servicios_dia), 2) as promedio_servicios_por_dia
    FROM hecho_entrega_servicio_diaria h
    JOIN dim_fecha df ON h.key_dim_fecha = df.key_dim_fecha
    GROUP BY df.dia_semana
    ORDER BY 
    CASE 
        WHEN df.dia_semana = 0 THEN 1
        WHEN df.dia_semana = 1 THEN 2
        WHEN df.dia_semana = 2 THEN 3
        WHEN df.dia_semana = 3 THEN 4
        WHEN df.dia_semana = 4 THEN 5
        WHEN df.dia_semana = 5 THEN 6
        WHEN df.dia_semana = 6 THEN 7
    END;
    ```

3. Pregunta 3: ¿A qué hora los mensajeros están más ocupados?

    ```sql
    WITH servicios_por_hora AS (
    SELECT 
        dh.hora,
        dh.periodo_dia,
        COUNT(h.servicio_id) as total_servicios,
        ROUND((COUNT(h.servicio_id)::numeric / SUM(COUNT(h.servicio_id)) OVER ()) * 100, 2) as porcentaje
    FROM hecho_entrega_servicio_hora h
    JOIN dim_hora dh ON h.key_dim_hora = dh.key_dim_hora
    GROUP BY dh.hora, dh.periodo_dia)
    SELECT 
        hora,
        periodo_dia,
        total_servicios,
        porcentaje || '%' as porcentaje_servicios
    FROM servicios_por_hora
    ORDER BY total_servicios DESC;
    ```

4. Pregunta 4: ¿Número de servicios solicitados por cliente y por mes?

    ```sql
    SELECT 
    c.cliente_id,
    c.nombre as nombre_cliente,
    EXTRACT(YEAR FROM df.fecha)::integer as año,
    CASE EXTRACT(MONTH FROM df.fecha)::integer
        WHEN 1 THEN 'Enero'
        WHEN 2 THEN 'Febrero'
        WHEN 3 THEN 'Marzo'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Mayo'
        WHEN 6 THEN 'Junio'
        WHEN 7 THEN 'Julio'
        WHEN 8 THEN 'Agosto'
        WHEN 9 THEN 'Septiembre'
        WHEN 10 THEN 'Octubre'
        WHEN 11 THEN 'Noviembre'
        WHEN 12 THEN 'Diciembre'
    END as mes,
    COUNT(h.servicio_id) as total_servicios
    FROM hecho_entrega_servicio_hora h
    JOIN dim_fecha df ON h.key_dim_fecha = df.key_dim_fecha
    JOIN dim_cliente c ON h.key_dim_cliente = c.key_dim_cliente
    GROUP BY 
        c.cliente_id, 
        c.nombre,
        EXTRACT(YEAR FROM df.fecha),
        EXTRACT(MONTH FROM df.fecha)
    ORDER BY c.cliente_id, año, 
    EXTRACT(MONTH FROM df.fecha);
    ```

5. Pregunta 5: Mensajeros más eficientes (Los que más servicios prestan)

    ```sql
    SELECT 
    m.mensajero_id,
    COUNT(*) AS total_servicios,
    AVG(CASE WHEN h.tiempo_asignacion = '00:00:00' THEN NULL 
        ELSE EXTRACT(EPOCH FROM h.tiempo_asignacion::interval)/60 END) AS promedio_minutos_asignacion,
    AVG(CASE WHEN h.tiempo_recogida = '00:00:00' THEN NULL 
        ELSE EXTRACT(EPOCH FROM h.tiempo_recogida::interval)/60 END) AS promedio_minutos_recogida,
    AVG(CASE WHEN h.tiempo_entrega = '00:00:00' THEN NULL 
        ELSE EXTRACT(EPOCH FROM h.tiempo_entrega::interval)/60 END) AS promedio_minutos_entrega,
    AVG(CASE WHEN h.tiempo_cierre = '00:00:00' THEN NULL 
        ELSE EXTRACT(EPOCH FROM h.tiempo_cierre::interval)/60 END) AS promedio_minutos_cierre,
    SUM(CASE WHEN h.cantidad_novedades > 0 THEN 1 ELSE 0 END) AS servicios_con_novedades,
    COUNT(*) FILTER (WHERE fecha_cerrado IS NOT NULL) AS servicios_completados,
    ROUND((COUNT(*) FILTER (WHERE fecha_cerrado IS NOT NULL)::NUMERIC / COUNT(*) * 100), 2) AS porcentaje_completados
    FROM hecho_entrega_acumulado h
    JOIN dim_mensajero m ON h.key_dim_mensajero = m.key_dim_mensajero
    GROUP BY m.mensajero_id
    ORDER BY total_servicios DESC;
    ```

6. Pregunta 6: ¿Cuáles son las sedes que más servicios solicitan por cada cliente?

    ```sql
    WITH totales_cliente AS (
    SELECT 
        c.cliente_id,
        c.nombre as nombre_cliente,
        COUNT(h.servicio_id) as total_servicios_cliente
    FROM hecho_entrega_servicio_hora h
    JOIN dim_cliente c ON h.key_dim_cliente = c.key_dim_cliente
    GROUP BY c.cliente_id, c.nombre),
    servicios_sede AS (
    SELECT 
        c.cliente_id,
        c.nombre as nombre_cliente,
        s.sede_id,
        s.nombre_sede,
        COUNT(h.servicio_id) as total_servicios_sede,
        RANK() OVER (PARTITION BY c.cliente_id ORDER BY COUNT(h.servicio_id) DESC) as ranking
    FROM hecho_entrega_servicio_hora h
    JOIN dim_cliente c ON h.key_dim_cliente = c.key_dim_cliente
    JOIN dim_sede s ON h.key_dim_sede = s.key_dim_sede
    GROUP BY 
        c.cliente_id,
        c.nombre,
        s.sede_id,
        s.nombre_sede)
    SELECT 
        ss.cliente_id,
        ss.nombre_cliente,
        ss.sede_id,
        ss.nombre_sede,
        ss.total_servicios_sede as servicios_en_sede,
        tc.total_servicios_cliente as total_servicios_cliente,
    ROUND((ss.total_servicios_sede::numeric / tc.total_servicios_cliente::numeric) * 100, 2) || '%' as porcentaje_del_total
    FROM servicios_sede ss
    JOIN totales_cliente tc ON ss.cliente_id = tc.cliente_id
    WHERE ss.ranking = 1
    ORDER BY ss.total_servicios_sede DESC;
    ```

7. Pregunta 7: ¿Cuál es el tiempo promedio de entrega desde que se solicita el servicio hasta que se cierra el caso?

    ```sql
    WITH tiempos_totales AS (
    SELECT 
        h.servicio_id,
        h.fecha_iniciado,
        h.hora_iniciado,
        h.fecha_cerrado,
        h.hora_cerrado,
        CASE 
            WHEN h.fecha_cerrado IS NOT NULL THEN
                EXTRACT(EPOCH FROM (
                    (h.fecha_cerrado || ' ' || h.hora_cerrado)::timestamp - 
                    (h.fecha_iniciado || ' ' || h.hora_iniciado)::timestamp
                ))/60.0
            ELSE NULL
        END AS minutos_totales
    FROM 
        hecho_entrega_acumulado h
    WHERE 
        h.fecha_cerrado IS NOT NULL)
    SELECT 
        COUNT(*) AS total_servicios_completados,
        ROUND(AVG(minutos_totales), 2) AS promedio_minutos,
        ROUND(AVG(minutos_totales)/60, 2) AS promedio_horas,
        ROUND(AVG(minutos_totales)/1440, 2) AS promedio_dias,
        ROUND(MIN(minutos_totales)/60, 2) AS min_horas,
        ROUND(MAX(minutos_totales)/60, 2) AS max_horas
    FROM tiempos_totales;
    ```

8. Pregunta 8: Mostrar los tiempos de espera por cada fase del servicio y ¿en qué fase hay más demoras?

    ```sql
    WITH tiempos_fase AS (
    SELECT 
        'Asignación' as fase,
        COUNT(*) FILTER (WHERE tiempo_asignacion != '00:00:00') as servicios_con_tiempo,
        AVG(EXTRACT(EPOCH FROM tiempo_asignacion::interval)/60) as promedio_minutos,
        MIN(EXTRACT(EPOCH FROM tiempo_asignacion::interval)/60) as min_minutos,
        MAX(EXTRACT(EPOCH FROM tiempo_asignacion::interval)/60) as max_minutos,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM tiempo_asignacion::interval)/60) as mediana_minutos
    FROM hecho_entrega_acumulado
    WHERE tiempo_asignacion != '00:00:00'
    
    UNION ALL
    
    SELECT 
        'Recogida' as fase,
        COUNT(*) FILTER (WHERE tiempo_recogida != '00:00:00') as servicios_con_tiempo,
        AVG(EXTRACT(EPOCH FROM tiempo_recogida::interval)/60) as promedio_minutos,
        MIN(EXTRACT(EPOCH FROM tiempo_recogida::interval)/60) as min_minutos,
        MAX(EXTRACT(EPOCH FROM tiempo_recogida::interval)/60) as max_minutos,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM tiempo_recogida::interval)/60) as mediana_minutos
    FROM hecho_entrega_acumulado
    WHERE tiempo_recogida != '00:00:00'
    
    UNION ALL
    
    SELECT 
        'Entrega' as fase,
        COUNT(*) FILTER (WHERE tiempo_entrega != '00:00:00') as servicios_con_tiempo,
        AVG(EXTRACT(EPOCH FROM tiempo_entrega::interval)/60) as promedio_minutos,
        MIN(EXTRACT(EPOCH FROM tiempo_entrega::interval)/60) as min_minutos,
        MAX(EXTRACT(EPOCH FROM tiempo_entrega::interval)/60) as max_minutos,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM tiempo_entrega::interval)/60) as mediana_minutos
    FROM hecho_entrega_acumulado
    WHERE tiempo_entrega != '00:00:00'
    
    UNION ALL
    
    SELECT 
        'Cierre' as fase,
        COUNT(*) FILTER (WHERE tiempo_cierre != '00:00:00') as servicios_con_tiempo,
        AVG(EXTRACT(EPOCH FROM tiempo_cierre::interval)/60) as promedio_minutos,
        MIN(EXTRACT(EPOCH FROM tiempo_cierre::interval)/60) as min_minutos,
        MAX(EXTRACT(EPOCH FROM tiempo_cierre::interval)/60) as max_minutos,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM tiempo_cierre::interval)/60) as mediana_minutos
    FROM hecho_entrega_acumulado
    WHERE tiempo_cierre != '00:00:00')
    SELECT 
        fase,
        servicios_con_tiempo,
        CAST(promedio_minutos AS NUMERIC(10,2)) as promedio_minutos,
        CAST(promedio_minutos/60 AS NUMERIC(10,2)) as promedio_horas,
        CAST(min_minutos AS NUMERIC(10,2)) as min_minutos,
        CAST(max_minutos AS NUMERIC(10,2)) as max_minutos,
        CAST(mediana_minutos AS NUMERIC(10,2)) as mediana_minutos
    FROM tiempos_fase
    ORDER BY promedio_minutos DESC;
    ```

9. Pregunta 9: ¿Cuáles son las novedades que más se presentan durante la prestación del servicio?

    ```sql
    SELECT 
    n.descripcion AS novedad,
    COUNT(*) AS cantidad_presentaciones
    FROM hecho_novedades_servicio hns
    JOIN dim_novedad n ON hns.key_dim_novedad = n.key_dim_novedad 
    GROUP BY n.descripcion
    ORDER BY cantidad_presentaciones DESC
    ```


## Notas adicionales

- Si tienes problemas de conexión con las bases de datos, asegúrate de que las credenciales en `config.yml` sean correctas.
- Si el entorno virtual no se activa correctamente, asegúrate de que Python esté correctamente instalado en tu sistema.
- Este proyecto requiere que tengas instaladas las librerías necesarias como `pandas`, `sqlalchemy`, `psycopg2`, entre otras, que están listadas en el archivo `requirements.txt`.

---
