import json
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN_ID = 'PG_WAREHOUSE_CONNECTION'

def load_dm_couriers():
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT courier_id, object_value FROM stg.api_couriers;")
    rows = cursor.fetchall()

    insert_sql = """
        INSERT INTO dds.dm_couriers (courier_id, courier_name)
        VALUES (%s, %s)
        ON CONFLICT (courier_id) DO UPDATE SET
            courier_name = EXCLUDED.courier_name;
    """

    couriers_to_insert = []
    for row in rows:
        courier_id = row[0]
        obj_val = row[1]
        if isinstance(obj_val, str):
            obj_val = json.loads(obj_val)
        
        name = obj_val.get("name")
        couriers_to_insert.append((courier_id, name))

    cursor.executemany(insert_sql, couriers_to_insert)
    conn.commit()
    
    cursor.close()
    conn.close()
    logging.info(f"Успешно загружено курьеров в DDS: {len(couriers_to_insert)}")

default_args = {
    'owner': 'student',
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='sprint5_dds_couriers_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['sprint5', 'dds', 'couriers'],
) as dag:

    load_couriers_task = PythonOperator(
        task_id='load_dm_couriers_task',
        python_callable=load_dm_couriers,
    )