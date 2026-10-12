import json
import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_BASE_URL = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'

NICKNAME = Variable.get("COURIER_API_NICKNAME", default_var="kostya")
COHORT = Variable.get("COURIER_API_COHORT", default_var="1")
API_KEY = Variable.get("COURIER_API_KEY", default_var="25c27781-8fde-4b30-a22e-524044a7580f")

HEADERS = {
    'X-Nickname': NICKNAME,
    'X-Cohort': COHORT,
    'X-API-KEY': API_KEY,
}

PG_CONN_ID = 'PG_WAREHOUSE_CONNECTION'

def fetch_and_load_couriers():
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = pg_hook.get_conn()
    cursor=conn.cursor()

    limit = 50
    offset = 0
    total_loaded = 0

    logging.info('начало загрузки курьеров')

    while True:
        params = {
            'sort_field':'id',
            'sort_direction':'asc',
            'limit': limit,
            'offset': offset,
        }

        response = requests.get(f'{API_BASE_URL}/couriers', headers = HEADERS, params = params)
        response.raise_for_status()
        couriers = response.json() 

        if not couriers:
            break

        insert_sql = '''
            insert into stg.api_couriers(courier_id, object_value, update_ts)
            values(%s, %s, %s)
            on conflict (courier_id) do update set
                object_value = EXCLUDED.object_value,
                update_ts = EXCLUDED.update_ts;
        '''

        current_ts = datetime.utcnow()
        rows_to_insert = [
            (
                courier['_id'],
                json.dumps(courier, ensure_ascii=False),
                current_ts,
            )
            for courier in couriers 
        ]

        cursor.executemany(insert_sql, rows_to_insert)
        conn.commit()

        total_loaded += len(couriers)

        if len(couriers) < limit:
            break 

        offset += limit 

    cursor.close()
    conn.close()

def fetch_and_load_deliveries(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    execution_date = kwargs.get("execution_date") or datetime.utcnow()
    to_dt = execution_date
    from_dt = to_dt - timedelta(days=7)

    from_str = from_dt.strftime("%Y-%m-%d %H:%M:%S")
    to_str = to_dt.strftime("%Y-%m-%d %H:%M:%S")

    logging.info(f"начало загрузки доставок из API за период: {from_str} — {to_str}")

    limit = 50
    offset = 0
    total_loaded = 0

    while True:
        params = {
            "sort_field": "_id",
            "sort_direction": "asc",
            "limit": limit,
            "offset": offset,
            "from": from_str,
            "to": to_str,
        }

        response = requests.get(f"{API_BASE_URL}/deliveries", headers=HEADERS, params=params)
        response.raise_for_status()
        deliveries = response.json()

        if not deliveries:
            break

        insert_sql = """
            insert into stg.api_deliveries (order_id, delivery_id, object_value, delivery_ts, update_ts)
            values (%s, %s, %s, %s, %s)
            on conflict (order_id) do update set
                delivery_id = EXCLUDED.delivery_id,
                object_value = EXCLUDED.object_value,
                delivery_ts = EXCLUDED.delivery_ts,
                update_ts = EXCLUDED.update_ts;
        """

        current_ts = datetime.utcnow()
        rows_to_insert = [
            (
                delivery["order_id"],
                delivery["delivery_id"],
                json.dumps(delivery, ensure_ascii=False),
                delivery["delivery_ts"],
                current_ts,
            )
            for delivery in deliveries
        ]

        cursor.executemany(insert_sql, rows_to_insert)
        conn.commit()

        total_loaded += len(deliveries)

        if len(deliveries) < limit:
            break

        offset += limit

    cursor.close()
    conn.close()
    logging.info(f"Загрузка доставок завершена.")

def load_fct_deliveries():
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    insert_fact_sql = """
        INSERT INTO dds.fct_deliveries (order_id, courier_id, timestamp_id, delivery_ts, rate, tip_sum)
        SELECT 
            o.id AS order_id,
            c.id AS courier_id,
            t.id AS timestamp_id,
            src.delivery_ts,
            (src.object_value->>'rate')::int AS rate,
            (src.object_value->>'tip_sum')::numeric AS tip_sum
        FROM stg.api_deliveries src
        JOIN dds.dm_orders o ON o.order_key = src.order_id
        JOIN dds.dm_couriers c ON c.courier_id = src.courier_id
        JOIN dds.dm_timestamps t ON t.date = DATE(src.delivery_ts)
        ON CONFLICT (order_id) DO UPDATE SET
            courier_id = EXCLUDED.courier_id,
            timestamp_id = EXCLUDED.timestamp_id,
            delivery_ts = EXCLUDED.delivery_ts,
            rate = EXCLUDED.rate,
            tip_sum = EXCLUDED.tip_sum;
    """
    
    cursor.execute(insert_fact_sql)
    conn.commit()
    
    cursor.close()
    conn.close()
    logging.info("Факты доставок успешно перенесены в dds.fct_deliveries.")

default_args = {
    'owner':'student',
    'start_date':datetime(2026, 1, 1),
    'retries':2,
    'retry_delay':timedelta(minutes=1),
}

with DAG(
    dag_id='sprint5_stg_courier_api_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['sprint5','stg','api'],
) as dag:

    load_couriers_task = PythonOperator(
        task_id='load_couriers_to_stg',
        python_callable=fetch_and_load_couriers,
    )

    load_deliveries_task = PythonOperator(
        task_id='load_deliveries_to_stg',
        python_callable=fetch_and_load_deliveries,
    )

    load_fct_deliveries_task = PythonOperator(
        task_id='load_fct_deliveries_to_dds',
        python_callable=load_fct_deliveries,
    )

    load_couriers_task >> load_deliveries_task >> load_fct_deliveries_task