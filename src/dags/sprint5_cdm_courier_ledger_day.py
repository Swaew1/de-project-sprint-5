import json 
import logging 
from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook 

PG_CONN_ID = 'PG_WAREHOUSE_CONNECTION'

def calculate_courier_ledger():
    pg_hook=PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn=pg_hook.get_conn()
    cursor=conn.cursor()

    ledger_sql = '''
        WITH order_sums AS (
            SELECT 
                order_id,
                SUM(total_sum) AS order_total_sum
            FROM dds.fct_product_sales
            GROUP BY order_id
        ),
        enriched_deliveries AS (
            SELECT 
                f.order_id,
                f.courier_id,
                f.delivery_ts,
                f.rate,
                f.tip_sum,
                COALESCE(os.order_total_sum, 0) AS order_total_sum
            FROM dds.fct_deliveries f
            LEFT JOIN order_sums os ON f.order_id = os.order_id
        )
        INSERT INTO cdm.dm_courier_ledger (
            courier_id,
            courier_name,
            settlement_year,
            settlement_month,
            orders_count,
            orders_total_sum,
            rate_avg,
            order_processing_fee,
            courier_order_sum,
            courier_tips_sum,
            courier_reward_sum
        )
        SELECT 
            c.courier_id,
            c.courier_name,
            EXTRACT(YEAR FROM ed.delivery_ts)::smallint AS settlement_year,
            EXTRACT(MONTH FROM ed.delivery_ts)::smallint AS settlement_month,
            COUNT(DISTINCT ed.order_id) AS orders_count,
            SUM(ed.order_total_sum) AS orders_total_sum,
            AVG(ed.rate)::numeric(3,2) AS rate_avg,
            SUM(ed.order_total_sum) * 0.25 AS order_processing_fee,
            SUM(
                CASE 
                    WHEN ed.rate < 4 THEN GREATEST(ed.order_total_sum * 0.05, 100)
                    WHEN ed.rate < 4.5 THEN GREATEST(ed.order_total_sum * 0.07, 150)
                    WHEN ed.rate < 4.9 THEN GREATEST(ed.order_total_sum * 0.08, 175)
                    ELSE GREATEST(ed.order_total_sum * 0.10, 200)
                END
            ) AS courier_order_sum,
            SUM(ed.tip_sum) AS courier_tips_sum,
            SUM(
                CASE 
                    WHEN ed.rate < 4 THEN GREATEST(ed.order_total_sum * 0.05, 100)
                    WHEN ed.rate < 4.5 THEN GREATEST(ed.order_total_sum * 0.07, 150)
                    WHEN ed.rate < 4.9 THEN GREATEST(ed.order_total_sum * 0.08, 175)
                    ELSE GREATEST(ed.order_total_sum * 0.10, 200)
                END
            ) + SUM(ed.tip_sum) * 0.95 AS courier_reward_sum
        FROM enriched_deliveries ed
        JOIN dds.dm_couriers c ON ed.courier_id = c.id
        GROUP BY 
            c.courier_id,
            c.courier_name,
            EXTRACT(YEAR FROM ed.delivery_ts),
            EXTRACT(MONTH FROM ed.delivery_ts)
        ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE SET
            courier_name = EXCLUDED.courier_name,
            orders_count = EXCLUDED.orders_count,
            orders_total_sum = EXCLUDED.orders_total_sum,
            rate_avg = EXCLUDED.rate_avg,
            order_processing_fee = EXCLUDED.order_processing_fee,
            courier_order_sum = EXCLUDED.courier_order_sum,
            courier_tips_sum = EXCLUDED.courier_tips_sum,
            courier_reward_sum = EXCLUDED.courier_reward_sum;
    '''

    logging.info('Запуск расчета витрины')
    cursor.execute(ledger_sql)
    conn.commit()

    cursor.close()
    conn.close()

default_args = {
    'owner':'student',
    'start_date':datetime(2026,1,1),
    'retries':2,
    'retry_delay':timedelta(minutes=1),
}

with DAG(
    dag_id='sptint5_cdm_courier_ledger_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['sprint5', 'cdm', 'ledger'],
) as dag:

    calculate_ledger_task = PythonOperator(
        task_id='calculate_courier_ledger_task',
        python_callable=calculate_courier_ledger,
    )