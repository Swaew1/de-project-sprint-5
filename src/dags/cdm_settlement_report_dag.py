from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


def load_settlement_report():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    with dwh_pg_connect.connection() as conn:
        with conn.cursor() as cursor:
            insert_sql = """
                INSERT INTO cdm.dm_settlement_report (
                    restaurant_id,
                    restaurant_name,
                    settlement_date,
                    orders_count,
                    orders_total_sum,
                    orders_bonus_payment_sum,
                    orders_bonus_granted_sum,
                    order_processing_fee,
                    restaurant_reward_sum
                )
                SELECT
                    r.restaurant_id AS restaurant_id,
                    r.restaurant_name AS restaurant_name,
                    t.date AS settlement_date,
                    COUNT(DISTINCT o.id) AS orders_count,
                    SUM(fps.total_sum) AS orders_total_sum,
                    SUM(fps.bonus_payment) AS orders_bonus_payment_sum,
                    SUM(fps.bonus_grant) AS orders_bonus_granted_sum,
                    ROUND(SUM(fps.total_sum) * 0.25, 5) AS order_processing_fee,
                    ROUND(SUM(fps.total_sum) - SUM(fps.bonus_payment) - (SUM(fps.total_sum) * 0.25), 5) AS restaurant_reward_sum
                FROM dds.fct_product_sales fps
                INNER JOIN dds.dm_orders o ON fps.order_id = o.id
                INNER JOIN dds.dm_restaurants r ON o.restaurant_id = r.id
                INNER JOIN dds.dm_timestamps t ON o.timestamp_id = t.id
                WHERE o.order_status = 'CLOSED'
                GROUP BY r.restaurant_id, r.restaurant_name, t.date
                ON CONFLICT (restaurant_id, settlement_date) DO UPDATE SET
                    restaurant_name = EXCLUDED.restaurant_name,
                    orders_count = EXCLUDED.orders_count,
                    orders_total_sum = EXCLUDED.orders_total_sum,
                    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                    order_processing_fee = EXCLUDED.order_processing_fee,
                    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
            """
            cursor.execute(insert_sql)
            conn.commit()
            log.info("Витрина cdm.dm_settlement_report успешно заполнена!")


with DAG(
    dag_id="cdm_settlement_report_dag",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["cdm", "sprint5"],
) as dag:

    load_report_task = PythonOperator(
        task_id="load_settlement_report_task",
        python_callable=load_settlement_report,
    )