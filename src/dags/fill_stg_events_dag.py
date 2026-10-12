from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib import ConnectionBuilder


def fill_stg_events():
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    with origin_pg_connect.connection() as origin_conn, dwh_pg_connect.connection() as dwh_conn:
        with origin_conn.cursor() as origin_cursor, dwh_conn.cursor() as dwh_cursor:
            # Читаем события из источника
            origin_cursor.execute("""
                SELECT id, event_type, event_value, event_ts
                FROM public.outbox
                ORDER BY id ASC;
            """)
            events = origin_cursor.fetchall()
            print(f"Загружено из источника: {len(events)} событий")

            if not events:
                return

            # Записываем в Staging
            insert_sql = """
                INSERT INTO stg.bonussystem_events (id, event_type, event_value, event_ts)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """
            dwh_cursor.executemany(insert_sql, events)
            dwh_conn.commit()
            print("Успешно записано в stg.bonussystem_events!")


with DAG(
    dag_id="stg_fill_events_dag",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["stg", "sprint5"],
) as dag:

    fill_events_task = PythonOperator(
        task_id="fill_stg_events_task",
        python_callable=fill_stg_events,
    )