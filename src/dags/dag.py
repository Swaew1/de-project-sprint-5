from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_ranks():
    # 1. Подключаемся к источнику и читаем данные
    origin_hook = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    origin_conn = origin_hook.get_conn()
    with origin_conn.cursor() as cursor:
        cursor.execute("SELECT id, name, bonus_percent, min_payment_threshold FROM ranks;")
        ranks = cursor.fetchall()

    # 2. Подключаемся к DWH, очищаем целевую таблицу и загружаем новые данные
    dest_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    dest_conn = dest_hook.get_conn()
    with dest_conn.cursor() as cursor:
        # Очищаем таблицу перед полной перезагрузкой
        cursor.execute("TRUNCATE TABLE stg.bonussystem_ranks;")
        
        # Записываем вычитанные данные
        insert_sql = """
            INSERT INTO stg.bonussystem_ranks (id, name, bonus_percent, min_payment_threshold)
            VALUES (%s, %s, %s, %s);
        """
        cursor.executemany(insert_sql, ranks)
        dest_conn.commit()

def load_users():
    # 1. Подключаемся к источнику и читаем users
    origin_hook = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    origin_conn = origin_hook.get_conn()
    with origin_conn.cursor() as cursor:
        cursor.execute("SELECT id, order_user_id FROM users;")
        users = cursor.fetchall()

    # 2. Подключаемся к DWH, очищаем и перезагружаем stg.bonussystem_users
    dest_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    dest_conn = dest_hook.get_conn()
    with dest_conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE stg.bonussystem_users;")
        insert_sql = """
            INSERT INTO stg.bonussystem_users (id, order_user_id)
            VALUES (%s, %s);
        """
        cursor.executemany(insert_sql, users)
        dest_conn.commit()
def load_events():
    WF_KEY = 'outbox_to_stg'
    dest_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    dest_conn = dest_hook.get_conn()

    # 1. Читаем сохраненный курсор из stg.srv_wf_settings
    last_id = 0
    with dest_conn.cursor() as cursor:
        cursor.execute("""
            SELECT workflow_settings 
            FROM stg.srv_wf_settings 
            WHERE workflow_key = %s;
        """, (WF_KEY,))
        row = cursor.fetchone()
        if row and row[0]:
            settings = json.loads(row[0]) if isinstance(row[0], str) else row[0]
            last_id = settings.get('last_id', 0)

    # 2. Вычитываем новые записи из таблицы outbox источника
    origin_hook = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    origin_conn = origin_hook.get_conn()
    with origin_conn.cursor() as cursor:
        cursor.execute("""
            SELECT id, event_ts, event_type, event_value 
            FROM public.outbox 
            WHERE id > %s 
            ORDER BY id ASC;
        """, (last_id,))
        events = cursor.fetchall()

    # Если новых записей нет, завершаем выполнение
    if not events:
        return

    # 3 и 4. Сохраняем данные и обновляем курсор В ЕДИНОЙ ТРАНЗАКЦИИ
    max_id = max(event[0] for event in events)
    wf_settings_json = json.dumps({'last_id': max_id})

    with dest_conn.cursor() as cursor:
        # Запись в таблицу stg.bonussystem_events
        insert_events_sql = """
            INSERT INTO stg.bonussystem_events (id, event_ts, event_type, event_value)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """
        cursor.executemany(insert_events_sql, events)

        # Обновление курсора в stg.srv_wf_settings
        update_wf_sql = """
            INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
            VALUES (%s, %s)
            ON CONFLICT (workflow_key) DO UPDATE SET workflow_settings = EXCLUDED.workflow_settings;
        """
        cursor.execute(update_wf_sql, (WF_KEY, wf_settings_json))

        # Фиксируем транзакцию
        dest_conn.commit()

default_args = {
    'owner': 'student',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='stg_bonussystem_ranks_dag',
    default_args=default_args,
    schedule_interval='*/15 * * * *',  # Запуск каждые 15 минут
    catchup=False,
    max_active_runs=1
) as dag:

    load_ranks_task = PythonOperator(
        task_id='load_ranks_to_stg',
        python_callable=load_ranks
    )

    load_users_task = PythonOperator(
        task_id='load_users_to_stg',
        python_callable=load_users 
    )

    load_events_task = PythonOperator(
        task_id='events_load',
        python_callable=load_events 
    )

    # Последовательность выполнения шагов
    load_ranks_task >> load_users_task >> load_events_task