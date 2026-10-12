from datetime import datetime, timedelta
import json
import logging
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


def load_dm_users():
    WF_KEY = 'dm_users_stg_to_dds_workflow'
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    
    with dwh_pg_connect.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT workflow_settings 
                FROM dds.srv_wf_settings 
                WHERE workflow_key = %s;
            """, (WF_KEY,))
            row = cursor.fetchone()
            
            last_id = 0
            if row and row[0]:
                settings = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                last_id = settings.get('last_loaded_id', 0)

            cursor.execute("""
                SELECT id, object_id, object_value 
                FROM stg.ordersystem_users 
                WHERE id > %s 
                ORDER BY id ASC;
            """, (last_id,))
            users_stg = cursor.fetchall()

            if not users_stg:
                log.info("Нет новых пользователей для загрузки.")
                return

            users_to_insert = []
            for row_user in users_stg:
                stg_id, object_id, object_value = row_user
                val = json.loads(object_value) if isinstance(object_value, str) else object_value
                users_to_insert.append((
                    str(object_id),
                    val.get("name", ""),
                    val.get("login", "")
                ))

            insert_users_sql = """
                INSERT INTO dds.dm_users (user_id, user_name, user_login)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET
                    user_name = EXCLUDED.user_name,
                    user_login = EXCLUDED.user_login;
            """
            cursor.executemany(insert_users_sql, users_to_insert)

            max_id = max(user[0] for user in users_stg)
            wf_settings_json = json.dumps({'last_loaded_id': max_id})

            update_wf_sql = """
                INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                VALUES (%s, %s)
                ON CONFLICT (workflow_key) DO UPDATE SET workflow_settings = EXCLUDED.workflow_settings;
            """
            cursor.execute(update_wf_sql, (WF_KEY, wf_settings_json))

            conn.commit()
            log.info(f"Загружено пользователей: {len(users_to_insert)}. Новый checkpoint id: {max_id}")


def load_dm_restaurants():
    WF_KEY = 'dm_restaurants_stg_to_dds_workflow'
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    with dwh_pg_connect.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT workflow_settings 
                FROM dds.srv_wf_settings 
                WHERE workflow_key = %s;
            """, (WF_KEY,))
            row = cursor.fetchone()

            last_id = 0
            if row and row[0]:
                settings = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                last_id = settings.get('last_loaded_id', 0)

            cursor.execute("""
                SELECT id, object_id, object_value, update_ts 
                FROM stg.ordersystem_restaurants 
                WHERE id > %s 
                ORDER BY id ASC;
            """, (last_id,))
            restaurants_stg = cursor.fetchall()

            if not restaurants_stg:
                log.info("Нет новых ресторанов для загрузки.")
                return

            restaurants_to_insert = []
            for row_rest in restaurants_stg:
                stg_id, object_id, object_value, update_ts = row_rest
                val = json.loads(object_value) if isinstance(object_value, str) else object_value
                
                active_from = update_ts or val.get("update_ts")
                active_to = datetime(2099, 12, 31, 0, 0, 0)

                restaurants_to_insert.append((
                    str(object_id),
                    val.get("name", ""),
                    active_from,
                    active_to
                ))

            insert_restaurants_sql = """
                INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (restaurant_id) DO UPDATE SET
                    restaurant_name = EXCLUDED.restaurant_name,
                    active_from = EXCLUDED.active_from,
                    active_to = EXCLUDED.active_to;
            """
            cursor.executemany(insert_restaurants_sql, restaurants_to_insert)

            max_id = max(rest[0] for rest in restaurants_stg)
            wf_settings_json = json.dumps({'last_loaded_id': max_id})

            update_wf_sql = """
                INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                VALUES (%s, %s)
                ON CONFLICT (workflow_key) DO UPDATE SET workflow_settings = EXCLUDED.workflow_settings;
            """
            cursor.execute(update_wf_sql, (WF_KEY, wf_settings_json))

            conn.commit()
            log.info(f"Загружено ресторанов: {len(restaurants_to_insert)}. Новый checkpoint id: {max_id}")

def load_dm_timestamps():
    WF_KEY = 'dm_timestamps_stg_to_dds_workflow'
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    with dwh_pg_connect.connection() as conn:
        with conn.cursor() as cursor:
            # 1. Читаем зафиксированный чекпоинт
            cursor.execute("""
                SELECT workflow_settings
                FROM dds.srv_wf_settings
                WHERE workflow_key = %s;
            """, (WF_KEY,))
            row = cursor.fetchone()
            last_loaded_id = row[0]['last_loaded_id'] if row else 0

            # 2. Получаем новые заказы из Staging
            cursor.execute("""
                SELECT id, object_value
                FROM stg.ordersystem_orders
                WHERE id > %s
                ORDER BY id ASC;
            """, (last_loaded_id,))
            orders = cursor.fetchall()

            if not orders:
                log.info("Нет новых данных для загрузки в dm_timestamps")
                return

            timestamps_to_insert = []
            max_id = last_loaded_id

            for order_id, object_value_raw in orders:
                max_id = max(max_id, order_id)
                order_json = json.loads(object_value_raw) if isinstance(object_value_raw, str) else object_value_raw
                
                # Фильтруем заказы по финальным статусам
                status = order_json.get('final_status') or order_json.get('status')
                if status in ('CLOSED', 'CANCELLED'):
                    ts_str = order_json.get('date')
                    if ts_str:
                        ts_dt = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
                        timestamps_to_insert.append((
                            ts_dt,
                            ts_dt.year,
                            ts_dt.month,
                            ts_dt.day,
                            ts_dt.date(),
                            ts_dt.time()
                        ))

            # 3. Вставляем записи в dds.dm_timestamps
            if timestamps_to_insert:
                insert_sql = """
                    INSERT INTO dds.dm_timestamps (ts, year, month, day, date, time)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ts) DO NOTHING;
                """
                cursor.executemany(insert_sql, timestamps_to_insert)

            # 4. Сохраняем новый чекпоинт
            update_wf_sql = """
                INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                VALUES (%s, %s)
                ON CONFLICT (workflow_key) DO UPDATE
                SET workflow_settings = EXCLUDED.workflow_settings;
            """
            cursor.execute(update_wf_sql, (WF_KEY, json.dumps({'last_loaded_id': max_id})))
            conn.commit()
            log.info(f"Успешно обработано заказов: {len(orders)}, загружено отметок: {len(timestamps_to_insert)}")

def load_dm_products():
    WF_KEY = 'dm_products_stg_to_dds_workflow'
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    with dwh_pg_connect.connection() as conn:
        with conn.cursor() as cursor:
            # 1. Читаем зафиксированный чекпоинт
            cursor.execute("""
                SELECT workflow_settings
                FROM dds.srv_wf_settings
                WHERE workflow_key = %s;
            """, (WF_KEY,))
            row = cursor.fetchone()
            last_loaded_id = row[0]['last_loaded_id'] if row else 0

            # 2. Маппинг restaurant_id (строковый ID источника -> integer ID в dds.dm_restaurants)
            cursor.execute("SELECT id, restaurant_id FROM dds.dm_restaurants;")
            rest_map = {row[1]: row[0] for row in cursor.fetchall()}

            # 3. Читаем новые данные из staging ресторанов
            cursor.execute("""
                SELECT id, object_value
                FROM stg.ordersystem_restaurants
                WHERE id > %s
                ORDER BY id ASC;
            """, (last_loaded_id,))
            restaurants = cursor.fetchall()

            if not restaurants:
                log.info("Нет новых данных для загрузки в dm_products")
                return

            products_to_insert = []
            max_id = last_loaded_id

            for stg_id, object_value_raw in restaurants:
                max_id = max(max_id, stg_id)
                rest_json = json.loads(object_value_raw) if isinstance(object_value_raw, str) else object_value_raw
                
                rest_str_id = rest_json.get('_id')
                dds_rest_id = rest_map.get(rest_str_id)
                
                if not dds_rest_id:
                    continue

                update_ts_str = rest_json.get('update_ts')
                active_from = datetime.strptime(update_ts_str, '%Y-%m-%d %H:%M:%S') if isinstance(update_ts_str, str) else update_ts_str
                active_to = datetime(2099, 12, 31, 0, 0, 0)

                menu = rest_json.get('menu', [])
                for item in menu:
                    products_to_insert.append((
                        item['_id'],
                        item['name'],
                        item['price'],
                        active_from,
                        active_to,
                        dds_rest_id
                    ))

            # 4. Вставляем / обновляем продукты
            if products_to_insert:
                insert_sql = """
                    INSERT INTO dds.dm_products (product_id, product_name, product_price, active_from, active_to, restaurant_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (product_id) DO UPDATE SET
                        product_name = EXCLUDED.product_name,
                        product_price = EXCLUDED.product_price,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to,
                        restaurant_id = EXCLUDED.restaurant_id;
                """
                cursor.executemany(insert_sql, products_to_insert)

            # 5. Обновляем чекпоинт
            update_wf_sql = """
                INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                VALUES (%s, %s)
                ON CONFLICT (workflow_key) DO UPDATE
                SET workflow_settings = EXCLUDED.workflow_settings;
            """
            cursor.execute(update_wf_sql, (WF_KEY, json.dumps({'last_loaded_id': max_id})))
            conn.commit()
            log.info(f"Успешно обработано ресторанов: {len(restaurants)}, продуктов: {len(products_to_insert)}")

def load_dm_orders():
    WF_KEY = 'dm_orders_stg_to_dds_workflow'
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    with dwh_pg_connect.connection() as conn:
        with conn.cursor() as cursor:
            # 1. Читаем зафиксированный чекпоинт
            cursor.execute("""
                SELECT workflow_settings
                FROM dds.srv_wf_settings
                WHERE workflow_key = %s;
            """, (WF_KEY,))
            row = cursor.fetchone()
            last_loaded_id = row[0]['last_loaded_id'] if row else 0

            # 2. Выгружаем справочники-маппинги из DDS
            # Пользователи: user_id (str) -> id (int)
            cursor.execute("SELECT id, user_id FROM dds.dm_users;")
            user_map = {r[1]: r[0] for r in cursor.fetchall()}

            # Рестораны: restaurant_id (str) -> id (int)
            cursor.execute("SELECT id, restaurant_id FROM dds.dm_restaurants;")
            rest_map = {r[1]: r[0] for r in cursor.fetchall()}

            # Временные метки: ts (timestamp) -> id (int)
            cursor.execute("SELECT id, ts FROM dds.dm_timestamps;")
            ts_map = {r[1]: r[0] for r in cursor.fetchall()}

            # 3. Получаем новые заказы из Staging
            cursor.execute("""
                SELECT id, object_value
                FROM stg.ordersystem_orders
                WHERE id > %s
                ORDER BY id ASC;
            """, (last_loaded_id,))
            orders = cursor.fetchall()

            if not orders:
                log.info("Нет новых данных для загрузки в dm_orders")
                return

            orders_to_insert = []
            max_id = last_loaded_id

            for stg_id, object_value_raw in orders:
                max_id = max(max_id, stg_id)
                order_json = json.loads(object_value_raw) if isinstance(object_value_raw, str) else object_value_raw

                order_key = order_json.get('_id')
                status = order_json.get('final_status') or order_json.get('status')
                
                # Поиск внешних ключей (FK)
                user_str_id = order_json.get('user', {}).get('id') if isinstance(order_json.get('user'), dict) else order_json.get('user_id')
                rest_str_id = order_json.get('restaurant', {}).get('id') if isinstance(order_json.get('restaurant'), dict) else order_json.get('restaurant_id')
                
                ts_str = order_json.get('date')
                ts_dt = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S') if isinstance(ts_str, str) else ts_str

                dds_user_id = user_map.get(user_str_id)
                dds_rest_id = rest_map.get(rest_str_id)
                dds_ts_id = ts_map.get(ts_dt)

                if dds_user_id and dds_rest_id and dds_ts_id:
                    orders_to_insert.append((
                        order_key,
                        status,
                        dds_rest_id,
                        dds_ts_id,
                        dds_user_id
                    ))

            # 4. Вставляем заказы в dds.dm_orders
            if orders_to_insert:
                insert_sql = """
                    INSERT INTO dds.dm_orders (order_key, order_status, restaurant_id, timestamp_id, user_id)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (order_key) DO UPDATE SET
                        order_status = EXCLUDED.order_status,
                        restaurant_id = EXCLUDED.restaurant_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        user_id = EXCLUDED.user_id;
                """
                cursor.executemany(insert_sql, orders_to_insert)

            # 5. Обновляем чекпоинт
            update_wf_sql = """
                INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                VALUES (%s, %s)
                ON CONFLICT (workflow_key) DO UPDATE
                SET workflow_settings = EXCLUDED.workflow_settings;
            """
            cursor.execute(update_wf_sql, (WF_KEY, json.dumps({'last_loaded_id': max_id})))
            conn.commit()
            log.info(f"Успешно обработано сырых заказов: {len(orders)}, загружено в dm_orders: {len(orders_to_insert)}")

def load_fct_product_sales():
    WF_KEY = 'fct_product_sales_stg_to_dds_workflow'
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    with dwh_pg_connect.connection() as conn:
        with conn.cursor() as cursor:
            # 1. Читаем зафиксированный чекпоинт
            cursor.execute("""
                SELECT workflow_settings
                FROM dds.srv_wf_settings
                WHERE workflow_key = %s;
            """, (WF_KEY,))
            row = cursor.fetchone()
            last_loaded_id = row[0]['last_loaded_id'] if row else 0

            # 2. Формируем словари-маппинги из измерений DDS
            cursor.execute("SELECT id, order_key FROM dds.dm_orders;")
            order_map = {r[1]: r[0] for r in cursor.fetchall()}

            cursor.execute("SELECT id, product_id FROM dds.dm_products;")
            product_map = {r[1]: r[0] for r in cursor.fetchall()}

            # 3. Выгружаем события bonus_transaction из Staging
            cursor.execute("""
                SELECT id, event_value
                FROM stg.bonussystem_events
                WHERE id > %s AND event_type = 'bonus_transaction'
                ORDER BY id ASC;
            """, (last_loaded_id,))
            events = cursor.fetchall()

            if not events:
                log.info("Нет новых данных для fct_product_sales")
                return

            sales_to_insert = []
            max_id = last_loaded_id

            for event_id, event_value_raw in events:
                max_id = max(max_id, event_id)
                event_json = json.loads(event_value_raw) if isinstance(event_value_raw, str) else event_value_raw

                order_key = event_json.get('order_id')
                dds_order_id = order_map.get(order_key)

                if not dds_order_id:
                    continue

                for item in event_json.get('product_payments', []):
                    prod_str_id = item.get('product_id')
                    dds_product_id = product_map.get(prod_str_id)

                    if not dds_product_id:
                        continue

                    # Извлекаем данные из JSON (учитываем quantity и product_cost)
                    count = int(item.get('quantity') or item.get('count') or 0)
                    price = float(item.get('price', 0.0))
                    total_sum = float(item.get('product_cost', price * count))
                    bonus_payment = float(item.get('bonus_payment', 0.0))
                    bonus_grant = float(item.get('bonus_grant', 0.0))

                    sales_to_insert.append((
                        dds_product_id,
                        dds_order_id,
                        count,
                        price,
                        total_sum,
                        bonus_payment,
                        bonus_grant
                    ))

            # 4. Вставляем записи в dds.fct_product_sales
            if sales_to_insert:
                insert_sql = """
                    INSERT INTO dds.fct_product_sales (
                        product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (product_id, order_id) DO UPDATE SET
                        count = EXCLUDED.count,
                        price = EXCLUDED.price,
                        total_sum = EXCLUDED.total_sum,
                        bonus_payment = EXCLUDED.bonus_payment,
                        bonus_grant = EXCLUDED.bonus_grant;
                """
                cursor.executemany(insert_sql, sales_to_insert)

            # 5. Обновляем чекпоинт
            update_wf_sql = """
                INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                VALUES (%s, %s)
                ON CONFLICT (workflow_key) DO UPDATE
                SET workflow_settings = EXCLUDED.workflow_settings;
            """
            cursor.execute(update_wf_sql, (WF_KEY, json.dumps({'last_loaded_id': max_id})))
            conn.commit()
            log.info(f"Успешно обработано событий: {len(events)}, загружено фактов: {len(sales_to_insert)}")

default_args = {
    'owner': 'student',
    'start_date': pendulum.datetime(2022, 5, 5, tz="UTC"),
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='sprint5_dds_dag',
    default_args=default_args,
    schedule_interval='*/15 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['sprint5', 'dds']
) as dag:

    load_users_task = PythonOperator(
        task_id='load_users_to_dds',
        python_callable=load_dm_users
    )

    load_restaurants_task = PythonOperator(
        task_id='load_restaurants_to_dds',
        python_callable=load_dm_restaurants
    )

    load_timestamps_task = PythonOperator(
        task_id='load_timestamps_to_dds',
        python_callable=load_dm_timestamps,
    )

    load_products_task = PythonOperator(
        task_id='load_products_to_dds',
        python_callable=load_dm_products,
    )

    load_orders_task = PythonOperator(
        task_id='load_orders_to_dds',
        python_callable=load_dm_orders,
    )

    load_fct_sales_task = PythonOperator(
        task_id='load_fct_product_sales_to_dds',
        python_callable=load_fct_product_sales,
    )

# Загрузка продуктов зависит от ресторанов
load_restaurants_task >> load_products_task

# Загрузка заказов зависит от трех измерений
[load_users_task, load_restaurants_task, load_timestamps_task] >> load_orders_task

# Загрузка фактов зависит от продуктов и заказов
[load_products_task, load_orders_task] >> load_fct_sales_task