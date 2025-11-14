from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def test_clickhouse_connection():
    """Тестируем подключение к ClickHouse"""
    try:
        import clickhouse_connect
        logging.info("🔄 Пытаемся подключиться к ClickHouse...")
        
        # Подключаемся к ClickHouse
        client = clickhouse_connect.get_client(
            host='clickhouse',  # Имя сервиса из docker-compose
            port=8123,
            username='default',
            password=''
        )
        
        # Выполняем тестовый запрос
        result = client.query('SELECT version() as version')
        version = result.result_rows[0][0]
        logging.info(f"✅ ClickHouse подключен успешно! Версия: {version}")
        
        # Создаем тестовую таблицу если её нет
        client.command('''
            CREATE TABLE IF NOT EXISTS test_connections (
                id Int32,
                service String,
                status String,
                timestamp DateTime
            ) ENGINE = MergeTree()
            ORDER BY timestamp
        ''')
        logging.info("✅ Тестовая таблица создана/проверена")
        
        # Вставляем тестовые данные
        client.command('''
            INSERT INTO test_connections VALUES
            (1, 'clickhouse', 'connected', now())
        ''')
        logging.info("✅ Тестовые данные добавлены в ClickHouse")
        
        return f"ClickHouse connection successful - version: {version}"
        
    except Exception as e:
        logging.error(f"❌ Ошибка подключения к ClickHouse: {str(e)}")
        raise

def test_minio_connection():
    """Тестируем подключение к MinIO"""
    try:
        from minio import Minio
        from io import BytesIO
        logging.info("🔄 Пытаемся подключиться к MinIO...")
        
        minio_client = Minio(
            'minio:9000',
            access_key='minioadmin',
            secret_key='minioadmin',
            secure=False
        )
        
        # Проверяем соединение через list_buckets
        buckets = minio_client.list_buckets()
        logging.info(f"✅ Подключение к MinIO установлено. Доступные бакеты: {[b.name for b in buckets]}")
        
        bucket_name = "test-bucket"
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logging.info(f"✅ Создан тестовый бакет: {bucket_name}")
        else:
            logging.info(f"✅ Бакет {bucket_name} уже существует")
        
        # Тестовый файл
        test_content = b"Hello from Airflow DAG!"
        data_stream = BytesIO(test_content)
        
        minio_client.put_object(
            bucket_name,
            "test-file.txt",
            data=data_stream,
            length=len(test_content)
        )
        logging.info("✅ Тестовый файл загружен в MinIO")
        
        # Чтение файла
        response = minio_client.get_object(bucket_name, "test-file.txt")
        content = response.read()
        response.close()
        response.release_conn()
        logging.info(f"✅ Файл прочитан из MinIO: {content.decode()}")
        
        return "MinIO connection successful"
        
    except Exception as e:
        logging.error(f"❌ Ошибка подключения к MinIO: {str(e)}")
        raise

def generate_report(**context):
    """Генерируем отчет после выполнения всех проверок"""
    logging.info("📊 Генерируем отчет о проверке подключений...")
    
    # Получаем результаты предыдущих задач
    ti = context['ti']
    clickhouse_result = ti.xcom_pull(task_ids='test_clickhouse_connection')
    minio_result = ti.xcom_pull(task_ids='test_minio_connection')
    
    logging.info("=" * 50)
    logging.info("ОТЧЕТ О ПРОВЕРКЕ ПОДКЛЮЧЕНИЙ")
    logging.info("=" * 50)
    logging.info(f"ClickHouse: {clickhouse_result or 'FAILED'}")
    logging.info(f"MinIO: {minio_result or 'FAILED'}")
    logging.info("=" * 50)
    
    if clickhouse_result and minio_result:
        logging.info("🎉 Все подключения работают корректно!")
        return "All connections are working properly"
    else:
        error_msg = "Некоторые подключения не работают: "
        errors = []
        if not clickhouse_result:
            errors.append("ClickHouse")
        if not minio_result:
            errors.append("MinIO")
        error_msg += ", ".join(errors)
        raise Exception(error_msg)

with DAG(
    'test_connections_dag',
    default_args=default_args,
    description='DAG для проверки подключений к ClickHouse и MinIO',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['connections', 'test', 'infrastructure'],
    max_active_runs=1,
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    test_clickhouse = PythonOperator(
        task_id='test_clickhouse_connection',
        python_callable=test_clickhouse_connection,
    )

    test_minio = PythonOperator(
        task_id='test_minio_connection',
        python_callable=test_minio_connection,
    )

    generate_report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        provide_context=True,
    )

    end = DummyOperator(
        task_id='end'
    )

    # Определяем порядок выполнения
    start >> [test_clickhouse, test_minio] >> generate_report_task >> end
