from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

# Аргументы по умолчанию для DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создаем DAG
with DAG(
    'first_dag',  # Уникальное имя DAG
    default_args=default_args,
    description='Мой первый DAG для тестирования',
    schedule_interval=timedelta(days=1),  # Запускать каждый день
    catchup=False,  # Не запускать пропущенные запуски
    tags=['example', 'test'],
) as dag:

    # Задача 1: Простая bash команда
    task1 = BashOperator(
        task_id='print_hello',
        bash_command='echo "Привет от Airflow!"',
    )

    # Задача 2: Python функция
    def print_current_time():
        logging.info(f"Текущее время: {datetime.now()}")
        print("Эта задача выполняется Python оператором!")

    task2 = PythonOperator(
        task_id='print_time',
        python_callable=print_current_time,
    )

    # Задача 3: Еще одна bash команда
    task3 = BashOperator(
        task_id='print_goodbye',
        bash_command='echo "Завершение работы DAG!"',
    )

    # Определяем порядок выполнения задач
    task1 >> task2 >> task3
