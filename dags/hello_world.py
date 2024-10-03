from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('my_hello_world',
          default_args=default_args,
          schedule_interval=None,
          catchup=False
)

def foo():
    print('Hello World!')

print_hello_world = PythonOperator(task_id='print_hello_world', dag=dag, python_callable=foo)
