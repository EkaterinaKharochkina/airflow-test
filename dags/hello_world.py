from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# function definition

def foo(name):
    print(f'Hello, {name}!')

# DAG logic

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 3),
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

print_hello_world = PythonOperator(task_id='print_hello_world',
                                   dag=dag,
                                   python_callable=foo,
                                   op_kwargs = {'name':'Katya'})
print_hello_world_2 = PythonOperator(task_id='print_hello_world_2',
                                   dag=dag,
                                   python_callable=foo,
                                   op_args = ['Katya'])
print_hello_world>>print_hello_world_2