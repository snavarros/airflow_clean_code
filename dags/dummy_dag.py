from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="HISTORICO_GRUPOS",
    schedule_interval="0 4 * * *",
    default_args={'owner': 'snavarro', 'retries': 5,
                  'retry_delay': timedelta(minutes=15)},
    start_date=datetime(2023, 6, 26),

) as DAG:
    
    python_task = PythonOperator(
    task_id="python_task",
    python_callable=lambda: print('Hi from python operator'),
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )