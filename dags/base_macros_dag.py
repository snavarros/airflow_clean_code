from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

dag_owner = 'snavarro'

default_args = {'owner': dag_owner,
                'depends_on_past': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=30)
                }


@dag(
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(0),
    tags=['parquet', 'bases']
)
def base_macro_dag():
    '''
    '''
    import pandas as pd
    from os import path

    hoy = datetime.now()
    anio = hoy.year
    mes = hoy.month
    dia = hoy.day
    anio_parvulario = anio
    if mes in (1, 2):
        anio_parvulario -= 1

    URL_SERVIDOR = path.join(Variable.get("RAW_PATH"),
                             f'BasesParvulos/{anio}/{mes:02d}/{dia:02d}')

    FILE_NAME = f'BASE_PARVULOS_MES_{mes:02d}_AÑO_{anio}_OBTENIDA_' + \
        pd.Timestamp.today().strftime('FECHA_%d_%m_%Y') + '.parquet'

    @task
    def leer_base_parvulos_parquet(url_servidor, file_name):
        df = pd.read_parquet(path=path.join(url_servidor, file_name))
        print(df)

    @task
    def tansformar_base_parvulos(dataframe):
        ...

    @task
    def cargar_base_macros():
        ...

    wait_for_dag = ExternalTaskSensor(
        task_id='esperar_base_parvulos_parquet',
        external_dag_id='base_parvulos_parquet',
        external_task_id=None,
        check_existence=True,
        timeout=3600,
    )
    wait_for_dag >> leer_base_parvulos_parquet(URL_SERVIDOR, FILE_NAME)


base_macro_dag()
