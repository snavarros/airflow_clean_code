from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

from src.gpw.database.consultas import consulta_gw_parvulos
from src.gpw.bases_parvulos import set_dtypes_base_parvulos

dag_owner = 'snavarro'

default_args = {'owner': dag_owner,
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=30)
                }

with DAG(dag_id='ETL_BASE_Parvulos_Parquet',
         default_args=default_args,
         description='''Extract, Transform and Load to Parquet Base
         Parvulos Consolidada''',
         start_date=datetime(2023, 6, 2),
         schedule_interval='@daily',
         catchup=False,
         tags=['parquet', 'bases']
         ):

    start = EmptyOperator(task_id='start')

    @task
    def extraer_base_parvulos():
        '''Se obtiene base de parvulos a traves de consulta SQL,
        luego se formatean los tipos de datos de cada una de las variables,
        para luego generar archivo parquet del dia.
        '''
        hoy = datetime.now()
        anio = hoy.year
        mes = hoy.month
        anio_parvulario = anio
        if mes in (1, 2):
            anio_parvulario -= 1

        hook = PostgresHook(postgres_conn_id="gpw_database")
        sql = consulta_gw_parvulos(anio, anio_parvulario, mes, 1)
        df = hook.get_pandas_df(sql)
        df = set_dtypes_base_parvulos(df)
        print(df.head(10))

    end = EmptyOperator(task_id='end')

    start >> extraer_base_parvulos() >> end
