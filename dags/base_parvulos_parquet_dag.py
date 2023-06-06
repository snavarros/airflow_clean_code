from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

from src.gpw.database.consultas import consulta_gw_parvulos
from src.gpw.bases_parvulos import set_dtypes_base_parvulos
from src.gpw.bases_parvulos import set_date_base_parvulos
from src.common.sharepoint.upload import upload_to_sharepoint
from src.common.crear_carpetas import create_data_folders
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

from os import path

dag_owner = 'snavarro'

default_args = {'owner': dag_owner,
                'depends_on_past': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=30)
                }


@dag(
    'base_parvulos_parquet',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(-1),
    tags=['parquet', 'bases']
)
def etl_base_parvulos_gpw():
    '''
    ### ETL BASE PARVULOS GESPARVU WEB
    Este dag es un proceso ETL, extraida de una consulta
    SQL, su finalidad es obtener una consulta maestra de la cual deriven otros
    reportes,siendo esta consulta almacenada como archivo RAW en formato
    PARQUET.
    '''
    import pandas as pd

    hoy = datetime.now()
    anio = hoy.year
    mes = hoy.month
    dia = hoy.day
    anio_parvulario = anio
    if mes in (1, 2):
        anio_parvulario -= 1

    URL_SHAREPOINT = 'https://junji.sharepoint.com/sites/datalake_datos/'
    DIR_SHAREPOINT = f'RAW/GPW/BaseParvulos/{anio}/{mes:02d}/{dia:02d}'

    URL_SERVIDOR = path.join(Variable.get("RAW_PATH"),
                             f'BasesParvulos/{anio}/{mes:02d}/{dia:02d}')

    FILE_NAME = f'BASE_PARVULOS_MES_{mes:02d}_AÑO_{anio}_OBTENIDA_' + \
        pd.Timestamp.today().strftime('FECHA_%d_%m_%Y') + '.parquet'

    @task
    def extraer_base_parvulos(anio, anio_parvulario, mes, tipo_grupo):
        '''
        Se obtiene base de parvulos a traves de consulta SQL,
        utilizando herramientas propias de Airflow (PostgresHook,
        get_pandas_df)
        '''

        # Consulta SQL GPW PARVULOS
        hook = PostgresHook(postgres_conn_id="gpw_database")
        sql = consulta_gw_parvulos(anio, anio_parvulario, mes, tipo_grupo)
        df = hook.get_pandas_df(sql)
        # FIN Consulta SQL
        return df

    @task
    def transformar_base_parvulos(dataframe):
        '''
        A cada una de las columnas se le asigna un tipo de datos para
        que sea mas optima la consulta y utilice menos memoria al ser
        almacenada.
        '''
        # Asignar un tipo de dato a cada columna
        df = set_dtypes_base_parvulos(dataframe)
        # Convertir fechas que son string a date
        df = set_date_base_parvulos(df)
        return df

    @task
    def cargar_base_parvulos(dataframe, sharepoint_url, sharepoint_dir,
                             servidor_url, file_name):
        '''
        Se almacena el dataframe como archivo PARQUET y,
        se sube a sharepoint "data_lake" directorio RAW.
        '''
        create_data_folders(servidor_url)
        full_url = f'{servidor_url}/{file_name}'
        dataframe.to_parquet(
            path=full_url
        )

        upload_to_sharepoint(sharepoint_url, sharepoint_dir, full_url)

    end = EmptyOperator(task_id='end')

    extract = extraer_base_parvulos(anio, anio_parvulario, mes, 1)
    transform = transformar_base_parvulos(extract)
    cargar_base_parvulos(transform, URL_SHAREPOINT,
                         DIR_SHAREPOINT, URL_SERVIDOR, FILE_NAME) >> end


etl_base_parvulos = etl_base_parvulos_gpw()
