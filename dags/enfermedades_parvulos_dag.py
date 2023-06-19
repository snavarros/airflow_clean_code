from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor


dag_owner = 'snavarro'

default_args = {'owner': dag_owner,
                'depends_on_past': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=30)
                }


@dag(
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 6, 12),
    tags=['parquet', 'bases']
)
def enfermedades_parvulos_dag():
    '''
    Proceso para obtener enfermedades de los parvulos, DAG
    dependiente de 'base_parvulos_parquet_dag', una vez
    finaliza el proceso padre, este es lanzado y genera
    archivo de enfermedades.
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

        df = pd.read_parquet(
            path=path.join(url_servidor, file_name),
            columns=['cod_reg_estab', 'cod_estab', 'grupo', 'cod_nivel',
                     'run_alu', 'dgv_alu', 'nom_alu', 'app_alu',
                     'apm_alu', 'gen_alu', 'enfermedades'])
        return df

    @task
    def tansformar_base_parvulos(df):
        from src.gpw.bases_parvulos import get_enfermedades_base_parvulos
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from src.gpw.database.consultas import consulta_enfermedades_gw

        # Consulta SQL GPW PARVULOS
        hook = PostgresHook(postgres_conn_id="gpw_database")
        sql = consulta_enfermedades_gw()
        df_db = hook.get_pandas_df(sql)

        df_db = df_db.loc[:, ['id_enfermedad', 'descripcion']]
        df_db.rename(columns={'id_enfermedad': 'cod_enferm'}, inplace=True)

        df3 = get_enfermedades_base_parvulos(df)
        df_salida = pd.merge(left=df3, right=df_db,
                             how='left', on='cod_enferm')
        return df_salida

    @task
    def cargar_base_parvulos(dataframe, servidor_url):
        from src.common.crear_carpetas import create_data_folders

        FILE_NAME = f'''ENFERMEDADES_PARVULOS_MES_{mes:02d}_AÑO_{anio}_ + \
            OBTENIDA_{pd.Timestamp.today().strftime('FECHA_%d_%m_%Y')}_.csv'''
        create_data_folders(servidor_url)
        full_url = f'{servidor_url}/{FILE_NAME}'
        dataframe.to_csv(
            full_url
        )

    wait_for_dag = ExternalTaskSensor(
        task_id='esperar_base_parvulos_parquet',
        external_dag_id='base_parvulos_parquet',
        external_task_id=None,
        check_existence=True,
        timeout=3600,
    )

    @task_group(group_id='etl_enfermedades')
    def call_dag():
        df = leer_base_parvulos_parquet(URL_SERVIDOR, FILE_NAME)
        df_salida = tansformar_base_parvulos(df)
        cargar_base_parvulos(df_salida, URL_SERVIDOR)

    wait_for_dag >> call_dag()


enfermedades_parvulos_dag()
