from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


from datetime import datetime, timedelta

dag_owner = 'snavarro'

default_args = {'owner': dag_owner,
                'depends_on_past': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=30)
                }


@dag(
    'asistencia_mensual_parvulos',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 6, 19),
    tags=['asistencia', 'bases', 'parvulos']
)
def etl_asistencia_mensual_gpw():

    hoy = datetime.now()
    mes = hoy.month
    anio = hoy.year
    anio_parvulario = anio
    if mes in (1, 2):
        anio_parvulario -= 1
    tipo_grupo = 1

    @task
    def extraer_asistencia_parvulos(mes, anio_parvulario, tipo_grupo):
        '''
        Se obtiene asistencia de parvulos a traves de consulta SQL,
        utilizando herramientas propias de Airflow (PostgresHook,
        get_pandas_df)
        '''
        # Consulta SQL GPW PARVULOS
        from src.gpw.database.consultas import asistencia_parvulos_gpw

        hook = PostgresHook(postgres_conn_id="gpw_database")
        sql = asistencia_parvulos_gpw(mes, anio_parvulario, tipo_grupo)
        dataframe = hook.get_pandas_df(sql)
        # FIN Consulta SQL
        return dataframe

    @task
    def transformar_base_parvulos(mes, anio_parvulario, dataframe):
        from src.gpw.bases_parvulos import set_dtypes_matricula_asistencia
        from src.gpw.bases_parvulos import set_date_base
        from src.gpw.bases_parvulos import eliminar_matriculas_duplicadas
        from src.gpw.bases_parvulos import corregir_fines_de_semana
        from src.gpw.bases_parvulos import corregir_feriados_mes
        from src.gpw.bases_parvulos import  \
            obtener_region_by_codigo_establecimiento
        from src.gpw.bases_parvulos import \
            obtener_comuna_by_codigo_establecimiento
        from src.gpw.bases_parvulos import corregir_feriados_regionales
        from src.gpw.bases_parvulos import corregir_ultimo_dia_mes

        dataframe = set_dtypes_matricula_asistencia(dataframe)
        dataframe = set_date_base(dataframe)
        dataframe = eliminar_matriculas_duplicadas(dataframe)
        dataframe = corregir_fines_de_semana(mes, anio_parvulario, dataframe)
        dataframe = corregir_feriados_mes(mes, anio_parvulario, dataframe)
        dataframe = obtener_region_by_codigo_establecimiento(dataframe)
        dataframe = obtener_comuna_by_codigo_establecimiento(dataframe)
        dataframe = corregir_feriados_regionales(
            mes, anio_parvulario, dataframe)
        dataframe = corregir_ultimo_dia_mes(
            mes, anio_parvulario, dataframe)
        return dataframe

    @task
    def cargar_base_parvulos(dataframe):
        from airflow.providers.sftp.hooks.sftp import SFTPHook

        dataframe.to_csv('/opt/airflow/dags/data/asistencia_parvulos.csv',
                         sep=";", index=False)
        destHook = SFTPHook(ftp_conn_id='sftp_sdep')

        source_files = destHook.list_directory('./Pruebas/JUNJI/')
        print(source_files)
        destHook.store_file(
            local_full_path='/opt/airflow/dags/data/asistencia_parvulos.csv',
            remote_full_path='./Pruebas/JUNJI/asistencia_parvulos.csv')

    @task_group(group_id='etl_asistencia')
    def etl_asistencia():
        extraer = extraer_asistencia_parvulos(mes, anio_parvulario, tipo_grupo)
        transformar = transformar_base_parvulos(mes, anio_parvulario, extraer)
        cargar_base_parvulos(transformar)

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    start >> etl_asistencia() >> end


etl_asistencia_mensual_gpw()
