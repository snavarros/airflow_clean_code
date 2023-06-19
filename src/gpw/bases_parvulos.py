import numpy as np
import pandas as pd
import logging
import holidays
import calendar
from datetime import datetime


def eliminar_matriculas_duplicadas(dataframe):
    '''
    Elimina las columnas de parvulos duplicados, para ello es
    requisito contar con columnas especificas para poder realizarlo.

    Args: dataframe con columnas requirentes.

    return: dataframe modificado.
    '''
    if {'run_alu', 'cod_estab', 'grupo'}.issubset(dataframe.columns):
        dataframe['llave_aux'] = dataframe.cod_estab.map(
            str) + '_' + dataframe.grupo.map(str) + '_' + \
            dataframe.run_alu.map(str)
        dataframe.sort_values(by=['id_matricula'], inplace=True)
        duplicados = dataframe[dataframe.duplicated(
            ['llave_aux'], keep='last')]
        if len(duplicados) > 0:
            # Guardar el dataframe e imprimir el resultado en logging
            logging.info(
                f'Se han eliminado {len(duplicados)}, parvulos duplicados')
        dataframe.drop_duplicates(
            subset='llave_aux', keep='last', inplace=True)
        dataframe.sort_values(by=['cod_estab', 'grupo'], inplace=True)
        dataframe.drop(columns=['llave_aux'], inplace=True)
        return dataframe
    logging.warning(
        '''El dataframe no posee las columnas requeridas
        para eliminar matriculas duplicadas''')
    return dataframe


def obtener_feriados_publicos_mes(mes: int, anio: int):
    '''
    Obtiene todos los dias feriados del año a nivel nacional
    y los retorna por la fecha desagregada (DIA, MES, ANIO).
    '''
    df = pd.DataFrame(holidays.Chile(years=anio).items(),
                      columns=['fecha', 'evento'])
    df.fecha = pd.to_datetime(df.fecha)
    df[['anio', 'mes', 'dia']] = [x.timetuple()[:3] for x in df.fecha.tolist()]
    df = df.loc[df.mes == mes]
    df = df[['anio', 'mes', 'dia', 'fecha', 'evento']]
    return df


def corregir_fines_de_semana(mes: int, anio: int, dataframe):
    '''
    En asistencia corrige los dias d1, d2 ...dx que son fin de semana,
    definiendolos como no trabajados (-1)

    Args: dataframe que tenga columnas d1, d2 ...dx

    return: dataframe con dias corregidos
    '''
    for dia in range(1, 31):
        fecha = datetime(anio, mes, dia)
        es_fin_de_semana = fecha.weekday()
        if es_fin_de_semana >= 5:
            dia = 'd' + str(dia)
            dataframe[dia] = -1
    return dataframe


def corregir_feriados_mes(mes: int, anio: int, dataframe):
    '''
    En asistencia corrige los dias d1, d2 ...dx que son feriados
    a nivel nacional,definiendolos como no trabajados (-1)
    (no incluye feriados regionales)

    Args:
        mes : Mes del dataframe.
        anio: Año del dataframe.
        dataframe: dataframe del mes y año ingresados.
    '''
    feriados = obtener_feriados_publicos_mes(mes, anio)
    if feriados.empty:
        logging.info('No existen feriados en el mes y año especificado')
        return dataframe
    print(feriados)
    for dia in range(1, 31):
        if dia in feriados['dia'].values:
            dia = 'd' + str(dia)
            dataframe[dia] = -1

    return dataframe


def obtener_region_by_codigo_establecimiento(dataframe):
    '''
    Crea la columna codigo region ('cod_reg_estab') ,a partir
    de la columna codigo establecimiento ('cod_estab').
    '''
    if {'cod_estab'}.issubset(dataframe.columns):
        dataframe['cod_estab'] = dataframe['cod_estab'].astype('str')
        dataframe['cod_reg_estab'] = np.where(
            dataframe.cod_estab.str.len() == 8,
            dataframe.cod_estab.str[:2],
            dataframe.cod_estab.str[:1])

        dataframe['cod_reg_estab'] = dataframe['cod_reg_estab'].astype('uint8')
        dataframe['cod_estab'] = dataframe['cod_estab'].astype('uint32')
        return dataframe


def obtener_comuna_by_codigo_establecimiento(dataframe):
    '''
    Crea la columna codigo comuna ('cod_reg_estab') ,a partir
    de la columna codigo establecimiento ('cod_estab').
    '''
    if {'cod_estab'}.issubset(dataframe.columns):
        dataframe['cod_estab'] = dataframe['cod_estab'].astype('str')
        dataframe['cod_com_estab'] = np.where(
            dataframe.cod_estab.str.len() == 8,
            dataframe.cod_estab.str[:5],
            dataframe.cod_estab.str[:4])
        dataframe['cod_com_estab'] = dataframe['cod_com_estab'].astype(
            'uint16')
        dataframe['cod_estab'] = dataframe['cod_estab'].astype('uint32')
        return dataframe


def corregir_feriados_regionales(mes: int, anio: int, dataframe):
    '''
    Se corrigen los feriados regionales, para ello es
    requisito contar con las columnas cod_reg_estab y cod_com_estab,
    que representan los codigos de region y comunas de los establecimientos
    '''
    if {'cod_reg_estab', 'cod_com_estab'}.issubset(dataframe.columns):
        feriados_regionales = [
            {
                'anio': 2020, 'mes': 6, 'dia': 7,
                'fecha': datetime(2020, 6, 7),
                'evento': 'Asalto y Toma del Morro de Arica',
                'cod_region': 15},
            {
                'anio': 2020, 'mes': 8, 'dia': 20,
                'fecha': datetime(2020, 8, 20),
                'evento': 'Nacimiento del Prócer de la Independencia',
                'cod_region': 16}
        ]
        df_feriados = pd.DataFrame(feriados_regionales)
        feriados_mes = df_feriados.loc[df_feriados.mes == mes]
        print(feriados_mes)
        if feriados_mes.empty:
            print('el mes no tiene feriados regionales')
            return dataframe
        for dia in range(1, 31):
            if dia in feriados_mes['dia'].values:
                dia = 'd' + str(dia)
                if feriados_mes['cod_region'].values == 15:
                    # si no funciona valida variable dia
                    dataframe.loc[dataframe.cod_reg_estab == 15, dia] = -1
                    return dataframe
                if feriados_mes['cod_region'].values == 16:
                    dataframe.loc[dataframe.cod_com_estab.isin(
                        [16101, 16103]), dia] = -1
                    return dataframe


def corregir_ultimo_dia_mes(mes: int, anio: int, dataframe):
    """
    Elimina del dataframe el dia d31, si el mes correspondiente,
    tiene 30 dias.

    Args:
        mes : Mes del dataframe.
        anio: Año del dataframe.
        dataframe: dataframe del mes y año ingresados.
    """
    fecha = datetime(anio, mes, 1)
    res = calendar.monthrange(fecha.year, fecha.month)[1]
    if res == 28:
        dataframe.drop(columns=['d29', 'd30', 'd31'], inplace=True)
    if res == 29:
        dataframe.drop(columns=['d30', 'd31'], inplace=True)
    if res == 30:
        dataframe.drop(columns=['d31'], inplace=True)
    return dataframe


def get_enfermedades_base_parvulos(dataframe):
    df_enfermedades = dataframe.loc[dataframe.enfermedades.notnull()]
    enfermedades_list = []
    for i in range(len(df_enfermedades)):
        for enfermedades in df_enfermedades.iloc[i]['enfermedades']:
            for enfermedad in enfermedades['meses']:
                for registros in enfermedad['registros']:
                    enfermedades_list.append([
                        df_enfermedades.iloc[i]['cod_reg_estab'],
                        df_enfermedades.iloc[i]['cod_estab'],
                        df_enfermedades.iloc[i]['grupo'],
                        df_enfermedades.iloc[i]['cod_nivel'],
                        df_enfermedades.iloc[i]['run_alu'],
                        df_enfermedades.iloc[i]['dgv_alu'],
                        df_enfermedades.iloc[i]['nom_alu'],
                        df_enfermedades.iloc[i]['app_alu'],
                        df_enfermedades.iloc[i]['apm_alu'],
                        df_enfermedades.iloc[i]['gen_alu'],
                        registros['inicio'],
                        registros['termino'],
                        registros['enfermedad'],
                    ])
    df3 = pd.DataFrame(enfermedades_list,
                       columns=['cod_reg_estab', 'cod_estab', 'grupo',
                                'cod_nivel', 'run_alu', 'dgv_alu', 'nom_alu',
                                'app_alu', 'apm_alu', 'gen_alu',
                                'fec_inic_enferm', 'fec_term_enferm',
                                'cod_enferm'])
    return df3


def set_dtypes_matricula_asistencia(dataframe):
    '''
    Se realiza el cambio de tipo de datos a cada una de las variables,
    cargadas en el dataframe de asistencia, con la finalidad de optimizar
    espacio y evitar errores.

    Arg: dataframe.

    return: dataframe.
    '''
    dataframe['agno'] = dataframe['agno'].fillna(0).astype('uint16')
    dataframe['mes'] = dataframe['mes'].fillna(0).astype('uint8')
    dataframe['id_matricula'] = dataframe['id_matricula'].fillna(
        0).astype('uint32')
    dataframe['id_parvulo'] = dataframe['id_parvulo'].fillna(
        0).astype('uint32')
    dataframe['run_alu'] = dataframe['run_alu'].fillna(0).astype('uint32')
    dataframe['dgv_alu'] = dataframe['dgv_alu'].fillna(np.nan).astype('str')
    dataframe['fec_ing_alu'] = dataframe['fec_ing_alu'].fillna(
        np.nan).astype('str')
    dataframe['fecha_retiro'] = dataframe['fecha_retiro'].fillna(
        np.nan).astype('str')
    dataframe['movimiento_id'] = dataframe['movimiento_id'].fillna(
        0).astype('int8')
    dataframe['ingreso_movimiento_id'] = dataframe['ingreso_movimiento_id'] \
        .fillna(0).astype('int8')
    dataframe['id_grupo'] = dataframe['id_grupo'].fillna(0).astype('uint32')
    dataframe['cod_estab'] = dataframe['cod_estab'].fillna(0).astype('uint32')
    dataframe['grupo'] = dataframe['grupo'].fillna(np.nan).astype('str')
    dataframe['capacidad_grupo'] = dataframe['capacidad_grupo'].fillna(
        0).astype('uint16')
    dataframe['cod_prog'] = dataframe['cod_prog'].fillna(
        np.nan).astype('category')
    dataframe['cod_modal'] = dataframe['cod_modal'].fillna(
        np.nan).astype('category')
    dataframe['cod_subnivel'] = dataframe['cod_subnivel'].fillna(
        np.nan).astype('category')
    dataframe['asi_fec_modif'] = dataframe['asi_fec_modif'].fillna(
        np.nan).astype('str')
    dataframe['asi_hora_modif'] = dataframe['asi_hora_modif'].fillna(
        np.nan).astype('str')
    dataframe['d1'] = dataframe['d1'].fillna(-2).astype('int8')
    dataframe['d2'] = dataframe['d2'].fillna(-2).astype('int8')
    dataframe['d3'] = dataframe['d3'].fillna(-2).astype('int8')
    dataframe['d4'] = dataframe['d4'].fillna(-2).astype('int8')
    dataframe['d5'] = dataframe['d5'].fillna(-2).astype('int8')
    dataframe['d6'] = dataframe['d6'].fillna(-2).astype('int8')
    dataframe['d7'] = dataframe['d7'].fillna(-2).astype('int8')
    dataframe['d8'] = dataframe['d8'].fillna(-2).astype('int8')
    dataframe['d9'] = dataframe['d9'].fillna(-2).astype('int8')
    dataframe['d10'] = dataframe['d10'].fillna(-2).astype('int8')
    dataframe['d11'] = dataframe['d11'].fillna(-2).astype('int8')
    dataframe['d12'] = dataframe['d12'].fillna(-2).astype('int8')
    dataframe['d13'] = dataframe['d13'].fillna(-2).astype('int8')
    dataframe['d14'] = dataframe['d14'].fillna(-2).astype('int8')
    dataframe['d15'] = dataframe['d15'].fillna(-2).astype('int8')
    dataframe['d16'] = dataframe['d16'].fillna(-2).astype('int8')
    dataframe['d17'] = dataframe['d17'].fillna(-2).astype('int8')
    dataframe['d18'] = dataframe['d18'].fillna(-2).astype('int8')
    dataframe['d19'] = dataframe['d19'].fillna(-2).astype('int8')
    dataframe['d20'] = dataframe['d20'].fillna(-2).astype('int8')
    dataframe['d21'] = dataframe['d21'].fillna(-2).astype('int8')
    dataframe['d22'] = dataframe['d22'].fillna(-2).astype('int8')
    dataframe['d23'] = dataframe['d23'].fillna(-2).astype('int8')
    dataframe['d24'] = dataframe['d24'].fillna(-2).astype('int8')
    dataframe['d25'] = dataframe['d25'].fillna(-2).astype('int8')
    dataframe['d26'] = dataframe['d26'].fillna(-2).astype('int8')
    dataframe['d27'] = dataframe['d27'].fillna(-2).astype('int8')
    dataframe['d28'] = dataframe['d28'].fillna(-2).astype('int8')
    dataframe['d29'] = dataframe['d29'].fillna(-2).astype('int8')
    dataframe['d30'] = dataframe['d30'].fillna(-2).astype('int8')
    dataframe['d31'] = dataframe['d31'].fillna(-2).astype('int8')
    return dataframe


def set_date_base(dataframe):
    '''
    Se transforma todos los datos de "fecha" en la consulta de asistencia
    de parvulos, en formato datetime.

    Args: dataframe.

    return: dataframe.
    '''
    dataframe['fec_ing_alu'] = pd.to_datetime(dataframe['fec_ing_alu'])
    dataframe['fecha_retiro'] = pd.to_datetime(dataframe['fecha_retiro'])
    dataframe['asi_fec_modif'] = pd.to_datetime(dataframe['asi_fec_modif'])
    return dataframe


def set_date_base_parvulos(dataframe):
    dataframe['fec_nac_alu'] = pd.to_datetime(dataframe['fec_nac_alu'])
    dataframe['fec_ing_alu'] = pd.to_datetime(dataframe['fec_ing_alu'])
    dataframe['fecha_retiro'] = pd.to_datetime(dataframe['fecha_retiro'])
    return dataframe


def set_dtypes_base_parvulos(dataframe):
    dataframe['cod_reg_estab'] = dataframe['cod_reg_estab'].fillna(
        0).astype('uint8')
    dataframe['cod_pro_estab'] = dataframe['cod_pro_estab'].fillna(
        0).astype('uint8')
    dataframe['desc_pro_estab'] = dataframe['desc_pro_estab'].fillna(
        np.nan).astype('str')
    dataframe['cod_com_estab'] = dataframe['cod_com_estab'].fillna(
        0).astype('uint8')
    dataframe['desc_com_estab'] = dataframe['desc_com_estab'].fillna(
        np.nan).astype('str')
    dataframe['desc_dir_estab'] = dataframe['desc_dir_estab'].fillna(
        np.nan).astype('str')
    dataframe['cod_ruralidad'] = dataframe['cod_ruralidad'].fillna(
        0).astype('uint8')
    dataframe['latitud'] = dataframe['latitud'].fillna(0).astype('float32')
    dataframe['longitud'] = dataframe['longitud'].fillna(0).astype('float32')
    dataframe['cod_estab'] = dataframe['cod_estab'].fillna(0).astype('uint32')
    dataframe['cod_rbd'] = dataframe['cod_rbd'].fillna(0).astype('uint32')
    dataframe['dgv_rbd'] = dataframe['dgv_rbd'].fillna(np.nan).astype('str')
    dataframe['nom_estab'] = dataframe['nom_estab'].fillna(
        np.nan).astype('str')
    dataframe['id_grupo'] = dataframe['id_grupo'].fillna(0).astype('uint32')
    dataframe['grupo'] = dataframe['grupo'].fillna(999).astype('uint32')
    dataframe['cod_nivel'] = dataframe['cod_nivel'].fillna(
        np.nan).astype('category')
    dataframe['cod_programa'] = dataframe['cod_programa'].fillna(
        np.nan).astype('category')
    dataframe['cod_modalidad'] = dataframe['cod_modalidad'].fillna(
        np.nan).astype('category')
    dataframe['capacidad_estab'] = dataframe['capacidad_estab'].fillna(
        0).astype('uint64')
    dataframe['id_jornada'] = dataframe['id_jornada'].fillna(
        np.nan).astype('category')
    dataframe['encarg_estab'] = dataframe['encarg_estab'].fillna(
        np.nan).astype('str')
    dataframe['email_estab'] = dataframe['email_estab'].fillna(
        np.nan).astype('str')
    dataframe['telefono_estab'] = dataframe['telefono_estab'].fillna(
        np.nan).astype('str')
    dataframe['ingresa_asistencia'] = dataframe['ingresa_asistencia'].fillna(
        np.nan).astype('category')
    dataframe['estado_estab'] = dataframe['estado_estab'].fillna(
        np.nan).astype('str')
    dataframe['grupo_activo'] = dataframe['grupo_activo'].fillna(
        np.nan).astype('category')
    dataframe['grupo_visible'] = dataframe['grupo_visible'].fillna(
        np.nan).astype('category')
    dataframe['id_matricula'] = dataframe['id_matricula'].fillna(
        0).astype('uint32')
    dataframe['run_alu'] = dataframe['run_alu'].fillna(0).astype('uint32')
    dataframe['dgv_alu'] = dataframe['dgv_alu'].fillna(np.nan).astype('str')
    dataframe['nom_alu'] = dataframe['nom_alu'].fillna(np.nan).astype('str')
    dataframe['app_alu'] = dataframe['app_alu'].fillna(np.nan).astype('str')
    dataframe['apm_alu'] = dataframe['apm_alu'].fillna(np.nan).astype('str')
    dataframe['gen_alu'] = dataframe['gen_alu'].fillna(0).astype('uint8')
    dataframe['fec_nac_alu'] = dataframe['fec_nac_alu'].fillna(
        np.nan).astype('str')
    dataframe['fec_ing_alu'] = dataframe['fec_ing_alu'].fillna(
        np.nan).astype('str')
    dataframe['fecha_retiro'] = dataframe['fecha_retiro'].fillna(
        np.nan).astype('str')
    dataframe['cod_etnia_alu'] = dataframe['cod_etnia_alu'].fillna(
        np.nan).astype('category')
    dataframe['cod_nac_alu'] = dataframe['cod_nac_alu'].fillna(
        np.nan).astype('category')
    dataframe['nee_alu'] = dataframe['nee_alu'].fillna(np.nan).astype('str')
    dataframe['id_movimiento'] = dataframe['id_movimiento'].fillna(
        np.nan).astype('category')
    dataframe['letra_movimiento'] = dataframe['letra_movimiento'].fillna(
        np.nan).astype('str')
    dataframe['numero_movimiento'] = dataframe['numero_movimiento'].fillna(
        np.nan).astype('category')
    dataframe['desc_movimiento'] = dataframe['desc_movimiento'].fillna(
        np.nan).astype('str')
    dataframe['mat_visible'] = dataframe['mat_visible'].fillna(
        np.nan).astype('category')
    dataframe['d1'] = dataframe['d1'].fillna(-2).astype('int8')
    dataframe['d2'] = dataframe['d2'].fillna(-2).astype('int8')
    dataframe['d3'] = dataframe['d3'].fillna(-2).astype('int8')
    dataframe['d4'] = dataframe['d4'].fillna(-2).astype('int8')
    dataframe['d5'] = dataframe['d5'].fillna(-2).astype('int8')
    dataframe['d6'] = dataframe['d6'].fillna(-2).astype('int8')
    dataframe['d7'] = dataframe['d7'].fillna(-2).astype('int8')
    dataframe['d8'] = dataframe['d8'].fillna(-2).astype('int8')
    dataframe['d9'] = dataframe['d9'].fillna(-2).astype('int8')
    dataframe['d10'] = dataframe['d10'].fillna(-2).astype('int8')
    dataframe['d11'] = dataframe['d11'].fillna(-2).astype('int8')
    dataframe['d12'] = dataframe['d12'].fillna(-2).astype('int8')
    dataframe['d13'] = dataframe['d13'].fillna(-2).astype('int8')
    dataframe['d14'] = dataframe['d14'].fillna(-2).astype('int8')
    dataframe['d15'] = dataframe['d15'].fillna(-2).astype('int8')
    dataframe['d16'] = dataframe['d16'].fillna(-2).astype('int8')
    dataframe['d17'] = dataframe['d17'].fillna(-2).astype('int8')
    dataframe['d18'] = dataframe['d18'].fillna(-2).astype('int8')
    dataframe['d19'] = dataframe['d19'].fillna(-2).astype('int8')
    dataframe['d20'] = dataframe['d20'].fillna(-2).astype('int8')
    dataframe['d21'] = dataframe['d21'].fillna(-2).astype('int8')
    dataframe['d22'] = dataframe['d22'].fillna(-2).astype('int8')
    dataframe['d23'] = dataframe['d23'].fillna(-2).astype('int8')
    dataframe['d24'] = dataframe['d24'].fillna(-2).astype('int8')
    dataframe['d25'] = dataframe['d25'].fillna(-2).astype('int8')
    dataframe['d26'] = dataframe['d26'].fillna(-2).astype('int8')
    dataframe['d27'] = dataframe['d27'].fillna(-2).astype('int8')
    dataframe['d28'] = dataframe['d28'].fillna(-2).astype('int8')
    dataframe['d29'] = dataframe['d29'].fillna(-2).astype('int8')
    dataframe['d30'] = dataframe['d30'].fillna(-2).astype('int8')
    dataframe['d31'] = dataframe['d31'].fillna(-2).astype('int8')
    return dataframe
