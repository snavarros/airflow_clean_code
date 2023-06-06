import numpy as np
import pandas as pd


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
    dataframe['grupo'] = dataframe['grupo'].fillna(np.nan).astype('category')
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
        np.nan).astype('category')
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
    dataframe['gen_alu'] = dataframe['gen_alu'].fillna(
        np.nan).astype('category')
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


def set_date_base_parvulos(dataframe):
    dataframe['fec_nac_alu'] = pd.to_datetime(dataframe['fec_nac_alu'])
    dataframe['fec_ing_alu'] = pd.to_datetime(dataframe['fec_ing_alu'])
    dataframe['fecha_retiro'] = pd.to_datetime(dataframe['fecha_retiro'])
    return dataframe
