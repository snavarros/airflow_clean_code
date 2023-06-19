def consulta_gw_parvulos(anio_asistencia, anio_parvulario,
                         mes_asistencia, tipo_grupo):
    consulta_global_parvulos = f'''
select
reg.cod_region as COD_REG_ESTAB,
pro.id_provincia as COD_PRO_ESTAB,
pro.nombre as DESC_PRO_ESTAB,
com.id_comuna as COD_COM_ESTAB,
com.nombre as DESC_COM_ESTAB,
est.direccion as DESC_DIR_ESTAB,
est.tipo_ruralidad_id as COD_RURALIDAD,
est.latitud as LATITUD,
est.longitud as LONGITUD,
est.id_establecimiento as ID_ESTAB,
est.cod_establec as COD_ESTAB,
est.rbd as COD_RBD,
est.dv_rbd as DGV_RBD,
est.nombre as NOM_ESTAB,
mat.grupo_id as ID_GRUPO,
gru.letra as GRUPO,
gru.nivel_grado_id  as COD_NIVEL,
gru.programa_id as COD_PROGRAMA,
gru.modalidad_id as COD_MODALIDAD,
gru.capacidad as CAPACIDAD_ESTAB,
jor.id_jornada as ID_JORNADA,
est.responsable as ENCARG_ESTAB,
est.correo_electronico as EMAIL_ESTAB,
est.telefono as TELEFONO_ESTAB,
est.ingresa_asistencia as INGRESA_ASISTENCIA,
est.estado as ESTADO_ESTAB,
gru.activo as GRUPO_ACTIVO,
gru.visible as GRUPO_VISIBLE,
mat.id_matricula as ID_MATRICULA,
par.rut as RUN_ALU,
par.dv as DGV_ALU,
par.nombres as NOM_ALU,
par.apellido_paterno as APP_ALU,
par.apellido_materno as APM_ALU,
par.sexualidad_id as GEN_ALU,
to_char(par.fecha_nacimiento,'YYYY-MM-DD') as FEC_NAC_ALU,
to_char(mat.fecha_incorporacion,'YYYY-MM-DD')  as FEC_ING_ALU,
to_char(mat.fecha_retiro ,'YYYY-MM-DD')  as FECHA_RETIRO,
par.etnia_id as COD_ETNIA_ALU,
par.nacionalidad_id as COD_NAC_ALU,
par.ndes_educvas_espc_id as NEE_ALU,
mov.id_movimiento as ID_MOVIMIENTO,
tip_mov.cod_tipo_mov as LETRA_MOVIMIENTO,
mov.correl_tipo_mov as NUMERO_MOVIMIENTO,
mov.descripcion as DESC_MOVIMIENTO,
mat.visible as MAT_VISIBLE,
fp.neep as FICHA_NEEP,
fp.peso_talla as FICHA_PESO_TALLA,
fp.mujer_jefa_hogar as FICHA_MJH,
fp.actividad_laboral as FICHA_ALM,
fp.accidentes as FICHA_ACC,
mat.enfermedades,
(asi.asistencia::json -> 'asistencia') -> '1' as D1,
(asi.asistencia::json -> 'asistencia') -> '2' as D2,
(asi.asistencia::json -> 'asistencia') -> '3' as D3,
(asi.asistencia::json -> 'asistencia') -> '4' as D4,
(asi.asistencia::json -> 'asistencia') -> '5' as D5,
(asi.asistencia::json -> 'asistencia') -> '6' as D6,
(asi.asistencia::json -> 'asistencia') -> '7' as D7,
(asi.asistencia::json -> 'asistencia') -> '8' as D8,
(asi.asistencia::json -> 'asistencia') -> '9' as D9,
(asi.asistencia::json -> 'asistencia') -> '10' as D10,
(asi.asistencia::json -> 'asistencia') -> '11' as D11,
(asi.asistencia::json -> 'asistencia') -> '12' as D12,
(asi.asistencia::json -> 'asistencia') -> '13' as D13,
(asi.asistencia::json -> 'asistencia') -> '14' as D14,
(asi.asistencia::json -> 'asistencia') -> '15' as D15,
(asi.asistencia::json -> 'asistencia') -> '16' as D16,
(asi.asistencia::json -> 'asistencia') -> '17' as D17,
(asi.asistencia::json -> 'asistencia') -> '18' as D18,
(asi.asistencia::json -> 'asistencia') -> '19' as D19,
(asi.asistencia::json -> 'asistencia') -> '20' as D20,
(asi.asistencia::json -> 'asistencia') -> '21' as D21,
(asi.asistencia::json -> 'asistencia') -> '22' as D22,
(asi.asistencia::json -> 'asistencia') -> '23' as D23,
(asi.asistencia::json -> 'asistencia') -> '24' as D24,
(asi.asistencia::json -> 'asistencia') -> '25' as D25,
(asi.asistencia::json -> 'asistencia') -> '26' as D26,
(asi.asistencia::json -> 'asistencia') -> '27' as D27,
(asi.asistencia::json -> 'asistencia') -> '28' as D28,
(asi.asistencia::json -> 'asistencia') -> '29' as D29,
(asi.asistencia::json -> 'asistencia') -> '30' as D30,
(asi.asistencia::json -> 'asistencia') -> '31' as D31
    from
        matricula as mat
    left join
        parvulo as par on mat.parvulo_id = par.id_parvulo
    left join
        grupo as gru on mat.grupo_id = gru.id_grupo
    left join
        establecimiento as est
        on gru.establecimiento_id  = est.id_establecimiento
    left join
        comuna as com on est.comuna_id = com.id_comuna
    left join
        provincia as pro on com.provincia_id = pro.id_provincia
    left join
        region reg on pro.region_id = reg.id_region
    left join
        asistencia asi on mat.id_matricula = asi.matricula_id
    left join
        movimiento mov on mat.movimiento_id = mov.id_movimiento
    left join
        tipo_movimiento tip_mov
        on mov.tipo_movimiento_id = tip_mov.id_tipo_movimiento
    left join
        jornada jor on jor.id_jornada = gru.jornada_id
    left join ficha_parvulo fp on fp.parvulo_id = par.id_parvulo
    where
            asi.ano = {anio_asistencia}
        and asi.mes = {mes_asistencia}
        and gru.ano_parvulario = {anio_parvulario}
        and (mov.correl_tipo_mov is null or mov.correl_tipo_mov not in (50,52))
        and gru.tipo_grupo_id = {tipo_grupo}
    ---and est.cod_establec = 1101001
    ---and reg.cod_region in (2,4,7,8,12)
    ---and reg.cod_region in (13)
    ---and gru.programa_id in (1,2)
    ---and gru.letra = '1'
    '''
    return consulta_global_parvulos


def consulta_enfermedades_gw():
    consulta_enfermedades = '''
    select * from enfermedad
    '''
    return consulta_enfermedades


def asistencia_parvulos_gpw(mes_asistencia, anio_asistencia, tipo_grupo):
    asistencia_parvulos = f'''
    select
        asi.ano as AGNO,
        asi.mes as MES,
        mat.id_matricula,
        mat.parvulo_id as ID_PARVULO,
        par.rut as RUN_ALU,
        par.dv as DGV_ALU,
        mat.fecha_incorporacion as FEC_ING_ALU,
        mat.fecha_retiro,
        mat.movimiento_id,
        mat.ingreso_movimiento_id,
        mat.enfermedades,
        mat.grupo_id as ID_GRUPO,
        est.cod_establec as COD_ESTAB,
        gru.letra as GRUPO,
        gru.capacidad as CAPACIDAD_GRUPO,
        gru.programa_id as COD_PROG,
        gru.modalidad_id as COD_MODAL,
        gru.nivel_grado_id as COD_SUBNIVEL,
        asi.fecha_modificacion as ASI_FEC_MODIF,
        asi.hora_modificacion as ASI_HORA_MODIF,
        (asi.asistencia::json -> 'asistencia') -> '1' as D1,
        (asi.asistencia::json -> 'asistencia') -> '2' as D2,
        (asi.asistencia::json -> 'asistencia') -> '3' as D3,
        (asi.asistencia::json -> 'asistencia') -> '4' as D4,
        (asi.asistencia::json -> 'asistencia') -> '5' as D5,
        (asi.asistencia::json -> 'asistencia') -> '6' as D6,
        (asi.asistencia::json -> 'asistencia') -> '7' as D7,
        (asi.asistencia::json -> 'asistencia') -> '8' as D8,
        (asi.asistencia::json -> 'asistencia') -> '9' as D9,
        (asi.asistencia::json -> 'asistencia') -> '10' as D10,
        (asi.asistencia::json -> 'asistencia') -> '11' as D11,
        (asi.asistencia::json -> 'asistencia') -> '12' as D12,
        (asi.asistencia::json -> 'asistencia') -> '13' as D13,
        (asi.asistencia::json -> 'asistencia') -> '14' as D14,
        (asi.asistencia::json -> 'asistencia') -> '15' as D15,
        (asi.asistencia::json -> 'asistencia') -> '16' as D16,
        (asi.asistencia::json -> 'asistencia') -> '17' as D17,
        (asi.asistencia::json -> 'asistencia') -> '18' as D18,
        (asi.asistencia::json -> 'asistencia') -> '19' as D19,
        (asi.asistencia::json -> 'asistencia') -> '20' as D20,
        (asi.asistencia::json -> 'asistencia') -> '21' as D21,
        (asi.asistencia::json -> 'asistencia') -> '22' as D22,
        (asi.asistencia::json -> 'asistencia') -> '23' as D23,
        (asi.asistencia::json -> 'asistencia') -> '24' as D24,
        (asi.asistencia::json -> 'asistencia') -> '25' as D25,
        (asi.asistencia::json -> 'asistencia') -> '26' as D26,
        (asi.asistencia::json -> 'asistencia') -> '27' as D27,
        (asi.asistencia::json -> 'asistencia') -> '28' as D28,
        (asi.asistencia::json -> 'asistencia') -> '29' as D29,
        (asi.asistencia::json -> 'asistencia') -> '30' as D30,
        (asi.asistencia::json -> 'asistencia') -> '31' as D31
    from matricula mat
    left join asistencia asi on mat.id_matricula = asi.matricula_id
    left join grupo as gru on mat.grupo_id = gru.id_grupo
    left join establecimiento as est on
        gru.establecimiento_id  = est.id_establecimiento
    left join parvulo as par on mat.parvulo_id = par.id_parvulo
    where asi.ano = {anio_asistencia}
    and asi.mes = {mes_asistencia}
    and gru.tipo_grupo_id = {tipo_grupo}
    and (mat.movimiento_id is null or mat.movimiento_id not in (15,20))
    '''
    return asistencia_parvulos
