import pandas as pd
from datetime import datetime

from pandasql import sqldf
from sqlalchemy import inspect

import CONNECTION as con
import DW_TOOLS as dwt

from sqlalchemy.types import Date, Integer, String


def seasons(day_of_year):

    fall = range(80, 172)
    winter = range(172, 264)
    spring = range(264, 355)
    # summer = everything else

    if day_of_year in spring:
        season = 'Primavera'
    elif day_of_year in winter:
        season = 'Inverno'
    elif day_of_year in fall:
        season = 'Outono'
    else:
        season = 'Verão'

    return season


def extract(conn, dim_exists):
    init_date = datetime.strptime("2019-01-01", "%Y-%m-%d")
    end_date = datetime.strptime("2019-12-31", "%Y-%m-%d")

    dim_data = pd.DataFrame(pd.date_range(start=init_date, end=end_date, freq='d'), columns={"dt_referencia"})

    if dim_exists:

        query = """
            SELECT stg.* 
            FROM dim_data as stg 
            LEFT JOIN dw.d_data as dd 
            ON stg.dt_referencia = dd.dt_referencia 
            WHERE dd.dt_referencia IS NULL
        """

        dim_data = sqldf(query, {"dim_data": dim_data}, conn.url)

    return dim_data


def treat(dim_data, conn, dim_exists):

    weekday_mapping = {
        0: "Segunda-Feira",
        1: "Terça-Feira",
        2: "Quarta-Feira",
        3: "Quinta-Feira",
        4: "Sexta-Feira",
        5: "Sábado",
        6: "Domingo"
    }

    dim_data = (
        dim_data.
        assign(
            dt_ano=lambda x: x.dt_referencia.apply(lambda y: y.year),
            dt_trimestre=lambda x: x.dt_referencia.apply(lambda y: y.quarter),
            dt_mes=lambda x: x.dt_referencia.apply(lambda y: y.month),
            dt_dia=lambda x: x.dt_referencia.apply(lambda y: y.day),
            nu_dia_do_ano=lambda x: x.dt_referencia.apply(lambda y: y.dayofyear),
            ds_estacao=lambda x: x.nu_dia_do_ano.apply(lambda y: seasons(y)),
            nu_semana=lambda x: x.dt_referencia.apply(lambda y: y.weekofyear),
            nu_dia_da_semana=lambda x: x.dt_referencia.apply(lambda y: y.weekday()),
            ds_dia_da_semana=lambda x: x.nu_dia_da_semana.replace(weekday_mapping),
        )
    )

    if dim_exists:
        sk_max = dwt.find_sk(conn=conn, schema_name="dw", table_name="d_data", sk_name="sk_data")
        dim_data.insert(0, "sk_data", range(sk_max, sk_max + len(dim_data)))

    else:
        dim_data.insert(0, "sk_data", range(1, 1 + len(dim_data)))

        dim_data = (
            pd.DataFrame([[-1, '1900-01-01', -1, -1, -1, -1, -1, 'Não informado', -1, -1, 'Não informado'],
                          [-2, '1900-01-01', -2, -2, -2, -2, -2, 'Não aplicável', -2, -2, 'Não aplicável'],
                          [-3, '1900-01-01', -3, -3, -3, -3, -3, 'Desconhecido', -3, -3, 'Desconhecido']
                          ], columns=dim_data.columns).append(dim_data)
        )

    return dim_data


def load(dim_data, conn):
    """
    Carrega a dimensão data para o servidor do DW.

    :param dim_data: dataframe contendo as informações da dimensão data tratadas.
    :param conn: conexão criada via sqlAlchemy com o servidor do DW.
    """

    data_types = {
        'sk_data': Integer(),
        'dt_referencia': Date(),
        'dt_ano': Integer(),
        'dt_trimestre': Integer(),
        'dt_mes': Integer(),
        'dt_dia': Integer(),
        'nu_dia_do_ano': Integer(),
        'ds_estacao': String(),
        'nu_semana': Integer(),
        'nu_dia_da_semana': Integer(),
        'ds_dia_da_semana': String(),
    }

    (
        dim_data.
        astype('string').
        to_sql(name="d_data", con=conn, schema="dw", if_exists='append', index=False, dtype=data_types)
    )


def run(conn):

    fl_dim = inspect(conn).has_table(table_name='d_data', schema='dw')
    dim_data = extract(conn=conn, dim_exists=fl_dim)

    if not dim_data.empty:
        (
            treat(dim_data=dim_data, conn=conn, dim_exists=fl_dim).
            pipe(load, conn)
        )


if __name__ == '__main__':
    pd.set_option('display.max_columns', 110)
    pd.set_option('display.max_rows', 110)
    pd.set_option('display.width', 110)
    pd.options.display.max_rows = 110
    pd.options.display.max_columns = 110

    conn_output = con.create_connection(server='localhost', database='trabalho_gbd', password='14159265',
                                        username='postgres', port=5432)

    run(conn=conn_output)


