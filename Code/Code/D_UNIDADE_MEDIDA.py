import pandas as pd
from pandasql import sqldf

from sqlalchemy import inspect
from sqlalchemy.types import Integer, String, Float

import CONNECTION as con
import DW_TOOLS as dwt


def extract(conn, dim_exists):
    stg_unidade_medida = dwt.read_table(conn, schema='stage', table_name='stg_disque_economia',
                                        columns=['CODIGO_TIPO_UNIDADE_MEDIDA_PRODUTO',
                                                 'TIPO_UNIDADE_MEDIDA_PRODUTO_DESCRICAO',
                                                 'TIPO_UNIDADE_MEDIDA_PRODUTO_SIGLA'])

    if dim_exists:
        query = """
            SELECT stg.*
            FROM stg_unidade_medida stg
            LEFT JOIN dw.d_unidade_medida du
            ON stg."CODIGO_TIPO_UNIDADE_MEDIDA_PRODUTO" = du.cd_unidade_medida
            WHERE du.cd_unidade_medida IS NULL
        """

        stg_unidade_medida = sqldf(query, {"stg_unidade_medida": stg_unidade_medida}, conn.url)

    # print(stg_unidade_medida)
    return stg_unidade_medida


def treat(stg_unidade_medida, conn, dim_exists):
    columns_select = ['CODIGO_TIPO_UNIDADE_MEDIDA_PRODUTO', 'TIPO_UNIDADE_MEDIDA_PRODUTO_DESCRICAO',
                      'TIPO_UNIDADE_MEDIDA_PRODUTO_SIGLA']

    columns_name = {'CODIGO_TIPO_UNIDADE_MEDIDA_PRODUTO': 'cd_unidade_medida',
                    'TIPO_UNIDADE_MEDIDA_PRODUTO_DESCRICAO': 'ds_unidade_medida',
                    'TIPO_UNIDADE_MEDIDA_PRODUTO_SIGLA': 'ds_unidade_medida_sigla'}

    dim_unidade_medida = (
        stg_unidade_medida.
            filter(columns_select).
            rename(columns=columns_name).
            assign(
            cd_unidade_medida=lambda x: x.cd_unidade_medida.astype('int64')
        ).drop_duplicates()
    )

    if dim_exists:
        sk_max = dwt.find_sk(con=conn, schema_name='dw', table_name='d_unidade_medida', sk_name='sk_unidade_medida')
        dim_unidade_medida.insert(0, 'sk_unidade_medida', range(sk_max, sk_max + len(dim_unidade_medida)))

    else:
        dim_unidade_medida.insert(0, 'sk_unidade_medida', range(1, 1 + len(dim_unidade_medida)))

        dim_unidade_medida = (
            pd.DataFrame([[-1, -1, "Não informado", "Não informado"],
                          [-2, -2, "Não aplicável", "Não aplicável"],
                          [-3, -3, "Desconhecido", "Desconhecido"]
                          ], columns=dim_unidade_medida.columns).append(dim_unidade_medida)
        )

    print(dim_unidade_medida)
    return dim_unidade_medida


def load(dim_unidade_medida, conn):
    data_tyes = {
        "sk_unidade_medida": Integer(),
        "cd_unidade_medida": Integer(),
        "ds_unidade_medida": String(),
        "ds_unidade_medida_sigla": String()
    }

    (
        dim_unidade_medida.
            astype('string').
            to_sql(name='d_unidade_medida', con=conn, schema='dw', if_exists='append', index=False, dtype=data_tyes)
    )


def run(conn):
    fl_dim = inspect(conn).has_table(table_name='d_unidade_medida', schema='dw')

    stg_unidade_medida = extract(conn=conn, dim_exists=fl_dim)

    if not stg_unidade_medida.empty:
        (
            treat(stg_unidade_medida=stg_unidade_medida, conn=conn, dim_exists=fl_dim).
                pipe(load, conn=conn)
        )


if __name__ == '__main__':
    conn_output = con.create_connection(server='localhost', database='trabalho_gbd', password='14159265',
                                        username='postgres', port=5432)

    pd.set_option('display.max_columns', 110)
    pd.set_option('display.max_rows', 110)
    pd.set_option('display.width', 110)
    pd.options.display.max_rows = 110
    pd.options.display.max_columns = 110

    run(conn=conn_output)
