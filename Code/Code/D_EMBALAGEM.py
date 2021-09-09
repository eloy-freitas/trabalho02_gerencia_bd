import pandas as pd
from pandasql import sqldf

from sqlalchemy import inspect
from sqlalchemy.types import Integer, String, Float

import CONNECTION as con
import DW_TOOLS as dwt


def extract(conn, dim_exists):
    stg_embalagem = dwt.read_table(conn, schema='stage', table_name='stg_disque_economia',
                                   columns=['CODIGO_TIPO_EMBALAGEM_PRODUTO', 'TIPO_EMBALAGEM_PRODUTO_DESCRICAO',
                                            'TIPO_EMBALAGEM_PRODUTO_SIGLA'])

    if dim_exists:
        query = """
            SELECT stg.* 
            FROM stg_embalagem stg 
            LEFT JOIN dw.d_embalagem de 
            ON stg."CODIGO_TIPO_EMBALAGEM_PRODUTO" = de.cd_embalagem 
            WHERE de.cd_embalagem IS NULL 
        """

        stg_embalagem = sqldf(query, {"stg_embalagem": stg_embalagem}, conn.url)

    # print(stg_embalagem)
    return stg_embalagem


def treat(stg_embalagem, conn, dim_exists):
    columns_select = ['CODIGO_TIPO_EMBALAGEM_PRODUTO', 'TIPO_EMBALAGEM_PRODUTO_DESCRICAO',
                      'TIPO_EMBALAGEM_PRODUTO_SIGLA']

    columns_name = {'CODIGO_TIPO_EMBALAGEM_PRODUTO': 'cd_embalagem',
                    'TIPO_EMBALAGEM_PRODUTO_DESCRICAO': 'ds_embalagem',
                    'TIPO_EMBALAGEM_PRODUTO_SIGLA': 'ds_embalagem_sigla'}

    dim_embalagem = (
        stg_embalagem.
            filter(columns_select).
            rename(columns=columns_name).
            assign(
            cd_embalagem=lambda x: x.cd_embalagem.astype('int64')
        ).drop_duplicates()
    )

    if dim_exists:
        sk_max = dwt.find_sk(conn=conn, schema_name='dw', table_name='d_embalagem', sk_name='sk_embalagem')
        dim_embalagem.insert(0, 'sk_embalagem', range(sk_max, sk_max + len(dim_embalagem)))

    else:
        dim_embalagem.insert(0, 'sk_embalagem', range(1, 1 + len(dim_embalagem)))

        dim_embalagem = (
            pd.DataFrame([[-1, -1, "Não informado", "Não informado"],
                          [-2, -2, "Não aplicável", "Não aplicável"],
                          [-3, -3, "Desconhecido", "Desconhecido"]
                          ], columns=dim_embalagem.columns).append(dim_embalagem)
        )

    print(dim_embalagem)
    return dim_embalagem


def load(dim_embalagem, conn):
    data_types = {
        "sk_embalagem": Integer(),
        "cd_embalagem": Integer(),
        "ds_embalagem": String(),
        "ds_embalagem_sigla": String(),
    }

    (
        dim_embalagem.
            astype('string').
            to_sql(name='d_embalagem', con=conn, schema='dw', if_exists='append', index=False, dtype=data_types)
    )


def run(conn):
    fl_dim = inspect(conn).has_table(table_name='d_embalagem', schema='dw')

    stg_embalagem = extract(conn=conn, dim_exists=fl_dim)

    if not stg_embalagem.empty:
        (
            treat(stg_embalagem=stg_embalagem, conn=conn, dim_exists=fl_dim).
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
