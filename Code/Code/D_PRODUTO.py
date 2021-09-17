import pandas as pd
from pandasql import sqldf

from sqlalchemy import inspect
from sqlalchemy.types import Integer, String, Float

import CONNECTION as con
import DW_TOOLS as dwt


def extract(conn, dim_exists):
    stg_produto = dwt.read_table(conn=conn, schema='stage', table_name='stg_produto',
                                 columns=['CODIGO_PRODUTO', 'PRODUTO_DESCRICAO', 'CATEGORIA_PRODUTO', 'UNIDADE'])

    stg_categoria = dwt.read_table(conn=conn, schema='stage', table_name='stg_disque_economia',
                                   columns=['CODIGO_PRODUTO', 'CODIGO_CATEGORIA_PRODUTO'])

    stg_produto = stg_produto.merge(right=stg_categoria, on='CODIGO_PRODUTO', how='inner')

    if dim_exists:

        query = """
            SELECT stg.* 
            FROM stg_produto stg 
            LEFT JOIN dw.d_produto dp 
            ON (stg."CODIGO_PRODUTO" = dp.cd_produto::INTEGER 
            AND stg."CODIGO_CATEGORIA_PRODUTO" = dp.cd_categoria::INTEGER) 
            WHERE dp.cd_produto IS NULL
        """

        stg_produto = sqldf(query, {"stg_produto": stg_produto}, conn.url)

    return stg_produto


def treat(stg_produto, conn, dim_exists):

    columns_select = ['cd_produto_categoria', 'cd_produto', 'cd_categoria', 'ds_produto', 'ds_categoria', 'qtd_medida']

    columns_name = {'CODIGO_PRODUTO': 'cd_produto',
                    'CODIGO_CATEGORIA_PRODUTO': 'cd_categoria',
                    'PRODUTO_DESCRICAO': 'ds_produto',
                    'CATEGORIA_PRODUTO': 'ds_categoria',
                    'UNIDADE': 'qtd_medida'}

    # print(stg_produto.dtypes)

    dim_produto = (
        stg_produto.
        rename(columns=columns_name).
        assign(
            cd_categoria=lambda x: x.cd_categoria.astype('int64').apply(lambda y: f'{y:0>2}'),
            cd_produto=lambda x: x.cd_produto.astype('int64').apply(lambda y: f'{y:0>5}'),
            cd_produto_categoria=lambda x: x.cd_categoria + x.cd_produto,
            qtd_medida=lambda x: x.qtd_medida.str.strip().apply(lambda y: y[:-2]).astype('float64')
        ).filter(columns_select).
        drop_duplicates()
    )

    if dim_exists:
        sk_max = dwt.find_sk(conn=conn, schema_name='dw', table_name='d_produto', sk_name='sk_produto')
        dim_produto.insert(0, "sk_produto", range(sk_max, sk_max + len(dim_produto)))

    else:
        dim_produto.insert(0, "sk_produto", range(1, 1 + len(dim_produto)))

        dim_produto = (
            pd.DataFrame([[-1, -1, -1, -1, "Não informado", "Não informado", -1],
                          [-2, -2, -2, -2, "Não aplicável", "Não aplicável", -2],
                          [-3, -3, -3, -3, "Desconhecido", "Desconhecido", -3]
                          ], columns=dim_produto.columns).append(dim_produto)
        )

    return dim_produto


def load(dim_produto, conn):

    data_types = {
        "sk_produto": Integer(),
        "cd_produto_categoria": String(),
        "cd_categoria": String(),
        "cd_produto": String(),
        "ds_produto": String(),
        "ds_categoria": String(),
        "qtd_medida": Float(),
    }

    (
        dim_produto.
        astype('string').
        to_sql(name='d_produto', con=conn, schema='dw', if_exists='append', index=False, dtype=data_types)
    )


def run(conn):

    fl_dim = inspect(conn).has_table(table_name='d_produto', schema='dw')

    stg_produto = extract(conn=conn, dim_exists=fl_dim)

    if not stg_produto.empty:
        (
            treat(stg_produto=stg_produto, conn=conn, dim_exists=fl_dim).
            pipe(load, conn=conn)
        )


if __name__ == '__main__':
    # conn_output = con.create_connection(server='localhost', database='trabalho_gbd', password='14159265',
    #                                     username='postgres', port=5432)

    conn_output = con.create_connection(server='localhost', database='trabalho_gbd', password='itix.123',
                                        username='postgres', port=5432)

    pd.set_option('display.max_columns', 110)
    pd.set_option('display.max_rows', 110)
    pd.set_option('display.width', 110)
    pd.options.display.max_rows = 110
    pd.options.display.max_columns = 110

    run(conn=conn_output)
