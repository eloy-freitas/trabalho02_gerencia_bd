import pandas as pd
from pandasql import sqldf
import sqlalchemy as sqla
from sqlalchemy.types import Integer, String
import DW_TOOLS as dwt
import CONNECTION as con


def extract(conn, dim_exists):
    stg_dados = dwt.read_table(
        conn=conn,
        table_name='stg_disque_economia',
        schema='stage',
        columns=[
            'CODIGO_BAIRRO', 'DESCRICAO_BAIRRO',
            'CIDADE_ESTABELECIMENTO'
        ]
    )

    if dim_exists:
        query = """
            SELECT stg.* 
            FROM stg_dados stg
            LEFT JOIN dw.d_bairro dim
            ON stg."CODIGO_BAIRRO" = dim.cd_bairro
            WHERE dim.cd_bairro IS NULL;
        """

        stg_dados = sqldf(query, {'stg_dados': stg_dados}, conn.url)

    return stg_dados


def treat(conn, stg_dados, dim_exists):
    columns_select = ['CODIGO_BAIRRO', 'DESCRICAO_BAIRRO',
                      'CIDADE_ESTABELECIMENTO']

    columns_name = {
        'CODIGO_BAIRRO': 'cd_bairro',
        'DESCRICAO_BAIRRO': 'ds_bairro',
        'CIDADE_ESTABELECIMENTO': 'no_cidade'
    }

    dim_bairro = (
        stg_dados.
        filter(columns_select).
        rename(columns=columns_name).
        assign(
            ds_bairro=lambda x: x.ds_bairro.apply(
                lambda y: 'Não informado' if y is None else y),
            no_cidade=lambda x: x.no_cidade.apply(
                lambda y: 'CURITIBA' if y is None else y)).
        assign(
            ds_bairro=lambda x: x.ds_bairro.apply(
                lambda y: y.replace('      ', ' '))).
        assign(
            cd_bairro=lambda x: x.cd_bairro.astype('int64'),
            ds_bairro=lambda x: x.ds_bairro.astype(str),
            no_cidade=lambda x: x.no_cidade.astype(str)
        ).drop_duplicates()
    )

    if dim_exists:
        sk_max = dwt.find_sk(
            conn=conn,
            schema_name='dw',
            table_name='d_bairro',
            sk_name='sk_bairro'
        )

        dim_bairro.insert(0, "sk_bairro", range(sk_max, sk_max + len(dim_bairro)))
    else:
        dim_bairro.insert(0, "sk_bairro", range(1, 1 + len(dim_bairro)))

        dim_bairro = pd.DataFrame([[-1, -1, 'Não informado', 'Não informado'],
                                   [-2, -2, 'Não aplicável', 'Não aplicável'],
                                   [-3, -3, 'Desconhecido', 'Desconhecido']
                                   ], columns=dim_bairro.columns).append(dim_bairro)

    return dim_bairro


def load(dim_bairro, conn):
    data_types = {
        "sk_bairro": Integer(),
        "cd_bairro": Integer(),
        "ds_bairro": String(),
        "no_cidade": String()
    }

    (
        dim_bairro.
        astype('string').
        to_sql(
            name='d_bairro',
            con=conn,
            schema='dw',
            if_exists='append',
            index=False,
            dtype=data_types
        )
    )


def run(conn):
    fl_dim = sqla.inspect(conn).has_table(table_name='d_bairro', schema='dw')

    stg_bairro = extract(conn=conn, dim_exists=fl_dim)

    if not stg_bairro.empty:
        (
            treat(stg_dados=stg_bairro, conn=conn, dim_exists=fl_dim).
            pipe(load, conn=conn)
        )


if __name__ == '__main__':
    conn_output = con.create_connection(server='localhost', database='trabalho_gbd', password='itix.123',
                                        username='postgres', port=5432)

    #conn_output = con.create_connection(server='192.168.3.2', database='trabalho2', password='itix123',
    #                                    username='itix', port=5432)

    pd.set_option('display.max_columns', 110)
    pd.set_option('display.max_rows', 110)
    pd.set_option('display.width', 110)
    pd.options.display.max_rows = 110
    pd.options.display.max_columns = 110

    run(conn=conn_output)
