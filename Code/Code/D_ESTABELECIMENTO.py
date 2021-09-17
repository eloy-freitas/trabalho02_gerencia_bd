import pandas as pd
from pandasql import sqldf
import sqlalchemy as sqla
from sqlalchemy.types import Integer, String

import DW_TOOLS as dwt
import CONNECTION as con


def extract(conn, dim_exists):
    stg_dados = dwt.read_table(conn=conn, table_name='stg_disque_economia', schema='stage',
                               columns=['CODIGO_ESTABELECIMENTO', 'CODIGO_ESTABELECIMENTO_FILIAL',
                                        'ENDERECO_ESTABELECIMENTO', 'NUMERO_ENDERECO_ESTABELECIMENTO',
                                        'COMPLEMENTO_ENDERECO_ESTABELECIMENTO', 'TELEFONE_ESTABELECIMENTO',
                                        'ESTABELECIMENTO_REDE', 'ESTABELECIMENTO_RAZAO_SOCIAL'])

    if dim_exists:
        query = """
            SELECT stg.* 
            FROM stg_dados stg 
            LEFT JOIN dw.d_estabelecimento de 
            ON (stg."CODIGO_ESTABELECIMENTO" = de.cd_rede::INTEGER 
            AND stg."CODIGO_ESTABELECIMENTO_FILIAL" = de.cd_filial::INTEGER) 
            WHERE de.cd_estabelecimento IS NULL
        """

        stg_dados = sqldf(query, {"stg_dados": stg_dados}, conn.url)

    return stg_dados


def treat(stg_dados, conn, dim_exists):
    columns_select = ["cd_estabelecimento", "cd_rede", "cd_filial", "ds_endereco", "nu_endereco",
                      "ds_complemento_endereco", "nu_telefone", "ds_rede", "ds_razao_social"]

    columns_name = {'CODIGO_ESTABELECIMENTO': 'cd_rede',
                    'CODIGO_ESTABELECIMENTO_FILIAL': 'cd_filial',
                    'ENDERECO_ESTABELECIMENTO': 'ds_endereco',
                    'NUMERO_ENDERECO_ESTABELECIMENTO': 'nu_endereco',
                    'COMPLEMENTO_ENDERECO_ESTABELECIMENTO': 'ds_complemento_endereco',
                    'TELEFONE_ESTABELECIMENTO': 'nu_telefone',
                    'ESTABELECIMENTO_REDE': 'ds_rede',
                    'ESTABELECIMENTO_RAZAO_SOCIAL': 'ds_razao_social'}

    dim_estab = (
        stg_dados.
        rename(columns=columns_name).
        assign(
            cd_rede=lambda x: x.cd_rede.apply(lambda y: f'{y:0>2}'),
            cd_filial=lambda x: x.cd_filial.apply(lambda y: f'{y:0>4}'),
            cd_estabelecimento=lambda x: x.cd_rede + x.cd_filial,
            nu_endereco=lambda x: x.nu_endereco.astype('int64'),
            nu_telefone=lambda x: x.nu_telefone.astype('string').str.replace('.0', "")
        ).drop_duplicates().
        filter(columns_select)
    )

    if dim_exists:
        sk_max = dwt.find_sk(conn=conn, schema_name='dw', table_name='d_estabelecimento', sk_name='sk_estabelecimento')
        dim_estab.insert(0, "sk_estabelecimento", range(sk_max, sk_max + len(dim_estab)))

    else:
        dim_estab.insert(0, "sk_estabelecimento", range(1, 1 + len(dim_estab)))

        dim_estab = pd.DataFrame([[-1, -1, -1, -1, 'Não informado', -1, 'Não informado', 'Não informado', 'Não informado',
                                   'Não informado'],
                                  [-2, -2, -2, -2, 'Não aplicável', -2, 'Não aplicável', 'Não aplicável', 'Não aplicável',
                                   'Não aplicável'],
                                  [-3, -3, -3, -3, 'Desconhecido', -3, 'Desconhecido', 'Desconhecido', 'Desconhecido',
                                   'Desconhecido']
                                  ], columns=dim_estab.columns).append(dim_estab)

    return dim_estab


def load(dim_estab, conn):

    data_types = {
        "sk_estabelecimento": Integer(),
        "cd_estabelecimento": String(),
        "cd_filial": String(),
        "cd_rede": String(),
        "ds_endereco": String(),
        "nu_endereco": String(),
        "ds_complemento_endereco": String(),
        "nu_telefone": String(),
        "ds_rede": String(),
        "ds_razao_social": String(),
    }

    (
        dim_estab.
        astype('string').
        to_sql(name='d_estabelecimento', con=conn, schema='dw', if_exists='append', index=False, dtype=data_types)
    )


def run(conn):

    fl_dim = sqla.inspect(conn).has_table(table_name='d_estabelecimento', schema='dw')

    stg_estab = extract(conn=conn, dim_exists=fl_dim)

    if not stg_estab.empty:
        (
            treat(stg_dados=stg_estab, conn=conn, dim_exists=fl_dim).
            pipe(load, conn=conn)
        )


if __name__ == '__main__':
    conn_output = con.create_connection(server='localhost', database='trabalho_gbd', password='14159265',
                                        username='postgres', port=5432)

    #conn_output = con.create_connection(server='192.168.3.2', database='trabalho2', password='itix123',
    #                                    username='itix', port=5432)

    pd.set_option('display.max_columns', 110)
    pd.set_option('display.max_rows', 110)
    pd.set_option('display.width', 110)
    pd.options.display.max_rows = 110
    pd.options.display.max_columns = 110

    run(conn=conn_output)
