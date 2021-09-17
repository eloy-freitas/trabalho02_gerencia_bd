from datetime import datetime

import pandas as pd
from sqlalchemy import inspect
from sqlalchemy.types import Float, Date, DateTime, Integer

import CONNECTION as con
import DW_TOOLS as dwt


def extract(conn, date):

    stg_pesquisa = (
        dwt.read_table(
            conn=conn,
            schema='stage',
            table_name='stg_disque_economia',
            columns=["DATA_PESQUISA", "PRECO_PESQUISADO", "CODIGO_ESTABELECIMENTO", "CODIGO_BAIRRO", "CODIGO_PRODUTO",
                     "CODIGO_CATEGORIA_PRODUTO", "CODIGO_TIPO_UNIDADE_MEDIDA_PRODUTO", "CODIGO_TIPO_EMBALAGEM_PRODUTO",
                     "CODIGO_ESTABELECIMENTO_FILIAL"],
            where=f'"DATA_PESQUISA" > \'{date}\'').
        assign(
            DATA_PESQUISA=lambda x: pd.to_datetime(x.DATA_PESQUISA, format="%d/%m/%Y"),
            CODIGO_PRODUTO=lambda x: x.CODIGO_PRODUTO.astype('int64'),
            CODIGO_CATEGORIA_PRODUTO=lambda x: x.CODIGO_CATEGORIA_PRODUTO.astype('int64'),
            CODIGO_ESTABELECIMENTO=lambda x: x.CODIGO_ESTABELECIMENTO.astype('int64'),
            CODIGO_ESTABELECIMENTO_FILIAL=lambda x: x.CODIGO_ESTABELECIMENTO_FILIAL.astype('int64'),
            CODIGO_BAIRRO=lambda x: x.CODIGO_BAIRRO.astype('int64'),
            CODIGO_TIPO_UNIDADE_MEDIDA_PRODUTO=lambda x: x.CODIGO_TIPO_UNIDADE_MEDIDA_PRODUTO.astype('int64'),
            CODIGO_TIPO_EMBALAGEM_PRODUTO=lambda x: x.CODIGO_TIPO_EMBALAGEM_PRODUTO.astype('int64')
        )
    )

    if not stg_pesquisa.empty:

        d_data = (
            dwt.read_table(
                conn=conn,
                schema='dw',
                table_name='d_data',
                columns=['sk_data', 'dt_referencia']
            ).
            assign(
                dt_referencia=lambda x: pd.to_datetime(x.dt_referencia, format="%Y-%m-%d")
            )
        )

        d_produto = (
            dwt.read_table(
                conn=conn,
                schema='dw',
                table_name='d_produto',
                columns=['sk_produto', 'cd_produto', 'cd_categoria']
            ).assign(
                cd_produto=lambda x: x.cd_produto.astype('int64'),
                cd_categoria=lambda x: x.cd_categoria.astype('int64')
            )
        )

        d_bairro = (
            dwt.read_table(
                conn=conn,
                schema='dw',
                table_name='d_bairro',
                columns=['sk_bairro', 'cd_bairro']
            )
        )

        d_estabelecimento = (
            dwt.read_table(
                conn=conn,
                schema='dw',
                table_name='d_estabelecimento',
                columns=['sk_estabelecimento', 'cd_rede', 'cd_filial']
            ).assign(
                cd_rede=lambda x: x.cd_rede.astype('int64'),
                cd_filial=lambda x: x.cd_filial.astype('int64')
            )
        )

        d_embalagem = (
            dwt.read_table(
                conn=conn,
                schema='dw',
                table_name='d_embalagem',
                columns=['sk_embalagem', 'cd_embalagem']
            )
        )

        d_unidade_medida = (
            dwt.read_table(
                conn=conn,
                schema='dw',
                table_name='d_unidade_medida',
                columns=['sk_unidade_medida', 'cd_unidade_medida']
            )
        )

        stg_pesquisa = (
            stg_pesquisa.
            pipe(
                dwt.merge_input,
                right=d_data,
                left_on="DATA_PESQUISA",
                right_on="dt_referencia",
                suff=["-1", "_2"],
                surrogate_key="sk_data").
            pipe(
                dwt.merge_input,
                right=d_produto,
                left_on=["CODIGO_PRODUTO", "CODIGO_CATEGORIA_PRODUTO"],
                right_on=["cd_produto", "cd_categoria"],
                suff=["_3", "_4"],
                surrogate_key="sk_produto").
            pipe(
                dwt.merge_input,
                right=d_bairro,
                left_on="CODIGO_BAIRRO",
                right_on="cd_bairro",
                suff=["_5", "_6"],
                surrogate_key="sk_bairro").
            pipe(
                dwt.merge_input,
                right=d_estabelecimento,
                left_on=["CODIGO_ESTABELECIMENTO", "CODIGO_ESTABELECIMENTO_FILIAL"],
                right_on=["cd_rede", "cd_filial"],
                suff=["_7", "_8"],
                surrogate_key="sk_estabelecimento").
            pipe(
                dwt.merge_input,
                right=d_unidade_medida,
                left_on="CODIGO_TIPO_UNIDADE_MEDIDA_PRODUTO",
                right_on="cd_unidade_medida",
                suff=["_9", "_10"],
                surrogate_key="sk_unidade_medida").
            pipe(
                dwt.merge_input,
                right=d_embalagem,
                left_on="CODIGO_TIPO_EMBALAGEM_PRODUTO",
                right_on="cd_embalagem",
                suff=["_11", "_12"],
                surrogate_key="sk_embalagem")
        )

    return stg_pesquisa


def treat(tbl_pesquisa):
    columns_select = [
        'sk_data', 'sk_produto', 'sk_bairro',
        'sk_estabelecimento', 'sk_unidade_medida',
        'sk_embalagem', 'dt_pesquisa', 'vl_preco_pesquisado', 'dt_carga'
    ]

    columns_names = {
        'DATA_PESQUISA': 'dt_pesquisa',
        'PRECO_PESQUISADO': 'vl_preco_pesquisado'
    }

    f_pesquisa = (
        tbl_pesquisa.
        rename(columns=columns_names).
        filter(columns_select).
        assign(
            dt_carga=pd.to_datetime('today', format='%Y-%m-%d'),
            vl_preco_pesquisado=lambda x: x.vl_preco_pesquisado.str.replace(',', '.')
        )
    )

    return f_pesquisa


def load(f_pesquisa, conn):
    data_type = {
        'sk_data': Integer(),
        'sk_produto': Integer(),
        'sk_bairro': Integer(),
        'sk_estabelecimento': Integer(),
        'sk_unidade_medida': Integer(),
        'sk_embalagem': Integer(),
        'dt_pesquisa': Date(),
        'vl_preco_pesquisado': Float(),
        'dt_carga': DateTime()
    }
    (
        f_pesquisa.
        to_sql(
            con=conn,
            name='f_pesquisa',
            schema="dw",
            if_exists='append',
            chunksize=100,
            index=False,
            dtype=data_type
        )
    )


def run(conn):

    if inspect(conn).has_table(table_name='f_pesquisa', schema='dw'):
        result = conn.execute('SELECT MAX("dt_pesquisa") FROM dw."f_pesquisa"')
        dt_max = str(result.fetchone()[0])
    else:
        dt_max = '1900-01-01'

    load_date = datetime.strptime(dt_max, "%Y-%m-%d")

    tbl_fact = extract(conn, date=load_date)

    if tbl_fact.shape[0] != 0:
        (
            treat(tbl_pesquisa=tbl_fact).
            pipe(load, conn=conn)
        )


if __name__ == '__main__':
    conn_output = con.create_connection(server='localhost', database='trabalho_gbd', password='14159265',
                                        username='postgres', port=5432)

    #conn_output = con.create_connection(server='localhost', database='trabalho_gbd', password='itix.123',
    #                                    username='postgres', port=5432)
    pd.set_option('display.max_columns', 110)
    pd.set_option('display.max_rows', 110)
    pd.set_option('display.width', 110)
    pd.options.display.max_rows = 110
    pd.options.display.max_columns = 110

    run(conn=conn_output)