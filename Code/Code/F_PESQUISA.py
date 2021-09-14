import time as t
import pandas as pd
from sqlalchemy.types import Integer, String, Float, DateTime
from pandasql import sqldf
import CONNECTION as con
import DW_TOOLS as dwt


def extract(conn):

    stg_pesquisa = (
        dwt.read_table(
            conn=conn,
            schema='stage',
            table_name='stg_disque_economia',
            columns=["DATA_PESQUISA", "PRECO_PESQUISADO", "CODIGO_ESTABELECIMENTO",
                     "CODIGO_BAIRRO", "CODIGO_PRODUTO", "CODIGO_TIPO_UNIDADE_MEDIDA_PRODUTO",
                     "CODIGO_TIPO_EMBALAGEM_PRODUTO"]).
        assign(
            CODIGO_PRODUTO=lambda x: x.CODIGO_PRODUTO.astype('int64'),
            CODIGO_ESTABELECIMENTO=lambda x: x.CODIGO_ESTABELECIMENTO.astype('int64'),
            CODIGO_BAIRRO=lambda x: x.CODIGO_BAIRRO.astype('int64'),
            CODIGO_TIPO_UNIDADE_MEDIDA_PRODUTO=lambda x: x.CODIGO_TIPO_UNIDADE_MEDIDA_PRODUTO.astype('int64'),
            CODIGO_TIPO_EMBALAGEM_PRODUTO=lambda x: x.CODIGO_TIPO_EMBALAGEM_PRODUTO.astype('int64')
        )
    )

    d_produto = (
        dwt.read_table(
            conn=conn,
            schema='dw',
            table_name='d_produto',
            columns=['sk_produto', 'cd_produto']
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
            columns=['sk_estabelecimento', 'cd_estabelecimento']
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

    tbl_pesquisa = (
        stg_pesquisa.
        pipe(
            dwt.merge_input,
            right=d_produto,
            left_on="CODIGO_PRODUTO",
            right_on="cd_produto",
            suff=["_1", "_2"],
            surrogate_key="sk_produto").
        pipe(
            dwt.merge_input,
            right=d_bairro,
            left_on="CODIGO_BAIRRO",
            right_on="cd_bairro",
            suff=["_3", "_4"],
            surrogate_key="sk_bairro").
        pipe(
            dwt.merge_input,
            right=d_estabelecimento,
            left_on="CODIGO_ESTABELECIMENTO",
            right_on="cd_estabelecimento",
            suff=["_5", "_6"],
            surrogate_key="sk_estabelecimento").
        pipe(
            dwt.merge_input,
            right=d_unidade_medida,
            left_on="CODIGO_TIPO_UNIDADE_MEDIDA_PRODUTO",
            right_on="cd_unidade_medida",
            suff=["_7", "_8"],
            surrogate_key="sk_unidade_medida").
        pipe(
            dwt.merge_input,
            right=d_embalagem,
            left_on="CODIGO_TIPO_EMBALAGEM_PRODUTO",
            right_on="cd_embalagem",
            suff=["_7", "_8"],
            surrogate_key="sk_embalagem")
    )

    return tbl_pesquisa


def treat(tbl_pesquisa):
    columns_select = [
        'sk_produto', 'sk_bairro',
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
            dt_pesquisa=lambda x: x.dt_pesquisa.astype('datetime64[ns]'),
            vl_preco_pesquisado=lambda x: x.vl_preco_pesquisado.apply(
                lambda y: y.replace(',', '.')
            )).
        assign(
            vl_preco_pesquisado=lambda x: x.vl_preco_pesquisado.astype('float64')
        )
    )

    return f_pesquisa


def load(f_pesquisa, conn):
    data_type = {
        'sk_produto': Integer(),
        'sk_bairro': Integer(),
        'sk_estabelecimento': Integer(),
        'sk_unidade_medida': Integer(),
        'sk_embalagem': Integer(),
        'dt_pesquisa': DateTime(),
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
    tbl_fact = extract(conn)

    if tbl_fact.shape[0] != 0:
        (
            treat(tbl_pesquisa=tbl_fact).
            pipe(load, conn=conn)
        )



if __name__ == '__main__':
    conn_output = con.create_connection(server='localhost', database='trabalho_gbd', password='14159265',
                                        username='postgres', port=5432)

    #conn_output = con.create_connection(server='192.168.3.2', database='trabalho2', password='14159265',
     #                                  username='itix', port=5432)
    pd.set_option('display.max_columns', 110)
    pd.set_option('display.max_rows', 110)
    pd.set_option('display.width', 110)
    pd.options.display.max_rows = 110
    pd.options.display.max_columns = 110

    run(conn=conn_output)
