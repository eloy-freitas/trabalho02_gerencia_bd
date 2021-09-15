import pandas as pd
from datetime import datetime

import CONNECTION as con
import DW_TOOLS as dwt

import D_DATA as dd
import D_BAIRRO as db
import D_EMBALAGEM as dem
import D_ESTABELECIMENTO as des
import D_PRODUTO as dp
import D_UNIDADE_MEDIDA as dum
import F_PESQUISA as fp


def create_stages(conn):
    dwt.create_stage_csv(file_path="../Data/Disque_Economia.csv",
                         stage_name="stg_disque_economia",
                         delimiter=';',
                         conn_output=conn)

    dwt.create_stage_csv(file_path="../Data/Disque_Economia_Produto.csv",
                         stage_name="stg_produto",
                         delimiter=";",
                         conn_output=conn)


if __name__ == '__main__':
    conn_output = con.create_connection(server='localhost',
                                        database='trabalho_gbd',
                                        password='itix.123',
                                        username='postgres',
                                        port=5432)

    # create_stages(conn=conn_output)

    dd.run(conn=conn_output)
    db.run(conn=conn_output)
    dem.run(conn=conn_output)
    des.run(conn=conn_output)
    dp.run(conn=conn_output)
    dum.run(conn=conn_output)
    fp.run(conn=conn_output)
