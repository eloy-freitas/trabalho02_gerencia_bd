import pandas as pd
from datetime import datetime

import CONNECTION as con
import DW_TOOLS as dwt


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
                                        password='14159265',
                                        username='postgres',
                                        port=5432)

    create_stages(conn=conn_output)
