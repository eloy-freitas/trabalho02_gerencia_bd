import pandas as pd
from datetime import datetime

import CONNECTION as con
import DW_TOOLS as dwt


if __name__ == '__main__':
    conn_output = con.create_connection(server='localhost', database='trabalho_gbd', password='itix.123',
                                        username='postgres', port=5432)

    dwt.create_stage_csv(file_path="../Data/dados_julho.csv", stage_name="stg_disque_economia", delimiter=';',
                         conn_output=conn_output)
