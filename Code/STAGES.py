import pandas as pd
from Code.CONEXAO import create_connection_postgre
import time as t


def extract_data(path):
    tbl_dados = (
        pd.read_csv(
            filepath_or_buffer=path,
            delimiter=';',
            encoding='windows-1252',
            low_memory=False
        )
    )

    return tbl_dados


def load_data(conn_output, stg_name, file, date_range, tbl_exists):
    for date in date_range:
        print('reading file...')
        tbl = extract_data(f'Data/{date}{file}').loc[1:]
        print('loading on database...')
        tbl.to_sql(
            name=stg_name,
            con=conn_output,
            schema="stage",
            if_exists=tbl_exists,
            index=False
        )


if __name__ == '__main__':
    file = '_Disque_Economia_-_Base_de_Dados.csv'

    date_range = ['2020-01-15', '2020-02-15', '2020-03-15',
                  '2020-04-15', '2020-05-15', '2020-06-15',
                  '2020-07-15', '2020-05-15', '2020-09-15',
                  '2020-10-15', '2020-11-15', '2020-12-15']

    conn_dw = create_connection_postgre(
        server="192.168.3.2",
        database="trabalho2",
        username="itix",
        password="itix123",
        port="5432"
    )

    start = t.time()
    load_data(
        conn_output=conn_dw,
        stg_name='stg_pesquisa',
        file=file,
        date_range=date_range,
        tbl_exists='append'
    )
    exec_time = t.time() - start
    print(f'exec time = {exec_time}')
