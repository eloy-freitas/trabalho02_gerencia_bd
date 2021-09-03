import pandas as pd


def create_stage_csv(stage_name, file_path, delimiter, conn_output):
    pd.read_csv(file_path, sep=delimiter, low_memory=False).to_sql(name=stage_name, con=conn_output, schema="stage",
                                                                   if_exists="replace")


def merge_input(left, right, left_on, right_on, surrogate_key, suff):
    dict_na = right.query(f"{surrogate_key} == -3").to_dict('index')

    df = (
        left.
            merge(right, how="left", left_on=left_on, right_on=right_on, suffixes=suff).
            fillna(dict_na[list(dict_na)[0]])
    )

    return df


def dict_to_str(dict):
    str = list()
    for value in dict.keys():
        temp_str = f'{value}" AS "{dict.get(value)}'
        str.append(temp_str)

    return str


def concat_cols(str):
    if isinstance(str, dict):
        str = dict_to_str(str)

    return '", "'.join(str)


def read_table(conn, schema, table_name, columns=None, where=None, distinct=False):
    if distinct:
        distinct_clause = "DISTINCT"
    else:
        distinct_clause = ""

    if where is None:
        where_clause = ""
    else:
        where_clause = f"WHERE {where}"

    if columns is None:
        query = f'SELECT {distinct_clause} * FROM "{schema}"."{table_name}" {where_clause}'
    else:
        query = f'SELECT {distinct_clause} "{concat_cols(columns)}" FROM "{schema}"."{table_name}" {where_clause}'

    response = pd.read_sql_query(query, conn)

    return response


def find_sk(conn, schema_name, table_name, sk_name):
    """
    Retorna um valor válido da SK da tabela consultada

    :param conn:
    :param schema_name: Schema onde se encontra a tabela
    :param table_name: Nome da tabela onde se encontra a coluna a ser verificada
    :param sk_name: Nome da coluna onde buscamos o id máximo.
    """
    result = conn.execute(f'SELECT MAX({sk_name}) FROM {schema_name}.{table_name}')
    index_sk = result.fetchone()[0] + 1

    return index_sk
