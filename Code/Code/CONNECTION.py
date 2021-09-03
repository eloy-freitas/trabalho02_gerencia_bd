import sqlalchemy as sa


def create_connection(server, database, username, password, port):
    """
    Cria uma conexão via sqlAlchemy com o servidor especificado pelos parâmetros.

    :param server: servidor para a conexão.
    :param database: banco de dados.
    :param username: nome de usuário.
    :param password: senha do banco.
    :param port: porta da conexão.
    """
    conn = f'postgresql+psycopg2://{username}:{password}@{server}:{port}/{database}'
    return sa.create_engine(conn)
