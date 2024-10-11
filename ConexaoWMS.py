import psycopg2
from sqlalchemy import create_engine



def conexaoEngine():
    db_name = "Reposicao"
    db_user = "postgres"
    db_password = "Master100"
    host = "localhost"
    portbanco = "5432"

    connection_string = f"postgresql://{db_user}:{db_password}@{host}:{portbanco}/{db_name}"
    return create_engine(connection_string)