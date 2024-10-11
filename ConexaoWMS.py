import os

import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv, dotenv_values



def conexaoEngine():
    load_dotenv('db.env')
    db_name = os.getenv('host_')
    db_user = "postgres"
    db_password = os.getenv('db_name_WMS_Password')
    host = os.getenv('db_name_WMS_Password')
    portbanco = "5432"

    connection_string = f"postgresql://{db_user}:{db_password}@{host}:{portbanco}/{db_name}"
    return create_engine(connection_string)


def conexaoInsercao():
    load_dotenv('db.env')

    db_name = os.getenv('host_')
    db_user = "postgres"
    db_password = os.getenv('db_name_WMS_Password')
    db_host = os.getenv('host_')

    portbanco = "5432"

    return psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=portbanco)