import pandas as pd
import gc
import psycopg2
from psycopg2 import sql

from sqlalchemy import create_engine
from models.configuracoes import empresaConfigurada

def Backup():
    # Define o tipo das colunas
    col_types = {
        'usuario': str,
        'codbarrastag':str,
        'codreduzido':str,
        'Endereco':str,
        'engenharia':str,
        'DataReposicao':str,
        'EngenhariaPai':str,
    'descricao':str,
    'epc':str,
    'StatusEndereco':str,
    'numeroop':str,
    'cor':str,
    'tamanho':str,
    'totalop':str,
    'natureza':str,
    'proveniencia':str,
    'codempresa':str,
    'usuario_inv':str,
    'usuario_carga':str,
    'datahora_carga':str,
    'resticao':str
        # Adicione outras colunas e seus tipos aqui
        # 'coluna2': int,
        # 'coluna3': float,
    }

    bac = pd.read_csv('tagsreposicao.csv', sep=';', dtype=col_types)
    bac.fillna('-',inplace=True)
    bac = bac.iloc[:1000]

    print(bac)

    # Definir parâmetros de conexão
    connection_params = {
        'dbname': 'Reposicao',
        'user': 'postgres',
        'password': 'Master100',
        'host': '192.168.0.183',
        'port': '5432'
    }

    # Chamar a função para inserir dados
    insert_into_db(bac, """"Reposicao".tagsreposicao""", connection_params)

def Funcao_Inserir(df_tags, tamanho, tabela, metodo):
    # Configurações de conexão ao banco de dados
    database = "Reposicao"
    user = "postgres"
    password = "Master100"
    host = "192.168.0.183"
    port = "5432"

    # Cria conexão ao banco de dados usando SQLAlchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    # Inserir dados em lotes
    chunksize = tamanho
    for i in range(0, len(df_tags), chunksize):
        df_tags.iloc[i:i + chunksize].to_sql(tabela, engine, if_exists=metodo, index=False, schema='Reposicao')

def insert_into_db(df, table_name, connection_params):
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()

        # Iterar sobre as linhas do DataFrame
        for idx, row in df.iterrows():
            columns = row.index.tolist()
            values = row.values.tolist()

            insert_statement = sql.SQL(
                'INSERT INTO {table} ({fields}) VALUES ({values})'
            ).format(
                table=sql.Identifier(table_name),
                fields=sql.SQL(',').join(map(sql.Identifier, columns)),
                values=sql.SQL(',').join(sql.Placeholder() * len(values))
            )

            cursor.execute(insert_statement, values)
            conn.commit()

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Erro ao inserir dados: {e}")


Backup()
