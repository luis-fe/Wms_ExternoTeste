import pandas as pd
import gc
import psycopg2
from sqlalchemy import create_engine
from models.configuracoes import empresaConfigurada

def Backup():
    bac = pd.read_csv('tagsreposicao.csv', sep=';')
    bac['codbarrastag'] = bac['codbarrastag'].astype(str)
    Funcao_Inserir(bac, 75000, 'tagsreposicao', 'append')

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
        try:
            df_tags.iloc[i:i + chunksize].to_sql(tabela, engine, if_exists=metodo, index=False, schema='Reposicao')
        except Exception as e:
            print(f"Erro ao inserir o bloco {i // chunksize + 1}: {e}")
            continue

Backup()