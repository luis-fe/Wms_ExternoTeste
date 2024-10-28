import gc
import psycopg2
from sqlalchemy import create_engine
from models.configuracoes import  empresaConfigurada


def conexao():

    db_name = "Reposicao"
    db_user = "postgres"
    db_password = "xDAWTEMLAvemoRTfDcUlSUMpcZUQASZv"
    if empresaConfigurada.EmpresaEscolhida() == '1':
        host = "autorack.proxy.rlwy.net"
    else:
        host = "autorack.proxy.rlwy.net"

    portbanco = "30687"

    return psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=host, port=portbanco)

def Funcao_Inserir (df_tags, tamanho,tabela, metodo):
    # Configurações de conexão ao banco de dados
    database = "Reposicao"
    user = "postgres"
    password = "xDAWTEMLAvemoRTfDcUlSUMpcZUQASZv"

    if empresaConfigurada.EmpresaEscolhida() == '1':
        host = "autorack.proxy.rlwy.net"
    else:
        host = "autorack.proxy.rlwy.net"

    port = "30687"

# Cria conexão ao banco de dados usando SQLAlchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    # Inserir dados em lotes
    chunksize = tamanho
    for i in range(0, len(df_tags), chunksize):
        df_tags.iloc[i:i + chunksize].to_sql(tabela, engine, if_exists=metodo, index=False , schema='Reposicao')

def Funcao_InserirOFF (df_tags, tamanho,tabela, metodo):
    # Configurações de conexão ao banco de dados
    database = "Reposicao"
    user = "postgres"
    password = "xDAWTEMLAvemoRTfDcUlSUMpcZUQASZv"
    if empresaConfigurada.EmpresaEscolhida() == '1':
        host = "autorack.proxy.rlwy.net"
    else:
        host = "autorack.proxy.rlwy.net"

    port = "30687"

# Cria conexão ao banco de dados usando SQLAlchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    # Inserir dados em lotes
    chunksize = tamanho
    for i in range(0, len(df_tags), chunksize):
        df_tags.iloc[i:i + chunksize].to_sql(tabela, engine, if_exists=metodo, index=False , schema='off')
def conexaoEngine():
    db_name = "Reposicao"
    db_user = "postgres"
    db_password = "xDAWTEMLAvemoRTfDcUlSUMpcZUQASZv"
    if empresaConfigurada.EmpresaEscolhida() == '1':
        host = "autorack.proxy.rlwy.net"
    else:
        host = "autorack.proxy.rlwy.net"
    portbanco = "30687"

    connection_string = f"postgresql://{db_user}:{db_password}@{host}:{portbanco}/{db_name}"
    return create_engine(connection_string)

def conexaoPCP():
    db_name = "PCP"
    db_user = "postgres"
    db_password = "xDAWTEMLAvemoRTfDcUlSUMpcZUQASZv"
    db_host = "autorack.proxy.rlwy.net"
    portbanco = "30687"

    return psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=portbanco)

def Funcao_InserirPCP (df_tags, tamanho,tabela, metodo):
    # Configurações de conexão ao banco de dados
    database = "PCP"
    user = "postgres"
    password = "xDAWTEMLAvemoRTfDcUlSUMpcZUQASZv"
    host = "autorack.proxy.rlwy.net"
    port = "30687"

# Cria conexão ao banco de dados usando SQLAlchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    # Inserir dados em lotes
    chunksize = tamanho
    for i in range(0, len(df_tags), chunksize):
        df_tags.iloc[i:i + chunksize].to_sql(tabela, engine, if_exists=metodo, index=False , schema='pcp')
    gc.collect()
