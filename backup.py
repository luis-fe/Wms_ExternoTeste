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
        'codbarrastag': str,
        'codreduzido': str,
        'Endereco': str,
        'engenharia': str,
        'DataReposicao': str,
        'EngenhariaPai': str,
        'descricao': str,
        'epc': str,
        'StatusEndereco': str,
        'numeroop': str,
        'cor': str,
        'tamanho': str,
        'totalop': str,
        'natureza': str,
        'proveniencia': str,
        'codempresa': str,
        'usuario_inv': str,
        'usuario_carga': str,
        'datahora_carga': str,
        'resticao': str
    }

    bac = pd.read_csv('tagsreposicao.csv', sep=';', dtype=col_types)
    bac.fillna('-', inplace=True)
    bac = bac.iloc[10001:50001].reset_index(drop=True)

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
    insert_into_db(bac, 'tagsreposicao', connection_params)


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
                'INSERT INTO "Reposicao".{table} ({fields}) VALUES ({placeholders})'
            ).format(
                table=sql.Identifier(table_name),
                fields=sql.SQL(',').join(map(sql.Identifier, columns)),
                placeholders=sql.SQL(',').join(sql.Placeholder() * len(values))
            )

            cursor.execute(insert_statement, values)
            conn.commit()

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Erro ao inserir dados: {e}")
        if conn:
            conn.rollback()
        if cursor:
            cursor.close()
        if conn:
            conn.close()


Backup()
