import jaydebeapi
import pandas as pd


def Conexao():
    conn = jaydebeapi.connect(
    'com.intersys.jdbc.CacheDriver',
    'jdbc:Cache://192.168.0.25:1972/CONSISTEM',
    {'user': '_system', 'password': 'ccscache'},
    'CacheDB.jar'
)
    return conn

try:
    conn = Conexao()
    teste = pd.read_sql('select top 1 * from tcp.tamanhos',conn)
    print('conectou')
except:
    print('caiu a conexao')