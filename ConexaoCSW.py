import jaydebeapi
import pandas as pd


def Conexao():
    conn = jaydebeapi.connect(
    'com.intersys.jdbc.CacheDriver',
    'jdbc:Cache://192.168.0.25:1972/CONSISTEM',
    {'user': '_SYSTEM', 'password': 'ccscache'},
    'CacheDB.jar'
)
    return conn
try:
    conn = Conexao()
    teste = pd.read_sql('select t.codigo ,t.descricao  from Fat.TipoDeNotaPadrao t ',conn)
    print(f'{teste}')
except:
    print('caiu a conexao')