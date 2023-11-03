import jaydebeapi
import pandas as pd

# Função de conectar com o CSW, com 2 opções de conexao:
def Conexao():
   # try:
        conn = jaydebeapi.connect(
    'com.intersys.jdbc.CacheDriver',
    'jdbc:Cache://192.168.0.25:1972/CONSISTEM?loginTimeout=10',
    {'user': 'root', 'password': 'ccscache'},
    'CacheDB.jar'
    )
        return conn
  #  except:
   #     conn2 = Conexao2()
    #    return conn2

def Conexao2():
    conn = jaydebeapi.connect(
    'com.intersys.jdbc.CacheDriver',
    'jdbc:Cache://192.168.0.25:1972/CONSISTEM?loginTimeout=10',
    {'user': '_SYSTEM', 'password': 'ccscache'},
    'CacheDB_root.jar'
)
    return conn
def ConexaoExterna2():
    conn = jaydebeapi.connect(
    'com.intersys.jdbc.CacheDriver',
    'jdbc:Cache://187.32.10.129:1972/CONSISTEM',
    {'user': 'root', 'password': 'ccscache'},
    'CacheDB_root.jar'
)
    return conn

def obter_notaCsw():
    conn = Conexao()
    data = pd.read_sql(" select t.codigo ,t.descricao  from Fat.TipoDeNotaPadrao t ", conn)
    conn.close()

    return data

def VerificarConexao():
    try:
        connPrincipal = jaydebeapi.connect(
                                    'com.intersys.jdbc.CacheDriver',
                                    'jdbc:Cache://192.168.0.25:1972/CONSISTEM',
                                    {'user': '_SYSTEM', 'password': 'ccscache'},
                                    'CacheDB.jar'
                                    )
        teste = pd.read_sql(" select t.codigo ,t.descricao  from Fat.TipoDeNotaPadrao t ", connPrincipal)
        data = pd.DataFrame([{'Mensagem':'Conexao com CSW normal com o servidor 192.168.0.25:1972 _system ','teste':'csw'}])
    except:
        data = pd.DataFrame([{'Mensagem': 'falha na conexao com o servidor 192.168.0.25:1972 _system ','teste':'csw'}])

    try:
        connContigencia = Conexao2()
        teste2 = pd.read_sql(" select t.codigo ,t.descricao  from Fat.TipoDeNotaPadrao t ", connContigencia)
        data2 = pd.DataFrame([{'Mensagem2':'Conexao com CSW normal com o servidor 192.168.0.25:1972 root ','teste':'csw'}])
    except:
        data2 = pd.DataFrame([{'Mensagem2': 'falha na conexao com o servidor 192.168.0.25:1972 root','teste':'csw'}])



    data = pd.merge(data,data2,on='teste')

    return data

def pesquisaTagCSW(codbarras):
    try:
        codbarras = "'"+codbarras+"'"
        conn = Conexao()
        data = pd.read_sql(" select codBarrasTag , codNaturezaAtual , situacao  FROM Tcr.TagBarrasProduto p"
                           " WHERE p.codBarrasTag = "+ codbarras , conn)
        conn.close()
        data['stauts conexao'] = True
        return data
    except:
        return pd.DataFrame([{'stauts conexao': False}])


####### TESTE NO INICIO DA APLICACAO,
'''''
try:
    teste = obter_notaCsw()
    print(f'{teste}')
except:
    print('caiu a conexao')
'''''