import BuscasSqlCSW
import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd
import psycopg2
from psycopg2 import Error
import datetime
import pytz
from models.configuracoes import  empresaConfigurada


####### Nesse arquivo ApontarTag.py é realizado o processo (regra de negócios) para a bipagem das tag's nas Caixas no processo
#conhecido como "ReposicaoOFF". Esse processo ocorre no Setor de Garantia da Qualidade antes das Op's entrarem no esotque.

# Passo1: Funcoes que serao utilizadas na funcao principal:

## Funcao que devolve a data-Hora:
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    return hora_str

##Funcao que retorna se a tag ja foi ou não bipada em alguma caixa
def PesquisarSeTagJaFoiBipada(codbarrastag, caixa):
    conn = ConexaoPostgreMPL.conexao()
    consulta = pd.read_sql('select caixa  from "off".reposicao_qualidade rq'
                           ' where rq.codbarrastag = '+codbarrastag, conn )
    conn.close()

    if consulta.empty:
        return 1 # Optei por retorna valores ao inves de booleano
    else:
        caixaAntes = consulta['caixa'][0] #Aqui retornamos o numero da caixa que essa tag foi bipada

        if  caixaAntes == str(caixa): # Caso 1 : a caixa anterior é a mesma caixa que ele está tendando bipar

            return 2 # Optei por retorna valores ao inves de booleano

        else: # Caso 2 : a caixa anterior nao é a mesma que o usuario está tentando bipar:

            return consulta['caixa'][0] # Optei por retorna valores ao inves de booleano

## Funcao que insere os dados na tabela "Reposicao".off.reposicao_qualidade , persistindo os dados com as tags bipada na caixa
def InculirTagCaixa(dataframe):

        ## Removendo duplicatas do dataframe:
        dataframe = dataframe.drop_duplicates(subset=['codbarrastag'])  ## Elimando as possiveis duplicatas

        conn = ConexaoPostgreMPL.conexao()

        cursor = conn.cursor()  # Crie um cursor para executar a consulta SQL
        insert = 'insert into off.reposicao_qualidade (codbarrastag, codreduzido, engenharia, descricao, natureza, codempresa, cor, tamanho, numeroop, caixa, usuario, "DataReposicao", resticao)' \
                 ' values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )'

        values = [(row['codbarrastag'], row['codreduzido'], row['engenharia'], row['descricao']
                   , row['natureza'], row['codempresa'], row['cor'], row['tamanho'], row['numeroop'], row['caixa'],
                   row['usuario'], row['DataReposicao'], row['resticao']) for index, row in dataframe.iterrows()]
        cursor.executemany(insert, values)
        conn.commit()  # Faça o commit da transação
        cursor.close()  # Feche o cursor

        conn.close()

# Funcao de Estorna tag na caixa
def EstornarTag(codbarrastag):
    conn = ConexaoPostgreMPL.conexao()
    delete = 'delete from "off".reposicao_qualidade ' \
             'where codbarrastag  = '+codbarrastag
    cursor = conn.cursor()
    cursor.execute(delete,)
    conn.commit()
    cursor.close()
    conn.close()

    return pd.DataFrame([{'status':True,'Mensagem':'tag estornada! '}])



###Passo 2: Funcao de ApontarTag na Caixa:
# Parametros da funcao "codbarras" , "Numero da caixa", "empresa", "usuario" , "natureza" e "opçao de estornar":

def ApontarTagCaixa(codbarras, Ncaixa, empresa, usuario, natureza, estornar = False):
    conn = ConexaoPostgreMPL.conexao() # Chama o banco de dados Postgree
    codbarras = "'"+codbarras+"'"
    usuario = usuario.strip()
    emp = empresaConfigurada.EmpresaEscolhida()

    ## Realiza uma consulta sql para verificar se a tag existe na tabela
    # "filareposicaoof"
    pesquisa = pd.read_sql('select * from "Reposicao".off.filareposicaoof where codbarrastag= '+codbarras+' and '
                            'codempresa = '+empresa,conn)
    conn.close() # Encerra a chamada do banco de dados

    if pesquisa.empty:
    ## Caso nao for encontrado tag, é feito uma pesquisada direto do CSW para recuperar a tag , porem ela deve estar nas situacoes 0 ou 9:
        conn2 = ConexaoCSW.Conexao()

        consultaCsw = pd.read_sql(BuscasAvancadas.SqlBuscaTags(emp,codbarras), conn2)


        if not consultaCsw.empty :
            consultaCsw['usuario'] = usuario
            consultaCsw['caixa'] = Ncaixa
            consultaCsw['natureza'] = natureza
            consultaCsw['DataReposicao'] = obterHoraAtual()
            consultaCsw['resticao'] = 'veio csw'
            InculirTagCaixa(consultaCsw)
            conn2.close()
            return pd.DataFrame([{'status':True , 'Mensagem':'tag inserido !'}])

        else:

            conn2.close()
            print('tag nao existe na tablea filareposicaooff ')
            return pd.DataFrame([{'status': False, 'Mensagem': f'tag {codbarras} nao encontrada !'}])

    else: # Caso a tag for encontrada na fila de reposicao da qualidade:

        ## Aqui complementamos no DataFrame "pesquisa" as informacoes de usuario, Ncaixa, caixa e Data e Hora
        pesquisa['usuario'] = usuario
        pesquisa['caixa'] = Ncaixa
        pesquisa['natureza'] = natureza
        pesquisa['DataReposicao'] = obterHoraAtual()

        pesquisarSituacao = PesquisarSeTagJaFoiBipada(codbarras,Ncaixa) # Nessa etapa é conferida se a Tag ja foi ou nao bipada

        # Caso a tag ainda nao esteja bipada, aprova a insercao !:
        if pesquisarSituacao == 1 and estornar == False:
            InculirTagCaixa(pesquisa) #
            return pd.DataFrame([{'status':True , 'Mensagem':'tag inserido !'}])

        # Caso a tag ja tenha sido bipado, avisa ao usuario :
        elif pesquisarSituacao ==2 and estornar == False:
                    return pd.DataFrame([{'status': False, 'Mensagem': f'tag {codbarras} ja bipado nessa caixa, deseja estornar ?'}])
        elif estornar == False and pesquisarSituacao != 2:
                    return pd.DataFrame(
                        [{'status': False, 'Mensagem': f'tag {codbarras} ja bipado em outra  caixa de n°{pesquisarSituacao}, deseja estornar ?'}])
        else:
            estorno = EstornarTag(codbarras)
            return estorno