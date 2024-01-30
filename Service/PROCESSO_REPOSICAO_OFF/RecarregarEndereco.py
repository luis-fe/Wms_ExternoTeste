import pandas as pd
import ConexaoPostgreMPL
import ConexaoCSW
from Service.configuracoes import  empresaConfigurada
import psycopg2

'''''
        Nesse Documento é realizado o processo de Recarregar o endenreço com a caixa que foi reposta no processo 
    da reposicao OFF. 
        As ***Regras**** sao funcoes , utilizadas nos processo, para chegagem e validacao da excucao. 
        Já o  Processo é a funcao que execulta o servico de RECARREGAR ENDERECOS
'''''

### Regra 1 - Validar se o Endereço esta desoculpado

def EnderecoOculpado(endereco_Repor):
    conn = ConexaoPostgreMPL.conexao()
    consulta = 'select distinct codreduzido from "Reposicao"."Reposicao".tagsreposicao '\
                ' where "Endereco" = %s '
    consulta = pd.read_sql(consulta,conn,params=(endereco_Repor,))
    conn.close()

    ## avaliando se está vazio:
    if not consulta.empty:
        return pd.DataFrame([{'Mensagem':f'Endereco está cheio, com o sequinte sku {consulta["codreduzido"][0]}', 'status':False}])
    else:
        return pd.DataFrame([{'status':'OK! Pronto para usar'}])

## Regra 2 - Validar se a OP foi encerrada no CSW

def ValidarSituacaoOPCSW(numeroOP):
    emp = empresaConfigurada.EmpresaEscolhida() # Aqui aponta-se de qual empresa está requerendo a informacao
    conn = ConexaoCSW.Conexao()
    consulta = pd.read_sql('Select numeroop, situacao from tco.ordemprod where codempresa = ' +emp+
                           ' and numeroop = ' +"'"+numeroOP+"'",conn)
    ## avaliando a situacao da OP:
    if consulta['situacao'][0] == '2':
        return pd.DataFrame([{'status': 'OK! OP Baixada'}])
    else:
        return pd.DataFrame([{'status': 'Atencao! Op da caixa Nao esta baixada'}])



##### Os Processos abaixo exceculta o recarregamento dos enderecos
## Processo 1 - Retorna as Informacoes do Ncaixa selecionada:
def InfoCaixa(caixa):
    conn1 = ConexaoPostgreMPL.conexao() # Abrindo a Conexao com o Postgre WMS
    consulta = pd.read_sql('select rq.caixa, rq.codbarrastag , rq.codreduzido, rq.engenharia, rq.descricao, rq.natureza'
                            ', rq.codempresa, rq.cor, rq.tamanho, rq.numeroop, rq.usuario, rq."DataReposicao"  from "off".reposicao_qualidade rq  '
                            "where rq.caixa = %s ", conn1, params=(caixa,))
    conn1.close() # Fechado a Conexao com o Postgre WMS

    if consulta.empty:
        return pd.DataFrame([{'caixa':'vazia'}])

    else:

        return consulta


## Processo 2 - Buscando os EPC no Csw  (Esse estratégia se dá devido ao fato de ser uma tabela pessada no CSW, optou-se por utilizar nessa etapa)

def EPC_CSW_OP(consulta):# Passamos como parametro o dataframe com as informacoes da caixa que desejamos ,
#...  processo feito na api /api/RecarrearEndereco


    emp = empresaConfigurada.EmpresaEscolhida()  # Aqui aponta-se de qual empresa está requerendo a informacao

    # Passo1: Pesquisar em outra funcao um dataframe que retorna a coluna numeroop

    caixaNova = consulta.drop_duplicates(subset=['codbarrastag'])## Elimando as possiveis duplicatas


    # Passo2: Retirar do dataframe somente a coluna numeroop e elimina duplicatas, onde **ops1 correspond ao numero da OP extraido da consulta
    ops1 = caixaNova[['numeroop']]
    ops1 = ops1.drop_duplicates(subset=['numeroop'])

    # Passo 3: Transformar o dataFrame em lista e pesquisar a OP no CSW
    resultado = '({})'.format(', '.join(["'{}'".format(valor) for valor in ops1['numeroop']]))


    # Passo 4: Inciar uma conexao com o csw e buscar os EPC's tag a tag:
    conn2 = ConexaoCSW.Conexao() # Abrindo a Conexao com o CSW

    epc = pd.read_sql(
        'SELECT t.codBarrasTag AS codbarrastag, numeroOP as numeroop, (SELECT epc.id FROM Tcr_Rfid.NumeroSerieTagEPC epc WHERE epc.codTag = t.codBarrasTag) AS epc '
        "FROM tcr.SeqLeituraFase t WHERE t.codempresa = " + emp + " and t.numeroOP IN " + resultado, conn2)

    conn2.close() # Fechado a Conexao com o CSW
    epc = epc.drop_duplicates(subset=['codbarrastag']) # removendo possiveis duplicatas
    result = pd.merge(caixaNova, epc, on=('codbarrastag', 'numeroop'), how='left') # Aqui é feito um merge entre o codbarras do WMS x CSW


    return result

#### PROCESSO 3: Incementando a caixa e salvando dados na tabela **"Reposicao".tagsreposicao do WMS
def IncrementarCaixa(endereco, dataframe): # Informamos como parametro o enderco

    try: # é feito um tratamnto de excessao para barrar possiveis discrepancias na persistencia dos dados ao BANCO POSTGRE
        conn = ConexaoPostgreMPL.conexao()
        insert = 'insert into "Reposicao".tagsreposicao ("Endereco","codbarrastag","codreduzido",' \
                 '"engenharia","descricao","natureza","codempresa","cor","tamanho","numeroop","usuario", "proveniencia","DataReposicao") values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )'
        dataframe['proveniencia'] = 'Veio da Caixa: '+ dataframe['caixa'][0]

        cursor = conn.cursor()  # Crie um cursor para executar a consulta SQL

        values = [(endereco,row['codbarrastag'], row['codreduzido'], row['engenharia'], row['descricao']
                   , row['natureza'], row['codempresa'], row['cor'], row['tamanho'], row['numeroop'],
                   row['usuario'],row['proveniencia'],row['DataReposicao']) for index, row in dataframe.iterrows()]

        cursor.executemany(insert, values)
        conn.commit()  # Faça o commit da transação
        cursor.close()  # Feche o cursor

        return dataframe


    except psycopg2.Error as e:
        if 'duplicate key value violates unique constraint' in str(e):

            dataframe['mensagem'] = "codbarras ja existe em outra prateleira"

            return dataframe

        else:
            # Lidar com outras exceções que não são relacionadas à chave única
            print("Erro inesperado:", e)
            dataframe['mensagem'] = "Erro inesperado:", e

            return dataframe