'''Arquivo .py que Representa a Carga de pedidos que vem do ERP para o WMS'''

import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd
import pytz
import datetime
#from psycopg2 import sql

'''Função para obter a Data e Hora e Minuto atual do sistema'''
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str

'''Função GENERICA para criar agrupamento de pedidos'''
def criar_agrupamentos(grupo):
    return '/'.join(sorted(set(grupo)))

'''Funcao Para acessar o ERP atual e obter os tipos de Notas'''
def obter_notaCsw():
    conn = ConexaoCSW.Conexao() # Abrir conexao com o csw
    data = pd.read_sql(" select t.codigo ,t.descricao  from Fat.TipoDeNotaPadrao t ", conn)
    conn.close() # Encerrar conexao com o csw

    return data


'''Funcao utilizada na Api de Recarregar os Pedidos'''
def RecarregarPedidos(empresa):

        # 1 - Acionano a funcao de Excluir os pedidos nao encontrados
        tamanhoExclusao = ExcuindoPedidosNaoEncontrados(empresa)
        conn = ConexaoCSW.Conexao() # Abrir conexao com o csw
        SugestoesAbertos = pd.read_sql("SELECT codPedido||'-'||codsequencia as codPedido, codPedido as codPedido2, dataGeracao,  "
                                       "priorizar, vlrSugestao,situacaosugestao, dataFaturamentoPrevisto  from ped.SugestaoPed  "
                                       'WHERE codEmpresa ='+empresa+
                                       ' and situacaoSugestao =2',conn)


'''
################  MODULO PEDIDOS ##############################
Funcao de Excluir os Pedidos nao Encotrados
'''
def ExcuindoPedidosNaoEncontrados(empresa):

    # 1 - Encontrar os pedidos atuais no retorna e os pedidos de MKT

    conn = ConexaoCSW.Conexao() # Estabelecer a Conexao com o CSW
    # Obter os pedidos do Retorna
    retornaCsw = pd.read_sql("""SELECT  codPedido||'-'||codsequencia as codigopedido,
        (SELECT codTipoNota  FROM ped.Pedido p WHERE p.codEmpresa = e.codEmpresa and p.codpedido = e.codPedido) as codtiponota,
        'ok' as valida 
        FROM ped.SugestaoPed e WHERE e.codEmpresa = """+ str(empresa) +
        """ and e.dataGeracao > DATEADD(DAY, -120, GETDATE()) and situacaoSugestao = 2""", conn)

    pedidosMKT = """"
        SELECT codPedido||'-Mkt' as codigopedido,
        (SELECT p.codTipoNota  FROM ped.Pedido p WHERE p.codEmpresa = e.codEmpresa and p.codpedido = e.codPedido) as codtiponota,
        'ok' as valida 
        FROM ped.Pedido e
        WHERE e.codTipoNota = 1001 and situacao = 0 and codEmpresa = """+str(empresa)+""" and dataEmissao > DATEADD(DAY, -120, GETDATE())"""
    retornaCsw = pd.concat([retornaCsw,pedidosMKT])
    print(retornaCsw)
    conn.close() # Encerrar a Conexao com o CSW


    # 2 - Obtendo a TABELA "filaseparacaopedidos" e "PEDIDOSSKU"  do WMS

    conn2 = ConexaoPostgreMPL.conexao()
    validacao = pd.read_sql(
        "select codigopedido, codtiponota " 
                                              ' from "Reposicao".filaseparacaopedidos f ', conn2)

    validacaoPedidossku = pd.read_sql(
        "select codpedido as codigopedido " 
                                              ' from "Reposicao".pedidossku f ', conn2)

    validacao = pd.merge(validacao, retornaCsw, on=['codigopedido','codtiponota'], how='left') #Merge entre as tabelas
    validacao.fillna('-', inplace=True)
    validacao = validacao[validacao['valida'] == '-'].reset_index()

    tamanho = validacao['codigopedido'].size

    validacao2 = pd.merge(validacaoPedidossku, retornaCsw, on=['codigopedido'], how='left')
    validacao2.fillna('-', inplace=True)

    validacao2 = validacao2[validacao2['valida'] == '-']
    validacao2 = validacao2.reset_index()
    tamanho2 = validacao2['codigopedido'].size


    for i in range(tamanho):

        pedido = validacao['codigopedido'][i]
        tiponota = validacao['codtiponota'][i]


        # Acessando os pedidos com enderecos reservados
        queue = 'Delete from "Reposicao".filaseparacaopedidos '\
                            " where codigopedido = %s and codtiponota = %s"


        cursor = conn2.cursor()
        cursor.execute(queue,(pedido,tiponota))
        conn2.commit()

    for i in range(tamanho2):
        pedido = validacao2['codigopedido'][i]

        # Acessando os pedidos com enderecos reservados
        queue = 'Delete from "Reposicao".pedidossku ' \
                " where codpedido = %s "

        cursor = conn2.cursor()
        cursor.execute(queue, (pedido,))
        conn2.commit()




    conn2.close()



    return tamanho