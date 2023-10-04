import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd
import pytz
import datetime
from psycopg2 import sql


def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str

def criar_agrupamentos(grupo):
    return '/'.join(sorted(set(grupo)))

def obter_notaCsw():
    conn = ConexaoCSW.Conexao()
    data = pd.read_sql(" select t.codigo ,t.descricao  from Fat.TipoDeNotaPadrao t ", conn)
    conn.close()

    return data

def RecarregarPedidos(empresa):

    #try:
        tamanhoExclusao = ExcuindoPedidosNaoEncontrados(empresa)
        conn = ConexaoCSW.Conexao()
        SugestoesAbertos = pd.read_sql("SELECT codPedido||'-'||codsequencia as codPedido, codPedido as codPedido2, dataGeracao,  "
                                       "priorizar, vlrSugestao,situacaosugestao, dataFaturamentoPrevisto  from ped.SugestaoPed  "
                                       'WHERE codEmpresa ='+empresa+
                                       ' and situacaoSugestao =2',conn)

        PedidosSituacao = pd.read_sql("select DISTINCT p.codPedido||'-'||p.codSequencia as codPedido , 'Em Conferencia' as situacaopedido  FROM ped.SugestaoPedItem p "
                                      'join ped.SugestaoPed s on s.codEmpresa = p.codEmpresa and s.codPedido = p.codPedido '
                                      'WHERE p.codEmpresa ='+empresa+
                                      ' and s.situacaoSugestao = 2', conn)

        SugestoesAbertos = pd.merge(SugestoesAbertos, PedidosSituacao, on='codPedido', how='left')

        CapaPedido = pd.read_sql('select top 100000 codPedido as codPedido2, codCliente, '
                                 '(select c.nome from fat.Cliente c WHERE c.codEmpresa = 1 and p.codCliente = c.codCliente) as desc_cliente, '
                                 '(select r.nome from fat.Representante  r WHERE r.codEmpresa = 1 and r.codRepresent = p.codRepresentante) as desc_representante, '
                                 '(select c.nomeCidade from fat.Cliente  c WHERE c.codEmpresa = 1 and c.codCliente = p.codCliente) as cidade, '
                                 '(select c.nomeEstado from fat.Cliente  c WHERE c.codEmpresa = 1 and c.codCliente = p.codCliente) as estado, '
                                 ' codRepresentante , codTipoNota, CondicaoDeVenda as condvenda  from ped.Pedido p  '
                                       ' WHERE p.codEmpresa ='+empresa+
                                 ' order by codPedido desc ',conn)
        SugestoesAbertos = pd.merge(SugestoesAbertos,CapaPedido,on= 'codPedido2', how = 'left')
        SugestoesAbertos.rename(columns={'codPedido': 'codigopedido', 'vlrSugestao': 'vlrsugestao'
            , 'dataGeracao': 'datageracao','situacaoSugestao':'situacaosugestao','dataFaturamentoPrevisto':'datafaturamentoprevisto',
            'codCliente':'codcliente', 'codRepresentante':'codrepresentante','codTipoNota':'codtiponota'}, inplace=True)
        tiponota = obter_notaCsw()
        tiponota['codigo'] = tiponota['codigo'].astype(str)
        tiponota.rename(columns={'codigo': 'codtiponota','descricao':'desc_tiponota'}, inplace=True)

        tiponota['desc_tiponota'] = tiponota['codtiponota']+'-'+tiponota['desc_tiponota']
        SugestoesAbertos = pd.merge(SugestoesAbertos, tiponota, on='codtiponota', how='left')
        condicaopgto = pd.read_sql("SELECT v.codEmpresa||'||'||codigo as condvenda, descricao as  condicaopgto FROM cad.CondicaoDeVenda v",conn)
        SugestoesAbertos = pd.merge(SugestoesAbertos, condicaopgto, on='condvenda', how='left')

        SugestoesAbertos.fillna('-', inplace=True)

        # Procurando somente pedidos novos a incrementar
        conn2 = ConexaoPostgreMPL.conexao()
        validacao = pd.read_sql('select codigopedido, '+"'ok'"+' as "validador"  from "Reposicao".filaseparacaopedidos f ', conn2)

        SugestoesAbertos2 = pd.merge(SugestoesAbertos, validacao, on='codigopedido', how='left')

        SugestoesAbertos2 = SugestoesAbertos2.loc[SugestoesAbertos2['validador'].isnull()]
        SugestoesAbertos2.drop('validador', axis=1, inplace=True)


        tamanho = SugestoesAbertos2['codigopedido'].size
        dataHora = obterHoraAtual()
        SugestoesAbertos2['datahora'] = dataHora
        # Contar o número de ocorrências de cada valor na coluna 'coluna'
        contagem = SugestoesAbertos2['codcliente'].value_counts()

        # Criar uma nova coluna 'contagem' no DataFrame com os valores contados
        SugestoesAbertos2['contagem'] = SugestoesAbertos2['codcliente'].map(contagem)
        # Aplicar a função de agrupamento usando o método groupby
        SugestoesAbertos2['agrupamentopedido'] = SugestoesAbertos2.groupby('codcliente')['codigopedido'].transform(
            criar_agrupamentos)
        SugestoesAbertos2.drop('codPedido2', axis=1, inplace=True)



        if tamanho >= 1:
            ConexaoPostgreMPL.Funcao_Inserir(SugestoesAbertos2, tamanho, 'filaseparacaopedidos', 'append')

            SugestoesAbertos2 = SugestoesAbertos2.reset_index()
            for i in range(tamanho):
                pedidox = SugestoesAbertos2['codigopedido'][i]
                DetalhandoPedidoSku(empresa, pedidox)


            status = Verificando_RetornaxConferido(empresa)
            return pd.DataFrame([{'Mensagem:':f'foram inseridos {tamanho} pedidos!','Excluido':f'{tamanhoExclusao} pedidos removidos pois ja foram faturados ',
                                  'Pedidos Atualizados para Retorna':f'{status}'}])
        else:
            status = Verificando_RetornaxConferido(empresa)
            return pd.DataFrame([{'Mensagem:':f'nenhum pedido atualizado','Excluido':f'{tamanhoExclusao} pedidos removidos pois ja foram faturados ',
                                  'Pedidos Atualizados para Retorna':f'{status}'}])
   # except:
    #    return pd.DataFrame([{'Mensagem:': 'perdeu conexao com csw'}])


def ExcuindoPedidosNaoEncontrados(empresa):

    conn = ConexaoCSW.Conexao()

    retornaCsw = pd.read_sql(
        "SELECT  codPedido||'-'||codsequencia as codigopedido,  "
        " (SELECT codTipoNota  FROM ped.Pedido p WHERE p.codEmpresa = e.codEmpresa and p.codpedido = e.codPedido) as codtiponota, "
        "'ok' as valida "
        " FROM ped.SugestaoPed e "
        ' WHERE e.codEmpresa =' + empresa +
        " and e.dataGeracao > '2023-01-01' and situacaoSugestao = 2", conn)

    conn.close()

    conn2 = ConexaoPostgreMPL.conexao()
    validacao = pd.read_sql(
        "select codigopedido, codtiponota " 
                                              ' from "Reposicao".filaseparacaopedidos f ', conn2)

    validacao = pd.merge(validacao, retornaCsw, on=['codigopedido','codtiponota'], how='left')
    validacao.fillna('-', inplace=True)

    validacao = validacao[validacao['valida'] == '-']
    validacao = validacao.reset_index()
    tamanho = validacao['codigopedido'].size


    for i in range(tamanho):

        pedido = validacao['codigopedido'][i]
        tiponota = validacao['codtiponota'][i]


        # Acessando os pedidos com enderecos reservados
        queue = 'Delete from "Reposicao".filaseparacaopedidos '\
                            " where codigopedido = %s and codtiponota = %s"

        cursor = conn2.cursor()
        cursor.execute(queue,(pedido,tiponota))
        conn2.commit()




    conn2.close()



    return tamanho

def Verificando_RetornaxConferido(empresa):
    conn = ConexaoCSW.Conexao()

    retornaCsw = pd.read_sql(
        "SELECT  i.codPedido, sum(i.qtdePecasConf) as conf , i.codSequencia  "
        " from ped.SugestaoPedItem i  "
        ' WHERE i.codEmpresa =' + empresa+
        ' group by i.codPedido, i.codSequencia', conn)



    retornaCsw['codPedido'] = retornaCsw['codPedido'] +'-'+retornaCsw['codSequencia']
    retornaCsw = retornaCsw[retornaCsw['conf'] == 0]




    # Transformando a coluna 'codPedido' em uma lista separada por vírgulas
    codPedido_lista = retornaCsw['codPedido'].str.cat(sep=',')

    conn.close()

    # Conectar ao banco de dados PostgreSQL
    conn_pg = ConexaoPostgreMPL.conexao()

    # Construir a consulta SQL parametrizada com psycopg2.sql

    values = sql.SQL(',').join(map(sql.Literal, codPedido_lista.split(',')))
    query = sql.SQL('UPDATE "Reposicao".filaseparacaopedidos SET situacaopedido = '
                    "'No Retorna' WHERE situacaopedido <> 'No Retorna' and codigopedido IN ({})").format( values)

    # Executar a consulta SQL
    cursor = conn_pg.cursor()

    
    cursor.execute(query)
    # Obter o número de linhas afetadas
    num_linhas_afetadas = cursor.rowcount
    conn_pg.commit()
    cursor.close()
    query2 = sql.SQL('UPDATE "Reposicao".filaseparacaopedidos SET situacaopedido = '
                    "'Em Conferencia' WHERE  codigopedido not IN ({})").format(values)
    cursor2 = conn_pg.cursor()

    cursor2.execute(query2)

    conn_pg.commit()
    cursor2.close()

    conn_pg.close()

    return num_linhas_afetadas

def DetalhandoPedidoSku(empresa, pedido):
    conncsw = ConexaoCSW.Conexao()
    pedido = pedido.split('-')[0]

    SugestoesAbertos = pd.read_sql(
        'select s.codPedido as codpedido, s.codSequencia , s.produto, s.qtdeSugerida as qtdesugerida , s.qtdePecasConf as qtdepecasconf  '
        'from ped.SugestaoPedItem s  '
        'WHERE s.codEmpresa =' + empresa+
        ' and s.codPedido = '"'"+pedido+"'", conncsw)

    conncsw.close()

    SugestoesAbertos['necessidade'] = SugestoesAbertos['qtdesugerida'] - SugestoesAbertos['qtdepecasconf']
    SugestoesAbertos['codpedido'] = SugestoesAbertos['codpedido'] + '-' + SugestoesAbertos['codSequencia']
    dataHora = obterHoraAtual()
    SugestoesAbertos['datahora'] = dataHora
    SugestoesAbertos['reservado'] = 'nao'
    SugestoesAbertos.drop('codSequencia', axis=1, inplace=True)
    SugestoesAbertos['endereco'] = 'Não Reposto'



    query =  'Insert into "Reposicao".pedidossku (codpedido, produto, qtdesugerida, qtdepecasconf, endereco,' \
             ' necessidade, datahora, reservado ) values (%s, %s, %s, %s, %s, %s, %s, %s )'

    conn_pg = ConexaoPostgreMPL.conexao()
    cursor = conn_pg.cursor()

    for _, row in SugestoesAbertos.iterrows():
        cursor.execute(query, (
            row['codpedido'], row['produto'], row['qtdesugerida'], row['qtdepecasconf'],
            row['endereco'], row['necessidade'], row['datahora'], row['reservado']
        ))
        conn_pg.commit()

    cursor.close()
    conn_pg.close()

    return SugestoesAbertos
