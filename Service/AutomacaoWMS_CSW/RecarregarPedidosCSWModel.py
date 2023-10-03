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
    SugestoesAbertos = pd.merge(SugestoesAbertos, tiponota, on='codtiponota', how='left')
    condicaopgto = pd.read_sql("SELECT v.codEmpresa||'||'||codigo as condvenda, descricao as  condicaopgto FROM cad.CondicaoDeVenda v",conn)
    SugestoesAbertos = pd.merge(SugestoesAbertos, condicaopgto, on='condvenda', how='left')

    SugestoesAbertos.fillna('-', inplace=True)

    conn2 = ConexaoPostgreMPL.conexao()
    validacao = pd.read_sql('select codigopedido, '+"'ok'"+' as "validador"  from "Reposicao".filaseparacaopedidos f ', conn2)

    SugestoesAbertos = pd.merge(SugestoesAbertos, validacao, on='codigopedido', how='left')
    SugestoesAbertos = SugestoesAbertos.loc[SugestoesAbertos['validador'].isnull()]
    # Excluir a coluna 'B' inplace
    SugestoesAbertos.drop('validador', axis=1, inplace=True)
    tamanho = SugestoesAbertos['codigopedido'].size
    dataHora = obterHoraAtual()
    SugestoesAbertos['datahora'] = dataHora
    # Contar o número de ocorrências de cada valor na coluna 'coluna'
    contagem = SugestoesAbertos['codcliente'].value_counts()

    # Criar uma nova coluna 'contagem' no DataFrame com os valores contados
    SugestoesAbertos['contagem'] = SugestoesAbertos['codcliente'].map(contagem)
    # Aplicar a função de agrupamento usando o método groupby
    SugestoesAbertos['agrupamentopedido'] = SugestoesAbertos.groupby('codcliente')['codigopedido'].transform(
        criar_agrupamentos)
    SugestoesAbertos.drop('codPedido2', axis=1, inplace=True)

    # try:
    ConexaoPostgreMPL.Funcao_Inserir(SugestoesAbertos, tamanho, 'filaseparacaopedidos', 'append')
    # except:

    return tamanho, dataHora


    return SugestoesAbertos