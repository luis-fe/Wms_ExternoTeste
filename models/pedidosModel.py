import ConexaoPostgreMPL, ConexaoCSW
import pandas as pd
import numpy as np
import datetime
import pytz
import locale
from models import finalizacaoPedidoModel


def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    return hora_str



def DetalhaPedido(codPedido):
    # 1- Filtrando o Pedido na tabela de pedidosSku
    conn = ConexaoPostgreMPL.conexao()

    skus1 = pd.read_sql("""select codigopedido, desc_tiponota  , codcliente ||""" + "'-'" + '|| desc_cliente as cliente  '
                        ',codrepresentante  ||' + "'-'" + '|| desc_representante  as repres, agrupamentopedido, cod_usuario as usuario '
                    'from "Reposicao".filaseparacaopedidos f  where codigopedido= ' + "'" + codPedido + "'"
                       , conn)

    if skus1.empty:
        # Olha Para os Pedidos de Transferencia
            skus = pd.read_sql("select descricaopedido as codigopedido, 'transferencia' as desc_tiponota,"
                               " 'transferencia de Naturezas' as cliente  "
                                ",'transferencia de Naturezas' as repres, "
                                'codigopedido as agrupamentopedido '
                                'from "Reposicao"."pedidosTransferecia" f  '
                                "where situacao = 'aberto'" 
                                ' and descricaopedido= ' + "'" + codPedido + "'"
                       , conn)
    else:

            skus = skus1

    grupo = pd.read_sql('select agrupamentopedido '
                        'from "Reposicao".filaseparacaopedidos f  where codigopedido= ' + "'" + codPedido + "'"
                        , conn)
    DetalhaSku = pd.read_sql(
        'select  produto as reduzido, qtdesugerida , endereco as endereco, necessidade as a_concluir , '
        'qtdesugerida as total, (qtdesugerida - necessidade) as qtdrealizado'
        ' from "Reposicao".pedidossku p  where codpedido= ' + "'" + codPedido + "'"
                                                                                " order by endereco asc", conn)

    # Validando as descricoes + cor + tamanho dos produtos para nao ser null

    descricaoSku = pd.read_sql(
                    """
                    select engenharia as referencia, codreduzido as reduzido, descricao, cor ,tamanho from "Reposicao"."Tabela_Sku"
                    where codreduzido in
                    (select  produto as reduzido
                    from "Reposicao".pedidossku p  where codpedido = """ + "'" + codPedido + "') """, conn)
    itensMkt = DescricaoCswItensMKT()
    descricaoSku = pd.concat([descricaoSku,itensMkt])


    descricaoSku.drop_duplicates(subset='reduzido', keep='first', inplace=True)


    DetalhaSku = pd.merge(DetalhaSku, descricaoSku, on='reduzido', how='left')

    # Agrupar os valores da col2 por col1 e concatenar em uma nova coluna
    DetalhaSku['endereco'] = DetalhaSku.groupby(['reduzido'])['endereco'].transform(lambda x: ', '.join(x))
    # Remover as linhas duplicadas
    DetalhaSku['total'] = DetalhaSku.groupby('reduzido')['total'].transform('sum')
    DetalhaSku['qtdesugerida'] = DetalhaSku.groupby('reduzido')['qtdesugerida'].transform('sum')

    DetalhaSku['qtdrealizado'] = DetalhaSku.groupby('reduzido')['qtdrealizado'].transform('sum')
    DetalhaSku['a_concluir'] = DetalhaSku.groupby('reduzido')['a_concluir'].transform('sum')
    DetalhaSku['qtdesugerida'] = DetalhaSku['qtdesugerida'].astype(int)
    DetalhaSku['qtdrealizado'] = DetalhaSku['qtdrealizado'].astype(int)
    DetalhaSku['concluido_X_total'] = DetalhaSku['qtdrealizado'].astype(str) +'/'+DetalhaSku['qtdesugerida'].astype(str)
    DetalhaSku = DetalhaSku.drop_duplicates()
    DetalhaSku.fillna('nao localizado', inplace=True)


    #finalizacaoPedidoModel.VerificarExisteApontamento(codPedido, skus['usuario'][0])

    data = {
        '1 - codpedido': f'{skus["codigopedido"][0]} ',
        '2 - Tiponota': f'{skus["desc_tiponota"][0]} ',
        '3 - Cliente': f'{skus["cliente"][0]} ',
        '4- Repres.': f'{skus["repres"][0]} ',
        # '4.1- Grupo.': f'{skus["grupo"][0]} ',
        '5- Detalhamento dos Sku:': DetalhaSku.to_dict(orient='records')
    }

    return [data]


def AtribuirPedido(usuario, pedidos, dataAtribuicao):
    tamanho = len(pedidos)
    pedidosNovo = []
    for i in range(tamanho):
       incr =  str(pedidos[i])
       pedidosNovo.append(incr)

    pedidosNovo = [p.replace(',', '/') for p in pedidosNovo]



    if tamanho >= 0:
        conn = ConexaoPostgreMPL.conexao()
        for i in range(tamanho):
            pedido_x = str(pedidosNovo[i])
            query = 'update "Reposicao".filaseparacaopedidos '\
                    'set cod_usuario = %s '\
                    'where codigopedido = %s'
            cursor = conn.cursor()
            cursor.execute(query, (usuario,pedido_x, ))
            conn.commit()
            cursor.close()
            consulta1 = pd.read_sql('select datahora, vlrsugestao  from "Reposicao".filaseparacaopedidos '
                                   ' where codigopedido = %s ', conn, params=(pedido_x,))

            consulta2 = pd.read_sql('select * from "Reposicao".finalizacao_pedido '
                                   ' where codpedido = %s ', conn, params=(pedido_x,))

            consulta3 = pd.read_sql('select sum(qtdesugerida) as qtdepcs  from "Reposicao".pedidossku '
                                   ' where codpedido = %s group by codpedido ', conn, params=(pedido_x,))
            dataatual = obterHoraAtual()
            try:
                if consulta2.empty and not consulta1.empty:
                    datahora = consulta1["datahora"][0]
                    vlrsugestao = consulta1["vlrsugestao"][0]
                    qtdepcs = consulta3["qtdepcs"][0]
                    cursor2 = conn.cursor()

                    insert = 'insert into "Reposicao".finalizacao_pedido (usuario, codpedido, datageracao, dataatribuicao, vlrsugestao, qtdepçs) values (%s , %s , %s , %s, %s, %s)'
                    cursor2.execute(insert, (usuario, pedido_x, datahora, dataatual, vlrsugestao,qtdepcs))
                    conn.commit()


                    cursor2.close()
                    print(f'Insert Pedido Finalizacao {usuario} e {datahora}')
                else:

                    cursor2 = conn.cursor()
                    vlrsugestao = consulta1["vlrsugestao"][0]
                    qtdepcs = consulta3["qtdepcs"][0]

                    update = 'update "Reposicao".finalizacao_pedido ' \
                             'set datageracao = %s , dataatribuicao = %s , usuario = %s, vlrsugestao = %s, qtdepçs= %s ' \
                             'where codpedido = %s'
                    cursor2.execute(update, (consulta1['datahora'][0], dataatual,usuario, vlrsugestao,qtdepcs, pedido_x))
                    conn.commit()
                    cursor2.close()

                    print(f'update {consulta3}')

            except:
                print('segue o baile')
        conn.close()
    else:
        print('sem pedidos')

    data = {
        '1- Usuario:': usuario,
        '2- Pedidos para Atribuir:': pedidos,
        '3- dataAtribuicao:': dataAtribuicao
    }
    return [data]

def AtribuicaoDiaria():
    conn = ConexaoPostgreMPL.conexao()

    query = pd.read_sql('select usuario, qtdepçs, vlrsugestao from "Reposicao".finalizacao_pedido '
                        'WHERE CAST(dataatribuicao AS DATE) = current_date;',conn)
    query['qtdPedidos'] =   query['usuario'].count()
    query ['qtdepçs'] = query ['qtdepçs'].astype(float)
    query['qtdepçs'] = query['qtdepçs'].astype(int)
    query['vlrsugestao'] = query['vlrsugestao'].astype(float)


    query = query.groupby('usuario').agg({
        'qtdepçs': 'sum',
        'vlrsugestao': 'sum',
        'qtdPedidos': 'count'})

    def format_with_separator(value):
        return locale.format('%0.2f', value, grouping=True)

    query['vlrsugestao'] = query['vlrsugestao'].apply(format_with_separator)


    query['vlrsugestao'] = query['vlrsugestao'].astype(str)
    query['vlrsugestao'] = 'R$ ' + query['vlrsugestao']
    query['Méd. pç/pedido'] = query['qtdepçs']/query['qtdPedidos']
    query['Méd. pç/pedido'] = query['Méd. pç/pedido'].round(2)
    query = query.sort_values(by='qtdPedidos', ascending=False)
    Usuarios = pd.read_sql('Select codigo as usuario, nome from "Reposicao".cadusuarios ', conn)
    Usuarios['usuario'] = Usuarios['usuario'].astype(str)
    query = pd.merge(query, Usuarios, on='usuario', how='left')
    query.rename(columns={'nome': '1- nome', "qtdPedidos": '2- qtdPedidos', "qtdepçs":"3- qtdepçs", "vlrsugestao": "5- Valor Atribuido",
                          "Méd. pç/pedido": "4- Méd. pç/pedido"}, inplace=True)

    return query
def InformacaoPedidoViaTag(codbarras):
    conn = ConexaoPostgreMPL.conexao()

    Informa = pd.read_sql('Select codpedido, usuario, dataseparacao  from "Reposicao".tags_separacao '
                          'where codbarrastag = '+"'"+codbarras+"'",conn)

    Informa2 = pd.read_sql('select codigopedido as codpedido, codcliente, desc_cliente, desc_tiponota  from "Reposicao".filaseparacaopedidos',conn)

    Informa = pd.merge(Informa,Informa2,on='codpedido',how='left')

    return Informa

def InformacaoImpresao(pedido):
    conn = ConexaoPostgreMPL.conexao()

    pedido = pd.read_sql('select codigopedido, desc_cliente as cliente, codcliente, cod_usuario, cidade, estado, agrupamentopedido,'
                         ' prioridade from "Reposicao".filaseparacaopedidos f '
                         'where codigopedido = %s',conn,params=(pedido,))

    pedido['agrupamentopedido'] = pedido.apply(lambda row: '-' if row['codigopedido'] == row['agrupamentopedido'] else row['agrupamentopedido'], axis=1)

    usuarios = pd.read_sql(
        'select codigo as cod_usuario , nome as separador  from "Reposicao".cadusuarios c ', conn)
    usuarios['cod_usuario'] = usuarios['cod_usuario'].astype(str)
    pedido = pd.merge(pedido, usuarios, on='cod_usuario', how='left')


    try:


        conn2 = ConexaoCSW.Conexao()
        transporta = pd.read_sql('SELECT  t.cidade , t.siglaEstado as estado, f.fantasia as transportadora  FROM Asgo_Trb.TransPreferencia t'
                                 ' join cad.Transportador  f on  f.codigo  = t.Transportador  '
                                 ' WHERE t.Empresa = 1 ',conn2)
        conn2.close()
        pedido = pd.merge(pedido, transporta, on=["cidade", "estado"], how='left')

       # pedido['transportadora'] = 'Perdeu Conexao Csw'


    except:

        pedido['transportadora'] = 'Perdeu Conexao Csw'
    codigoCliente = pedido['codcliente'][0]

    pedido.fillna(' - ', inplace=True)


    return codigoCliente, pedido['cliente'][0],pedido['separador'][0],pedido['transportadora'][0],pedido['agrupamentopedido'][0], pedido['prioridade'][0]
#
def PrioridadePedido(pedidos):

    tamanho = len(pedidos)

    if tamanho >= 0:
        conn = ConexaoPostgreMPL.conexao()
        for i in range(tamanho):

            pedido = str(pedidos[i])
            prioridade = ConsultaPrioridade(pedido)

            if prioridade == 'URGENTE':
                prioridade = '-'
            else:
                prioridade = 'URGENTE'

            pedido_x = '%'+ pedido +'%'

            query = 'update "Reposicao".filaseparacaopedidos ' \
                    'set prioridade = %s ' \
                    'where agrupamentopedido like %s'
            cursor = conn.cursor()
            cursor.execute(query, (prioridade,pedido_x,))
            conn.commit()
            cursor.close()
        conn.close()
        return True
    else:
        return False

def ConsultaPrioridade(pedido):
    conn = ConexaoPostgreMPL.conexao()


    pedido_x = '%' + pedido + '%'

    consulta = pd.read_sql('select prioridade from "Reposicao".filaseparacaopedidos '
                           'where agrupamentopedido like %s', conn,params=(pedido_x,))
    conn.close()

    prioridade = consulta['prioridade'][0]
    return prioridade

def DescricaoCswItensMKT():
    sql = """
    SELECT d.codItem as referencia, d.codItem as reduzido, i.nome||'-saldo:'||d.estoqueAtual  as descricao, 'uni' as tamanho, 'cor-unica'  as cor    FROM est.DadosEstoque d
inner join Cgi.Item i on i.codigo = d.codItem 
inner join cgi.Item2 i2 on i2.Empresa = 1 and i2.codItem = i.codigo 
WHERE d.codNatureza =21 and d.codEmpresa =1 and d.estoqueAtual > 0
    """
    with ConexaoCSW.Conexao() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            itensMKt = pd.DataFrame(rows, columns=colunas)

    return itensMKt


def obtendoAsultimasDevolucoes(empresa):
    sql = """
    SELECT
	DISTINCT 
	d.codCliente as codcliente,
	'DEVOLUCAO' as prioridade
from
	Fat_Dev.NotaFiscalDevolucao d
inner join 
	Fat_Dev.DevolucaoMotivos m on m.codEmpresa = d.codEmpresa and m.motivosPvt = d.motivosPvt 
WHERE
	dataEmissao >= DATEADD(DAY, -180, GETDATE()) and d.codEmpresa = """ +str(empresa) +""" and (m.descricao like '%DEF%' OR m.descricao like '%QUAL%') 
    """

    with ConexaoCSW.Conexao() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            devolucao = pd.DataFrame(rows, columns=colunas)



    #Atualizando a prioridade

    consultaWMS = """
    select codcliente, codigopedido  from "Reposicao"."Reposicao".filaseparacaopedidos f
    """

    conn = ConexaoPostgreMPL.conexaoEngine()
    consulta = pd.read_sql(consultaWMS,conn)

    devolucao['codcliente'] = devolucao['codcliente'].astype(str)
    consulta = pd.merge(consulta,devolucao, on='codcliente')
    print(consulta)


    update = """
UPDATE "Reposicao"."Reposicao".filaseparacaopedidos
SET prioridade = 
    CASE 
        WHEN prioridade IS NULL THEN 'REVISAR'  
        WHEN prioridade = 'REVISAR' THEN 'REVISAR'  -- Não altera se já for 'REVISAR'
        ELSE prioridade || 'REVISAR'           
    END
WHERE codigopedido = %s;
    """

    with conn.connect() as connection:
        for index, row in consulta.iterrows():
            connection.execute(update, (
                row["codigopedido"]
            ))

    return devolucao





