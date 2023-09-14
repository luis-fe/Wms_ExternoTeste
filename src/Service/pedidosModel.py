import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd
import numpy as np
import datetime
import pytz
import locale
from src.Service import finalizacaoPedido


def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    return hora_str

def FilaPedidos():
    conn = ConexaoPostgreMPL.conexao()
    pedido = pd.read_sql(
        ' select f.codigopedido , f.vlrsugestao, f.codcliente , f.desc_cliente, f.cod_usuario, f.cidade, f.estado, '
        'datageracao, f.codrepresentante , f.desc_representante, f.desc_tiponota, condicaopgto, agrupamentopedido, situacaopedido  '
        '  from "Reposicao".filaseparacaopedidos f '
         , conn)
    pedidosku = pd.read_sql('select codpedido, sum(qtdesugerida) as qtdesugerida, sum(necessidade) as necessidade   from "Reposicao".pedidossku p  '
                            'group by codpedido ', conn)
    pedidosku.rename(columns={'codpedido': '01-CodPedido', 'qtdesugerida': '15-qtdesugerida','necessidade': '19-necessidade'}, inplace=True)

    usuarios = pd.read_sql(
        'select codigo as cod_usuario , nome as nomeusuario_atribuido  from "Reposicao".cadusuarios c ', conn)
    usuarios['cod_usuario'] = usuarios['cod_usuario'].astype(str)
    pedido = pd.merge(pedido, usuarios, on='cod_usuario', how='left')

    try:
        conn2 = ConexaoCSW.Conexao()
        transporta = pd.read_sql('SELECT  t.cidade , t.siglaEstado as estado, f.fantasia as transportadora  FROM Asgo_Trb.TransPreferencia t'
                                 ' join cad.Transportador  f on  f.codigo  = t.Transportador  '
                                 ' WHERE t.Empresa = 1 ',conn2)
        conn2.close()
        pedido = pd.merge(pedido, transporta, on=["cidade", "estado"], how='left')
    except:

        pedido['transportadora'] = 'Perdeu Conexao Csw'



    pedido.rename(
        columns={'codigopedido': '01-CodPedido', 'datageracao': '02- Data Sugestao', 'desc_tiponota': '03-TipoNota',
                 'codcliente': '04-codcliente',
                 'desc_cliente': '05-desc_cliente', 'cidade': '06-cidade', 'estado': '07-estado',
                 'codrepresentante': '08-codrepresentante', 'desc_representante': '09-Repesentante',
                 'cod_usuario': '10-codUsuarioAtribuido',
                 'nomeusuario_atribuido': '11-NomeUsuarioAtribuido', 'vlrsugestao': '12-vlrsugestao',
                 'condicaopgto': '13-CondPgto', 'agrupamentopedido': '14-AgrupamentosPedido','situacaopedido': '22- situacaopedido',"transportadora":"23-transportadora"}, inplace=True)
    pedido['12-vlrsugestao'] = 'R$ ' + pedido['12-vlrsugestao']
    pedido = pd.merge(pedido, pedidosku, on='01-CodPedido', how='left')
    pedido['15-qtdesugerida'] = pedido['15-qtdesugerida'].fillna(0)
    pedidoskuReposto = pd.read_sql('select codpedido, sum(necessidade) as reposto  from "Reposicao".pedidossku p '
                                   "where endereco <> 'Não Reposto' "
                                   'group by codpedido ', conn)
    pedidoskuReposto.rename(columns={'codpedido': '01-CodPedido', 'reposto': '16-Endereco Reposto'}, inplace=True)
    pedido = pd.merge(pedido, pedidoskuReposto, on='01-CodPedido', how='left')
    pedidoskuReposto2 = pd.read_sql('select codpedido, sum(necessidade) as naoreposto  from "Reposicao".pedidossku p '
                                    "where endereco = 'Não Reposto' "
                                    'group by codpedido ', conn)
    pedidoskuReposto2.rename(columns={'codpedido': '01-CodPedido', 'naoreposto': '17-Endereco NaoReposto'},
                             inplace=True)
    pedido = pd.merge(pedido, pedidoskuReposto2, on='01-CodPedido', how='left')
    pedido['16-Endereco Reposto'] = pedido['16-Endereco Reposto'].fillna(0)
    pedido['17-Endereco NaoReposto'] = pedido['17-Endereco NaoReposto'].fillna(0)
    pedido['19-necessidade'] = pedido['19-necessidade'].fillna(0)

    pedido['18-%Reposto'] = pedido['17-Endereco NaoReposto'] + pedido['16-Endereco Reposto']


    pedido['18-%Reposto'] = pedido['16-Endereco Reposto'] / pedido['18-%Reposto']


    pedido['20-Separado%'] = 1 - (pedido['19-necessidade'] / pedido['15-qtdesugerida'])
    pedido['18-%Reposto'] = (pedido['18-%Reposto'] * 100).round(2)
    pedido['18-%Reposto'] = pedido['18-%Reposto'].fillna(0)
    pedido['20-Separado%'] = (pedido['20-Separado%'] * 100).round(2)
    pedido['20-Separado%'] = pedido['20-Separado%'].fillna(0)
    # obtendo a Marca do Pedido
    marca = pd.read_sql('select codpedido ,  t.engenharia   from "Reposicao".pedidossku p '
                        'join "Reposicao".tagsreposicao t on t.codreduzido = p.produto '
                        'group by codpedido, t.engenharia ',conn)
    marca['engenharia'] = marca['engenharia'].str.slice(1)
    marca['21-MARCA'] =np.where((marca['engenharia'].str[:3] == '102') | (marca['engenharia'].str[:3] == '202') , 'M.POLLO', 'PACO')
    marca.drop('engenharia', axis=1, inplace=True)
    marca.drop_duplicates(subset='codpedido', inplace=True)
    marca.rename(columns={'codpedido': '01-CodPedido'}, inplace=True)
    pedido = pd.merge(pedido, marca, on='01-CodPedido', how='left')
    pedido['21-MARCA'].fillna('-', inplace=True)
    pedido['22- situacaopedido'].fillna('No Retorna', inplace=True)
    pedido.fillna('-', inplace=True)
    pedido.replace([np.inf, -np.inf], 0, inplace=True)

    return pedido




def FilaAtribuidaUsuario(codUsuario):
    x = FilaPedidos()
    codUsuario = str(codUsuario)
    x = x[x['10-codUsuarioAtribuido'] == codUsuario]
    return x

def DetalhaPedido(codPedido):
    # 1- Filtrando o Pedido na tabela de pedidosSku
    conn = ConexaoPostgreMPL.conexao()

    skus1 = pd.read_sql('select codigopedido, desc_tiponota  , codcliente ||' + "'-'" + '|| desc_cliente as cliente  '
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
                    'select engenharia as referencia, codreduzido as reduzido, descricao, cor ,tamanho from "Reposicao"."Tabela_Sku" '
                    ' where codreduzido in '
                    '(select  produto as reduzido '
                    'from "Reposicao".pedidossku p  where codpedido = ' + "'" + codPedido + "') ", conn)


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

    finalizacaoPedido.VerificarExisteApontamento(codPedido, skus['usuario'][0])

    data = {
        '1 - codpedido': f'{skus["codigopedido"][0]} ',
        '2 - Tiponota': f'{skus["desc_tiponota"][0]} ',
        '3 - Cliente': f'{skus["cliente"][0]} ',
        '4- Repres.': f'{skus["repres"][0]} ',
        # '4.1- Grupo.': f'{skus["grupo"][0]} ',
        '5- Detalhamento dos Sku:': DetalhaSku.to_dict(orient='records')
    }

    return [data]

def ClassificarFila(coluna, tipo):
    fila = FilaPedidos()
    fila['12-vlrsugestao'] = fila['12-vlrsugestao'].str.replace("R\$", "").astype(float)

    if tipo == 'desc':
        fila = fila.sort_values(by=coluna, ascending=False)
        fila['12-vlrsugestao'] = fila['12-vlrsugestao'] .astype(str)
        fila['12-vlrsugestao'] = 'R$ ' + fila['12-vlrsugestao']

        return fila

    else:
        fila['12-vlrsugestao'] = fila['12-vlrsugestao'] .astype(str)
        fila['12-vlrsugestao'] = 'R$ ' + fila['12-vlrsugestao']
        fila = fila.sort_values(by=coluna, ascending=True)
        return fila

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

    pedido = pd.read_sql('select desc_cliente as cliente, codcliente, cod_usuario as separador, cidade, estado from "Reposicao".filaseparacaopedidos f '
                         'where codigopedido = %s',conn,params=(pedido,))
    try:
        conn2 = ConexaoCSW.Conexao()
        transporta = pd.read_sql('SELECT  t.cidade , t.siglaEstado as estado, f.fantasia as transportadora  FROM Asgo_Trb.TransPreferencia t'
                                 ' join cad.Transportador  f on  f.codigo  = t.Transportador  '
                                 ' WHERE t.Empresa = 1 ',conn2)
        conn2.close()
        pedido = pd.merge(pedido, transporta, on=["cidade", "estado"], how='left')
    except:

        pedido['transportadora'] = 'Perdeu Conexao Csw'

    return pedido['codcliente'][0],pedido['cliente'][0],pedido['separador'][0],pedido['transportadora'][0]

