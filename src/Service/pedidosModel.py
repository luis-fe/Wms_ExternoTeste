import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd
import numpy as np

import finalizacaoPedido


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