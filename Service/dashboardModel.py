import ConexaoPostgreMPL
import pandas as pd
def relatorioTotalFila(empresa, natureza):
    conn = ConexaoPostgreMPL.conexao()

    noRetorna = pd.read_sql('SELECT codigopedido as codpedido from "Reposicao".filaseparacaopedidos '
                        "where situacaopedido = 'No Retorna'",conn)


    query = pd.read_sql('SELECT numeroop, COUNT(codbarrastag) AS Saldo '
        'FROM "Reposicao".filareposicaoportag t where codnaturezaatual = %s ' 
        ' GROUP BY "numeroop" ',conn,params=(natureza,))


    # obtendo os skus nao repostos
    query2 = pd.read_sql('select *, 1 as contagem from "Reposicao".pedidossku p'
                        " where reservado = 'nao' ",conn)
    query2 = pd.merge(query2, noRetorna, on='codpedido' )

    query3 = pd.read_sql('select *, 1 as contagem from "Reposicao".pedidossku p'
                        " where reservado = 'sim' ",conn)
    query3 = pd.merge(query3, noRetorna, on='codpedido')

    Inventario = pd.read_sql('select codreduzido  from "Reposicao".tagsreposicao_inventario ti' ,conn)
    Reposto = pd.read_sql('select codreduzido  from "Reposicao".tagsreposicao ti where natureza = %s ' ,conn, params=(natureza,))

    query['saldo'] = query['saldo'].sum()
    query2['contagem'] = query2['qtdesugerida'].sum()
    query3['contagem'] = query3['qtdesugerida'].sum()
    Inventario['codreduzido'] = Inventario['codreduzido'].count()
    Reposto['codreduzido'] = Reposto['codreduzido'].count()
    if  query3.empty:
        total = 0
    else:
        total =  query3['contagem'][0] +  query2['contagem'][0]

    Percentual = query3['contagem'][0] / total
    Percentual = round(Percentual, 2) * 100
    totalPecas = query["saldo"][0] + Reposto["codreduzido"][0]+Inventario["codreduzido"][0]
    # Aplicando a formatação para exibir como "100.000"
    query['saldo'] = query['saldo'].apply(lambda x: "{:,.0f}".format(x))
    saldo_str= str(query["saldo"][0])
    saldo_str = saldo_str.replace(',', '.')
    totalPecas = "{:,.0f}".format(totalPecas)
    totalPecas = str(totalPecas)
    totalPecas = totalPecas.replace(',', '.')
    total = "{:,.0f}".format(total)
    total = str(total)
    total2 = total.replace(',', '.')


    conn.close()
    data = {
        '1.0':f' Informacoes Gerais do Estoque natureza: {natureza}',
        '1.1-Total de Peças Nat. 5':   f'{totalPecas} pçs',
        '1.2-Saldo na Fila':   f'{saldo_str} pçs',
        '1.3-Peçs Repostas':   f'{Reposto["codreduzido"][0]} pçs',
        '1.4-Peçs em Inventario':   f'{Inventario["codreduzido"][0]} pçs',
        '2.0':' Informacoes dos pedidos',
        '2.1- Total de Skus nos Pedidos em aberto ': f'{total2} pçs',
        '2.2-Qtd de Enderecos Nao Reposto em Pedido': f'{query2["contagem"][0]}',
        '2.3-Qtd de Enderecos OK Reposto nos Pedido': f'{query3["contagem"][0]}',
        '2.4- Percentual Reposto':f'{Percentual}%'
    }
    return [data]

def Pedidos_fecha100():
    conn = ConexaoPostgreMPL.conexao()
    query = pd.read_sql('SELECT codigopedido from "Reposicao".filaseparacaopedidos '
                        "where situacaopedido = 'No Retorna'",conn)

    totalPedido = pd.read_sql('SELECT codpedido as codigopedido, count (necessidade) totalpc from "Reposicao".pedidossku '
                              'group by codpedido'
                        ,conn)

    totalPedido100 = pd.read_sql('SELECT codpedido as codigopedido, count (necessidade) totalpc100 from "Reposicao".pedidossku '
                                 " where reservado = 'sim' "
                              ' group by codpedido'
                        ,conn)
    totalPedido = pd.merge(totalPedido, totalPedido100, on='codigopedido', how='left')
    query = pd.merge(query,totalPedido,on='codigopedido')


    conn.close()

    totalPedidos = query['codigopedido'].count()
    query['percentual'] = query['totalpc100']/query['totalpc']

    Fecha100 = query[query['percentual'] == 1]
    query.drop_duplicates(subset='codigopedido', inplace=True)
    query.fillna('-', inplace=True)


    totalPedidos100 = Fecha100['codigopedido'].count()

    data = {
        '0. Mensagem':'Essa analise só considera pecas ainda nao separadas',
        '1. Total de Pedidos no Retorna':f'{totalPedidos}',
        '2. Total de Pedidos fecham 100%': f'{totalPedidos100}',
        '2.1 lista de Pedidos': query.to_dict(orient='records')
    }
    return [data]
