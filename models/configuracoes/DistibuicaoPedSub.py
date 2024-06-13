import pandas as pd
import ConexaoPostgreMPL
import numpy as np



def PedidosSkuEspecial():
    conn = ConexaoPostgreMPL.conexao()

# Etapa 1: Pesquisando os pedidos enviados com itens que podem ter reserva de Substitutos
    consulta = """
    select p.codpedido as pedido, ts.engenharia , ts.cor , produto, p.necessidade ,  p.endereco, e.resticao as "Restricao"  from "Reposicao"."Reposicao".pedidossku p 
inner join "Reposicao"."Reposicao"."Tabela_Sku" ts on ts.codreduzido = p.produto 
left join (select "Endereco" , max(resticao) as resticao from "Reposicao"."Reposicao".tagsreposicao t group by t."Endereco") e on e."Endereco" = p.endereco  
where engenharia ||cor in (
select t.engenharia||cor from "Reposicao"."Reposicao".tagsreposicao t 
        where t.resticao  like '%||%')
        order by p.codpedido,ts.engenharia , ts.cor
    """

    consulta = pd.read_sql(consulta,conn)

    SaldoPorRestricao = """
    select ts.engenharia , ts.cor, ce."SaldoLiquid", ce.endereco , tag."Restricao"  from "Reposicao"."Reposicao"."calculoEndereco" ce 
join "Reposicao"."Reposicao"."Tabela_Sku" ts on ts.codreduzido = ce.codreduzido 
join (select "Endereco", max(resticao) as "Restricao" from "Reposicao"."Reposicao".tagsreposicao t group by "Endereco") tag on tag."Endereco" = ce.endereco 
    """
    SaldoPorRestricao = pd.read_sql(SaldoPorRestricao,conn)
    SaldoPorRestricao['SaldoLiquid'].fillna(0, inplace=True)
    SaldoPorRestricao2 = SaldoPorRestricao.groupby(['Restricao', 'engenharia', 'cor']).agg(
        {'SaldoLiquid': 'sum'}).reset_index()
    SaldoPorRestricao2 = SaldoPorRestricao2.sort_values(by='SaldoLiquid', ascending=False,
                        ignore_index=True)  # escolher como deseja classificar
    SaldoPorRestricao2['repeticao'] = SaldoPorRestricao2.groupby(['engenharia','cor'])['Restricao'].cumcount()

    SaldoPorRestricao2 = SaldoPorRestricao2[SaldoPorRestricao2['repeticao']==0]
    SaldoPorRestricao2.rename(columns={'SaldoLiquid': 'SaldoTotalEngCor','Restricao':'Restricao Sugerida'}, inplace=True)


    consultaSaldoRestricaoProduto = """
    select ce.endereco as endereco_sugerido, ce.produto , tag."Restricao" as "Restricao Sugerida", ce."SaldoLiquid" as "SaldoEndereco"  from "Reposicao"."Reposicao"."calculoEndereco" ce 
join (select "Endereco", max(resticao) as "Restricao" from "Reposicao"."Reposicao".tagsreposicao t group by "Endereco") tag on tag."Endereco" = ce.endereco 
    """
    consultaSaldoRestricaoProduto = pd.read_sql(consultaSaldoRestricaoProduto,conn)


    consultaSaldoRestricaoProduto['SaldoEndereco'].fillna(0, inplace=True)
    consultaSaldoRestricaoProduto = consultaSaldoRestricaoProduto.sort_values(by='SaldoEndereco', ascending=False,
                        ignore_index=True)  # escolher como deseja classificar
    consultaSaldoRestricaoProduto['repeticao2'] = consultaSaldoRestricaoProduto.groupby('produto')['endereco_sugerido'].cumcount()
    consultaSaldoRestricaoProduto = consultaSaldoRestricaoProduto[consultaSaldoRestricaoProduto['repeticao2']==0]

    conn.close()

    consulta['Restricao'].fillna('Sem Restricao',inplace=True)
    consulta['SomaNecessidade'] = consulta.groupby(['pedido','engenharia','cor'])['necessidade'].transform('sum')
    consulta = consulta[consulta['SomaNecessidade'] >0]


    def avaliar_grupo(df_grupo):
        return len(set(df_grupo)) == 1

    df_resultado = consulta.groupby(['pedido','engenharia','cor'])['Restricao'].apply(avaliar_grupo).reset_index()
    df_resultado.columns = ['pedido','engenharia','cor', 'Resultado']

    consulta = pd.merge(consulta, df_resultado,on=['pedido','engenharia','cor'],how='left')
    consulta = consulta[consulta['Resultado'] == False]

    consulta = pd.merge(consulta,SaldoPorRestricao2,on=['engenharia','cor'], how='left')
    consulta = pd.merge(consulta,consultaSaldoRestricaoProduto,on=['produto','Restricao Sugerida'], how='left')
    consulta['SaldoEndereco'].fillna(0, inplace=True)
    consulta['endereco_sugerido'].fillna('-', inplace=True)
    consulta.fillna('-', inplace=True)
    try:
        # Verifique se a chave 'consulta' existe no DataFrame
        if 'consulta' not in consulta.columns:
                consulta['consulta'] = None
    except KeyError as e:
        print(f"KeyError: {e}")

    consulta['consulta'] = consulta.apply(lambda row: 'MUDAR' if row['Restricao'] != row['Restricao Sugerida'] and row['SaldoEndereco'] > 0 else 'MANTER',axis=1    )

    mudar = consulta[consulta['consulta']=='MUDAR'].reset_index()
    UpdateEndereco(mudar)

    return consulta

def UpdateEndereco(dataframe):

    update = """
    update "Reposicao"."Reposicao".pedidossku 
    set endereco = %s 
    where codpedido = %s and produto = %s 
    """

    conn = ConexaoPostgreMPL.conexao()

    cursor = conn.cursor()
    for index, row in dataframe.iterrows():
        endereco = row['endereco_sugerido']
        produto = row['produto']
        codpedido = row['pedido']

        cursor.execute(update, (endereco, codpedido, produto,))
        conn.commit()

    cursor.close()
    conn.close()



def DashbordPedidosAAprovar():
    dados = PedidosSkuEspecial()
    dados['Restricao'] = dados['Restricao'].replace('Sem Restricao','Sem Restricao||Normal')

    usuarioAtribuido = """
    SELECT f.codigopedido AS pedido, c.nome AS "UsuarioAtribuido"
FROM "Reposicao"."Reposicao".filaseparacaopedidos f 
LEFT JOIN "Reposicao"."Reposicao".cadusuarios c ON c.codigo::varchar = f.cod_usuario
    """

    dados['Pedido||Engenharia||Cor'] = dados.groupby(['pedido','engenharia','cor'])['Restricao'].cumcount()

    totalPedidos = dados[dados['Pedido||Engenharia||Cor'] == 0]
    totalPedidos = totalPedidos['Pedido||Engenharia||Cor'].count()

    dados2 = dados.loc[:, ['pedido', 'engenharia', 'cor', 'Restricao','necessidade']]
    dados2['Restricao'] = dados2['Restricao'].str.split('\|\|').str[1]
    dados2['Necessidade'] = dados2['necessidade'].round(0).astype(int)
    dados2 = dados2.groupby(['pedido', 'engenharia', 'cor', 'Restricao']).agg({'necessidade': 'sum'}).reset_index()

    df_summary = dados2.groupby(['pedido', 'cor', 'engenharia']).apply(
        lambda x: ';'.join(f"{rest}({nec})" for rest, nec in zip(x['Restricao'], x['necessidade']))).reset_index()
    df_summary.columns = ['pedido', 'cor', 'engenharia', 'Sugerido WMS']
    conn = ConexaoPostgreMPL.conexao()
    usuarioAtribuido = pd.read_sql(usuarioAtribuido,conn,)
    df_summary = pd.merge(df_summary,usuarioAtribuido,on='pedido',how="left")
    df_summary.fillna('-',inplace=True)

    pedidosOK = """
        select p.codpedido as pedido, ts.engenharia , ts.cor , produto, p.necessidade ,  p.endereco, e.resticao as "Restricao"  from "Reposicao"."Reposicao".pedidossku p 
    inner join "Reposicao"."Reposicao"."Tabela_Sku" ts on ts.codreduzido = p.produto 
    left join (select "Endereco" , max(resticao) as resticao from "Reposicao"."Reposicao".tagsreposicao t group by t."Endereco") e on e."Endereco" = p.endereco  
    where engenharia ||cor in (
    select t.engenharia||cor from "Reposicao"."Reposicao".tagsreposicao t 
            where t.resticao  like '%||%')
            order by p.codpedido,ts.engenharia , ts.cor
        """
    pedidosOK = pd.read_sql(pedidosOK,conn,)

    def avaliar_grupo(df_grupo):
        return len(set(df_grupo)) == 1

    df_resultado = pedidosOK.groupby(['pedido', 'engenharia', 'cor'])['Restricao'].apply(avaliar_grupo).reset_index()
    df_resultado.columns = ['pedido', 'engenharia', 'cor', 'Resultado']

    pedidosOK = pd.merge(pedidosOK, df_resultado, on=['pedido', 'engenharia', 'cor'], how='left')
    pedidosOK = pedidosOK[pedidosOK['Resultado'] == True]
    pedidosOK['Pedido||Engenharia||Cor'] = pedidosOK.groupby(['pedido','engenharia','cor'])['Restricao'].cumcount()

    totalPedidosok = pedidosOK[pedidosOK['Pedido||Engenharia||Cor'] == 0]
    totalPedidosok = totalPedidosok['Pedido||Engenharia||Cor'].count()


    conn.close()

    data = {

        '1-Total Pedidos - Pedido||Engenharia||Cor':f'{totalPedidos} (pedidos casados ok: {totalPedidosok})',
        '4- Detalhamento ': df_summary.to_dict(orient='records')

    }

    return pd.DataFrame([data])