import pandas as pd
import ConexaoPostgreMPL
import numpy as np



def PedidosSkuEspecial():
    conn = ConexaoPostgreMPL.conexao()

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
    consulta['endereco_sugerido'].fillna(0, inplace=True)

    consulta['consulta'] = consulta.apply(lambda row: 'MUDAR' if row['Restricao'] != row['Restricao Sugerida'] and row['SaldoEndereco'] > 0 else 'MANTER',axis=1    )
    return consulta

def UpdateEndereco(dataframe):

    update = """
    update "Reposicao"."Reposicao".pedidossku 
    set p.endereco = %s 
    where codpedido = %s and p.produto = %s 
    """

    conn= ConexaoPostgreMPL.conexao()

    for i in dataframe:
        endereco = dataframe['BASE'][i]
        produto = dataframe['produto'][i]
        codpedido = dataframe['codpedido'][i]

    conn.close()