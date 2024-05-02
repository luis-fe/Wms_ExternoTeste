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


    conn.close()

    consulta['Restricao'].fillna('Sem Restricao',inplace=True)
    consulta['SomaNecessidade'] = consulta.groupby(['pedido','engenharia','cor'])['necessidade'].transform('sum')
    consulta = consulta[consulta['SomaNecessidade'] > 0]
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