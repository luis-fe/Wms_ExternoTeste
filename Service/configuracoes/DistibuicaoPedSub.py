import pandas as pd
import ConexaoPostgreMPL



def PedidosSkuEspecial():
    conn = ConexaoPostgreMPL.conexao()

    consulta = """
select p.codpedido, p.produto, ts.cor, ts.engenharia, p.endereco, p.necessidade  from "Reposicao"."Reposicao".pedidossku p 
inner join "Reposicao"."Reposicao"."Tabela_Sku" ts on ts.codreduzido = p.produto
where p.codpedido||ts.engenharia ||ts.cor in (
select p.codpedido||data2.engenharia||data2.cor from "Reposicao"."Reposicao".pedidossku p
inner join (select "Endereco", max(resticao) as resticao, max(cor) as cor, max(engenharia) as engenharia from "Reposicao"."Reposicao".tagsreposicao t  group by "Endereco")data2 on data2."Endereco" = p.endereco  
where data2.resticao like '%||%'
)
    """

    consulta = pd.read_sql(consulta,conn)

    conn.close()
    consulta = consulta.sort_values(by=['codpedido','engenharia','cor'], ascending=False,
                                ignore_index=True)


    return consulta

