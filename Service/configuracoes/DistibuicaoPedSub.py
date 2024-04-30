import pandas as pd
import ConexaoPostgreMPL
import numpy as np



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

    consulta2 = """
    select t."Endereco" as endereco, max(resticao) as restricao  from "Reposicao"."Reposicao".tagsreposicao t where t.resticao like '%||%'
    group by "Endereco" 
    """

    consulta3= """
select ce.codendereco as endereco , codreduzido as produto , saldo, "SaldoLiquid", resticao  from "Reposicao"."Reposicao"."calculoEndereco" ce 
left  join (select "Endereco", max(resticao) as resticao from "Reposicao"."Reposicao".tagsreposicao t  group by "Endereco")data2 on data2."Endereco" = ce.codendereco  
where ce."SaldoLiquid" > 0 and 
codendereco in (select t."Endereco" from "Reposicao"."Reposicao".tagsreposicao t where t.resticao like '%||%')
order by "SaldoLiquid" desc 
    """

    consulta = pd.read_sql(consulta,conn)
    consulta2 = pd.read_sql(consulta2,conn)
    consulta3 = pd.read_sql(consulta3,conn)

    conn.close()

    # Adicionando uma coluna de contagem para cada produto
    consulta3['count'] = consulta3.groupby('produto').cumcount() + 1
    pivot_df = consulta3.pivot_table(index='produto', columns='count', values=['endereco', 'SaldoLiquid','saldo','resticao'], aggfunc='first')
    pivot_df.columns = [f"{col[0]}{col[1]}" for col in pivot_df.columns]
    pivot_df.reset_index(inplace=True)

    consulta = pd.merge(consulta, consulta2,on='endereco',how='left')
    consulta['restricao'].fillna('-',inplace=True)


    consulta = consulta.sort_values(by=['codpedido','engenharia','cor'], ascending=False,
                                ignore_index=True)

    def avaliar_grupo(df_grupo):
        return len(set(df_grupo)) == 1

    df_resultado = consulta.groupby(['codpedido','engenharia','cor'])['restricao'].apply(avaliar_grupo).reset_index()
    df_resultado.columns = ['codpedido','engenharia','cor', 'Resultado']

    consulta = pd.merge(consulta, df_resultado,on=['codpedido','engenharia','cor'],how='left')
    consulta = pd.merge(consulta, pivot_df,on='produto',how='left')
    consulta.fillna('-',inplace=True)

    consulta = consulta[consulta['Resultado'] == False]
    # Acrescentar um atributo chamado de "ENDERECO BASE", para todas os enderecos divergentes seguir ele como padrao.
    ### ele seria aquele com mais saldo
    consulta['1REpeticao'] = consulta.groupby(['codpedido','engenharia','cor'])['endereco'].transform('count')
    consulta['2REpeticaoEndereco'] = consulta.groupby(['codpedido','engenharia','cor','restricao'])['restricao'].transform('count')
    consulta['3REpeticaoMax'] = consulta.groupby(['codpedido','engenharia','cor'])['2REpeticaoEndereco'].transform('max')

    base = consulta[consulta['3REpeticaoMax'] == consulta['2REpeticaoEndereco'] ]
    base = base.loc[:,
                ['codpedido','engenharia','cor','restricao']]
    base = base.drop_duplicates()
    base.rename(columns={'restricao': 'BASE'}, inplace=True)
    consulta = pd.merge(consulta, base,on=['codpedido','engenharia','cor'],how='left')

    # Função para encontrar a coluna onde o valor da 'base' está presente
    def encontrar_coluna(row):
        for col in consulta.columns[8:]:
            if isinstance(row[col], str) and row['BASE'] in row[col]:
                return col
        return None
    print(consulta.columns[8:])
    # Aplicar a função em cada linha do DataFrame
    consulta['encontrada'] = consulta.apply(encontrar_coluna, axis=1)


    # Case I: Se a necessidade for maior que 0 , a restricao for '-' verificar se é possivel encontrar endereco BASE para fechar o substitutos

    #Case II: Se nao conseguir, informar o relatorio para a Rapha


    return consulta

