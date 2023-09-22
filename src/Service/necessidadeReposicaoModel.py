import pandas as pd
import ConexaoPostgreMPL
def RelatorioNecessidadeReposicao():
    conn = ConexaoPostgreMPL.conexao()
    relatorioEndereço = pd.read_sql(
        'select produto as codreduzido , sum(necessidade) as "Necessidade p/repor", count(codpedido) as "Qtd_Pedidos que usam"  from "Reposicao".pedidossku p '
        "where necessidade > 0 and endereco = 'Não Reposto' "
        " group by produto ", conn)
    relatorioEndereçoEpc = pd.read_sql(
        'select codreduzido , max(epc) as epc_Referencial, engenharia from "Reposicao".filareposicaoportag f '
        'where epc is not null and engenharia is not null '
        'group by codreduzido, engenharia', conn)

    relatorioEndereço = pd.merge(relatorioEndereço, relatorioEndereçoEpc, on='codreduzido', how='left')
    # Clasificando o Dataframe para analise
    relatorioEndereço = relatorioEndereço.sort_values(by='necessidade_pedidos', ascending=False,
                                                      ignore_index=True)  # escolher como deseja classificar

    conn.close()
    data = {

        '1- Detalhamento das Necessidades ': relatorioEndereço.to_dict(orient='records')
    }
    return [data]