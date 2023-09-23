import pandas as pd
import ConexaoPostgreMPL


def RelatorioNecessidadeReposicao():
    conn = ConexaoPostgreMPL.conexao()
    relatorioEndereço = pd.read_sql(
        'select produto as codreduzido , sum(necessidade) as "Necessidade p/repor", count(codpedido) as "Qtd_Pedidos que usam"  from "Reposicao".pedidossku p '
        "where necessidade > 0 and endereco = 'Não Reposto' "
        " group by produto ", conn)
    relatorioEndereçoEpc = pd.read_sql(
        'select codreduzido , max(epc) as epc_Referencial, engenharia, count(codreduzido) as saldoFila from "Reposicao".filareposicaoportag f '
        'where epc is not null and engenharia is not null '
        'group by codreduzido, engenharia', conn)

    OP = pd.read_sql('select f.codreduzido, numeroop as OP, count(codreduzido) as qtde '
                     ' from "Reposicao".filareposicaoportag f group by codreduzido, numeroop',conn)

    # Agrupar os valores da coluna 'qtde' com base na coluna 'OP'
    OP = OP.groupby('OP')['qtde'].apply(lambda x: ', '.join(map(str, x))).reset_index()

    # Renomear a coluna resultante
    OP.columns = ['OP', 'OPs']

    relatorioEndereço = pd.merge(relatorioEndereço, relatorioEndereçoEpc, on='codreduzido', how='left')
    relatorioEndereço = pd.merge(relatorioEndereço, OP, on='codreduzido', how='left')

    # Clasificando o Dataframe para analise
    relatorioEndereço = relatorioEndereço.sort_values(by='Necessidade p/repor', ascending=False,
                                                      ignore_index=True)  # escolher como deseja classificar
    relatorioEndereço.fillna('-', inplace=True)
    relatorioEndereço = relatorioEndereço[relatorioEndereço['engenharia']!= '-']

    conn.close()
    data = {

        '1- Detalhamento das Necessidades ': relatorioEndereço.to_dict(orient='records')
    }
    return [data]