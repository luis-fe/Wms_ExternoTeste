import pandas as pd
import ConexaoPostgreMPL


def RelatorioNecessidadeReposicao():
    conn = ConexaoPostgreMPL.conexao()
    relatorioEndereço = pd.read_sql(
        'select produto as codreduzido , sum(necessidade) as "Necessidade p/repor", count(codpedido) as "Qtd_Pedidos que usam"  from "Reposicao".pedidossku p '
        "where necessidade > 0 and endereco = 'Não Reposto' and codnaturezaatual = '5' "
        " group by produto ", conn)
    relatorioEndereçoEpc = pd.read_sql(
        'select codreduzido , max(epc) as epc_Referencial, engenharia, count(codreduzido) as saldoFila from "Reposicao".filareposicaoportag f '
        "where engenharia is not null and codnaturezaatual = '5' "
        'group by codreduzido, engenharia ', conn)

    OP = pd.read_sql('select f.codreduzido, numeroop as ops, count(codreduzido) as qtde '
                     ' from "Reposicao".filareposicaoportag f group by codreduzido, numeroop',conn)

    OP = OP.sort_values(by='qtde', ascending=False,
                                                      ignore_index=True)  # escolher como deseja classificar

    # Criar uma nova coluna que combina 'OP' e 'qtde' com um hífen
    OP['ops'] =  OP['ops'].astype(str)
    OP['qtde'] = OP['qtde'].astype(str)
    OP['ops'] = OP['ops'] + ':' + OP['qtde']+' Pçs'

    # Agrupar os valores da coluna 'novaColuna' com base na coluna 'reduzido'
    OP_ag = OP.groupby('codreduzido')['ops'].apply(lambda x: ', '.join(x)).reset_index()

    relatorioEndereço = pd.merge(relatorioEndereço, relatorioEndereçoEpc, on='codreduzido', how='left')
    relatorioEndereço = pd.merge(relatorioEndereço, OP_ag, on='codreduzido', how='left')

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