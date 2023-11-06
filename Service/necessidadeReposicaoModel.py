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
        "where engenharia is not null and codnaturezaatual = '5' "
        'group by codreduzido, engenharia ', conn)

    OP = pd.read_sql('select f.codreduzido, numeroop as ops, count(codreduzido) as qtde '
                     ' from "Reposicao".filareposicaoportag f '
                     " where engenharia is not null and codnaturezaatual = '5' "
                     ' group by codreduzido, numeroop',conn)

    reservaEndereco = pd.read_sql('select codreduzido, sum("SaldoLiquid") as "DisponivelPrateleira"  from "Reposicao"."Reposicao"."calculoEndereco" ce '
                                  ' where ce."SaldoLiquid" > 0'
                                  ' group by codreduzido',conn)

    OP = OP.sort_values(by='qtde', ascending=False,
                                                      ignore_index=True)  # escolher como deseja classificar

    # Criar uma nova coluna que combina 'OP' e 'qtde' com um hífen
    OP['ops'] =  OP['ops'].astype(str)
    OP['ops'] = OP['ops'].str.replace('-001', '')

    OP['qtde'] = OP['qtde'].astype(str)
    OP['ops'] = OP['ops'] + ': ' + OP['qtde']+'Pç'
    # Agrupar os valores da coluna 'novaColuna' com base na coluna 'reduzido'
    OP_ag = OP.groupby('codreduzido')['ops'].apply(lambda x: ' / '.join(x)).reset_index()

    relatorioEndereço = pd.merge(relatorioEndereço, relatorioEndereçoEpc, on='codreduzido', how='left')
    relatorioEndereço = pd.merge(relatorioEndereço, OP_ag, on='codreduzido', how='left')
    relatorioEndereço = pd.merge(relatorioEndereço, reservaEndereco, on='codreduzido', how='left')

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


def RelatorioNecessidadeReposicaoDisponivel():
    conn = ConexaoPostgreMPL.conexao()
    relatorioEndereço = pd.read_sql(
        'select produto as codreduzido , sum(necessidade) as "Necessidade p/repor", count(codpedido) as "Qtd_Pedidos que usam"  from "Reposicao".pedidossku p '
        "where necessidade > 0 and endereco = 'Não Reposto' "
        " group by produto ", conn)
    relatorioEndereçoEpc = pd.read_sql(
        'select codreduzido , max(epc) as epc_Referencial, engenharia, count(codreduzido) as saldoFila from "Reposicao".filareposicaoportag f '
        "where engenharia is not null and codnaturezaatual = '5' "
        'group by codreduzido, engenharia ', conn)

    OP = pd.read_sql('select f.codreduzido, numeroop as ops, count(codreduzido) as qtde '
                     ' from "Reposicao".filareposicaoportag f '
                     " where engenharia is not null and codnaturezaatual = '5' "
                     ' group by codreduzido, numeroop',conn)

    reservaEndereco = pd.read_sql('select codreduzido, sum("SaldoLiquid") as "DisponivelPrateleira"  from "Reposicao"."Reposicao"."calculoEndereco" ce '
                                  ' where ce."SaldoLiquid" > 0'
                                  ' group by codreduzido',conn)

    OP = OP.sort_values(by='qtde', ascending=False,
                                                      ignore_index=True)  # escolher como deseja classificar

    # Criar uma nova coluna que combina 'OP' e 'qtde' com um hífen
    OP['ops'] =  OP['ops'].astype(str)
    OP['ops'] = OP['ops'].str.replace('-001', '')

    OP['qtde'] = OP['qtde'].astype(str)
    OP['ops'] = OP['ops'] + ': ' + OP['qtde']+'Pç'
    # Agrupar os valores da coluna 'novaColuna' com base na coluna 'reduzido'
    OP_ag = OP.groupby('codreduzido')['ops'].apply(lambda x: ' / '.join(x)).reset_index()

    relatorioEndereço = pd.merge(relatorioEndereço, relatorioEndereçoEpc, on='codreduzido', how='left')
    relatorioEndereço = pd.merge(relatorioEndereço, OP_ag, on='codreduzido', how='left')
    relatorioEndereço = pd.merge(relatorioEndereço, reservaEndereco, on='codreduzido', how='left')

    # Clasificando o Dataframe para analise
    relatorioEndereço = relatorioEndereço.sort_values(by='Necessidade p/repor', ascending=False,
                                                      ignore_index=True)  # escolher como deseja classificar
    relatorioEndereço.fillna('-', inplace=True)
    relatorioEndereço = relatorioEndereço[relatorioEndereço['engenharia']!= '-']
    relatorioEndereço = relatorioEndereço[relatorioEndereço['DisponivelPrateleira'] != '-']

    pedidos = pd.read_sql('select codpedido, produto as codreduzido from "Reposicao".pedidossku p '
                          "where p.necessidade > 0 and p.reservado = 'nao' ",conn)
    pedidos = pedidos.groupby('produto')['codpedido'].agg(', '.join).reset_index()

    relatorioEndereço = pd.merge(relatorioEndereço, pedidos, on='codreduzido', how='left')


    conn.close()

    data = {

        '1- Detalhamento das Necessidades ': relatorioEndereço.to_dict(orient='records')
    }
    return [data]