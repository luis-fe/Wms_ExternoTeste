import pandas as pd

import ConexaoCSW
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

    relatorioEndereço = relatorioEndereço[relatorioEndereço['engenharia']!= '-']

    pedidos = pd.read_sql('select codpedido, produto as codreduzido, sum(necessidade) as "necessidadePedido"  from "Reposicao".pedidossku p '
                          "where p.necessidade > 0 and p.reservado = 'nao' "
                          " group by codpedido, produto", conn)
    pedidos1 = pedidos.groupby('codreduzido')['codpedido'].agg(', '.join).reset_index()
    pedidos['necessidadePedido'] = pedidos['necessidadePedido'].astype(str)
    pedidos2 = pedidos.groupby('codreduzido')['necessidadePedido'].agg(', '.join).reset_index()

    #pedidos = pedidos.groupby(['codreduzido']).agg({'codpedido': list, 'necessidadePedido': list}).reset_index()


    relatorioEndereço = pd.merge(relatorioEndereço, pedidos1, on='codreduzido', how='left')
    relatorioEndereço = pd.merge(relatorioEndereço, pedidos2, on='codreduzido', how='left')
    relatorioEndereço.fillna('-', inplace=True)
    conn.close()
    data = {

        '1- Detalhamento das Necessidades ': relatorioEndereço.to_dict(orient='records')
    }
    return [data]


def RelatorioNecessidadeReposicaoDisponivel(empresa, natureza):
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
                                  ' where ce."SaldoLiquid" > 0 and natureza = %s '
                                  ' group by codreduzido',conn,params=(natureza,))

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
    #relatorioEndereço = relatorioEndereço[relatorioEndereço['engenharia']!= '-']
    relatorioEndereço = relatorioEndereço[relatorioEndereço['DisponivelPrateleira'] != '-']

    pedidos = pd.read_sql('select codpedido, produto as codreduzido from "Reposicao".pedidossku p '
                          "where p.necessidade > 0 and p.reservado = 'nao' ",conn)
    pedidos = pedidos.groupby('codreduzido')['codpedido'].agg(', '.join).reset_index()

    relatorioEndereço = pd.merge(relatorioEndereço, pedidos, on='codreduzido', how='left')

    for i in range(relatorioEndereço['codpedido'].count()):
        pedido = relatorioEndereço.loc[i, 'codpedido'].split(', ')[0]
        produto = relatorioEndereço['codreduzido'][i]

        Redistribuir(pedido,str(produto),natureza)

    conn.close()

    data = {

        '1- Detalhamento das Necessidades ': relatorioEndereço.to_dict(orient='records')
    }
    return [data]


def Redistribuir(pedido, produto, natureza):
    conn = ConexaoPostgreMPL.conexao()

    EnderecosDisponiveis = pd.read_sql('select ce.codendereco as endereco , ce."SaldoLiquid"  from "Reposicao"."Reposicao"."calculoEndereco" ce '
                                       'where ce.natureza = %s and ce.produto = %s and ce."SaldoLiquid" > 0 order by ce."SaldoLiquid" desc ',conn,params=(natureza, produto,))

    tamanho = EnderecosDisponiveis['endereco'].count()
    if tamanho >= 0:
        for i in range(tamanho):
            pedidosku = pd.read_sql('select * from "Reposicao".pedidossku '
                                    "where produto = %s and codpedido = %s and necessidade > 0 and endereco = 'Não Reposto' "
                                    , conn, params=(produto, pedido))



            endereco_i= EnderecosDisponiveis['endereco'][i]
            saldo_i = EnderecosDisponiveis['SaldoLiquid'][i]

            if not pedidosku.empty:
                sugerido = pedidosku['qtdesugerida'][0]
                data_Hora = pedidosku['datahora'][0]

                if sugerido <= saldo_i:
                    qurery = 'update "Reposicao".pedidossku ' \
                             "set endereco = %s, reservado = 'sim' " \
                             "where produto = %s and codpedido = %s and necessidade > 0 and endereco = 'Não Reposto' "

                    cursor = conn.cursor()
                    cursor.execute(qurery, (endereco_i, produto, pedido,))
                    conn.commit()
                    cursor.close()

                elif sugerido > saldo_i:
                    insert = 'insert into "Reposicao".pedidossku ' \
                             '(codpedido, produto, qtdesugerida, qtdepecasconf, endereco, necessidade, datahora, valorunitarioliq, reservado) ' \
                             'values (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
                    cursor = conn.cursor()
                    cursor.execute(insert, (pedido, produto, saldo_i, 0, endereco_i, saldo_i,data_Hora,str(i), 'sim'))
                    conn.commit()
                    nova_sugerido = sugerido - saldo_i

                    update = 'update "Reposicao".pedidossku ' \
                             'set qtdesugerida = %s, necessidade = %s ' \
                             "where produto = %s and codpedido = %s and necessidade > 0 and endereco = 'Não Reposto' "
                    cursor.execute(update, (nova_sugerido, nova_sugerido, produto, pedido,))
                    conn.commit()

                    cursor.close()


                else:

                    print('fim')
            else:
                print('fim')

        return pd.DataFrame([{'status': True, 'Mensagem': 'ok'}])
    else:
        return pd.DataFrame([{'status': False, 'Mensagem': 'Tamanho é iqual a 0', 'natureza':natureza}])





