import pandas as pd

import ConexaoCSW
import ConexaoPostgreMPL


def RelatorioNecessidadeReposicao():
    conn = ConexaoPostgreMPL.conexao()
    relatorioEndereçoPedidosSku = pd.read_sql(
        """
        select produto as codreduzido , sum(necessidade) as "Necessidade p/repor", count(codpedido) as "Qtd_Pedidos que usam", max(ts.engenharia) as engenharia  from "Reposicao".pedidossku p 
inner join "Reposicao"."Reposicao"."Tabela_Sku" ts on ts.codreduzido = p.produto 
where necessidade > 0 and endereco = 'Não Reposto'
group by produto 
        """, conn)

    relatorioEndereçoEpc = pd.read_sql(
        """select f.codreduzido , max(epc) as epc_Referencial, count(f.codreduzido) as saldoFila
        from "Reposicao".filareposicaoportag f
        where f.engenharia is not null and codnaturezaatual = '5'
        group by f.codreduzido
        """, conn)



    OP = pd.read_sql('select f.codreduzido, numeroop as ops, count(codreduzido) as qtde '
                     ' from "Reposicao".filareposicaoportag f '
                     " where engenharia is not null and codnaturezaatual = '5' "
                     ' group by codreduzido, numeroop',conn)

    reservaEndereco = pd.read_sql('select codreduzido, sum("SaldoLiquid") as "DisponivelPrateleira"  from "Reposicao"."Reposicao"."calculoEndereco" ce '
                                  ' where ce."SaldoLiquid" > 0 '
                                                         " and natureza = '5'"
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

    relatorioEndereço = pd.merge(relatorioEndereçoPedidosSku, relatorioEndereçoEpc, on='codreduzido', how='left')
    relatorioEndereço = pd.merge(relatorioEndereço, OP_ag, on='codreduzido', how='left')
    relatorioEndereço = pd.merge(relatorioEndereço, reservaEndereco, on='codreduzido', how='left')

    # Clasificando o Dataframe para analise
    relatorioEndereço = relatorioEndereço.sort_values(by='Necessidade p/repor', ascending=False,
                                                      ignore_index=True)  # escolher como deseja classificar

    relatorioEndereço = relatorioEndereço[relatorioEndereço['engenharia']!= '-']

    pedidos = pd.read_sql('select codpedido, produto as codreduzido, sum(necessidade) as "necessidadePedido" ,'
                          '(select f.desc_tiponota from "Reposicao".filaseparacaopedidos f where f.codigopedido = p.codpedido) as desc_tiponota'
                          ' from "Reposicao".pedidossku p '
                          "where p.necessidade > 0 and p.reservado = 'nao' "
                          " group by codpedido, produto", conn)
    pedidos1 = pedidos.groupby('codreduzido')['codpedido'].agg(', '.join).reset_index()
    pedidos['necessidadePedido'] = pedidos['necessidadePedido'].astype(str)
    pedidos2 = pedidos.groupby('codreduzido')['necessidadePedido'].agg(', '.join).reset_index()
    pedidos['desc_tiponota'].fillna('-',inplace =True)
    pedidos3 = pedidos.groupby('codreduzido')['desc_tiponota'].agg(', '.join).reset_index()


    #pedidos = pedidos.groupby(['codreduzido']).agg({'codpedido': list, 'necessidadePedido': list}).reset_index()


    relatorioEndereço = pd.merge(relatorioEndereço, pedidos1, on='codreduzido', how='left')
    relatorioEndereço = pd.merge(relatorioEndereço, pedidos2, on='codreduzido', how='left')
    relatorioEndereço = pd.merge(relatorioEndereço, pedidos3, on='codreduzido', how='left')
    relatorioEndereço.fillna('-', inplace=True)
    conn.close()
    data = {

        '1- Detalhamento das Necessidades ': relatorioEndereço.to_dict(orient='records')
    }
    return [data]

#Essa Funcao se aplica para distribuir os enderecos restantes que nao conseguimos distribuir no RECARREGAR PEDIDOS
def RelatorioNecessidadeReposicaoDisponivel(empresa, natureza):
    conn = ConexaoPostgreMPL.conexao() #Abre a conexao com o Postgre

#EPATA 1: LEVANTA O TODTAL DE NECESSIDADE POR SKU (SOMENTE O QUE NAO RESERVO) E A QUANTIDADE DE PEDIDOS QUE USAM ESSE SKU (QUE NAO RESERERVO)

    if natureza == '5':
        consulta = """
            select 
                produto as codreduzido , 
                sum(necessidade) as "Necessidade p/repor", 
                count(codpedido) as "Qtd_Pedidos que usam"  
            from 
                "Reposicao".pedidossku p
            where 
                necessidade > 0 and endereco = 'Não Reposto' and 
                p.codpedido in (
                                    select
                                        codigopedido
                                     from
                                        "Reposicao"."Reposicao".filaseparacaopedidos f
                                    where
                                        codtiponota <> '64'
                                    )
            group by produto 
            """
        relatorioEndereço = pd.read_sql(consulta, conn)

    elif natureza == '7':
        consulta = """
            select 
                produto as codreduzido , 
                sum(necessidade) as "Necessidade p/repor", 
                count(codpedido) as "Qtd_Pedidos que usam"  
            from 
                "Reposicao".pedidossku p
            where 
                necessidade > 0 and endereco = 'Não Reposto' and 
                p.codpedido in (
                                    select
                                        codigopedido
                                     from
                                        "Reposicao"."Reposicao".filaseparacaopedidos f
                                    where
                                        codtiponota = '64'
                                    )
            group by produto 
            """
        relatorioEndereço = pd.read_sql(consulta, conn)

    else:
        consulta = """
            select 
                produto as codreduzido , 
                sum(necessidade) as "Necessidade p/repor", 
                count(codpedido) as "Qtd_Pedidos que usam"  
            from 
                "Reposicao".pedidossku p
            where 
                necessidade > 0 and endereco = 'Não Reposto' and 
                p.codpedido in (
                                    select
                                        codigopedido
                                     from
                                        "Reposicao"."Reposicao".filaseparacaopedidos f
                                    where
                                        codtiponota <> '64'
                                    )
            group by produto 
            """
        relatorioEndereço = pd.read_sql(consulta, conn)


# EPATA 2: LEVANTA O ESTOQUE DISPONIVEL NA PRATELEIRA POR SKU E POR ENGENHARIA
    relatorioEndereçoEpc = pd.read_sql(
        'select codreduzido , max(epc) as epc_Referencial, engenharia, count(codreduzido) as saldoFila from "Reposicao".filareposicaoportag f '
        "where engenharia is not null and codnaturezaatual = %s "
        'group by codreduzido, engenharia ', conn, params=(natureza,))
# EPATA 3: LEVANTA O ESTOQUE DA FILA A REPOR POR SKU E POR ENGENHARIA E POR OP
    OP = pd.read_sql('select f.codreduzido, numeroop as ops, count(codreduzido) as qtde '
                     ' from "Reposicao".filareposicaoportag f '
                     " where engenharia is not null and codnaturezaatual = %s "
                     ' group by codreduzido, numeroop',conn,params=(natureza,))
    OP = OP.sort_values(by='qtde', ascending=False,ignore_index=True)

# ETAPA 4: REALIZA UM TRATAMENTO NAS OPS PARA  QUE POSSA REALIZAR UMA OPERACAO DE AGRUPAMENTO NO DATAFRAME
    OP['ops'] = OP['ops'].astype(str)
    OP['ops'] = OP['ops'].str.replace('-001', '')

    OP['qtde'] = OP['qtde'].astype(str)
    OP['ops'] = OP['ops'] + ': ' + OP['qtde'] + 'Pç'
    OP_ag = OP.groupby('codreduzido')['ops'].apply(lambda x: ' / '.join(x)).reset_index()

# EPATA 5: LEVANTA A DISPONIBILIDADE LIQUIDA DE CADA SKU
    reservaEndereco = pd.read_sql('select codreduzido, sum("SaldoLiquid") as "DisponivelPrateleira"  from "Reposicao"."Reposicao"."calculoEndereco" ce '
                                  ' where ce."SaldoLiquid" > 0 and natureza = %s '
                                  ' group by codreduzido',conn,params=(natureza,))
# ETAPA 6: REALIZA UM MERGE PARA OBTER UM UNICO DATAFRAME CHAMADO *** relatorioEndereço *****
    relatorioEndereço = pd.merge(relatorioEndereço, relatorioEndereçoEpc, on='codreduzido', how='left')
    relatorioEndereço = pd.merge(relatorioEndereço, OP_ag, on='codreduzido', how='left')
    relatorioEndereço = pd.merge(relatorioEndereço, reservaEndereco, on='codreduzido', how='left')

# ETAPA 7: REALIZA A CLASSIFICACAO E TRATAMENTO DOS DADOS PARA MELHOR VISUALIZACAO , MANTENDO APENAS OS PEDIDOS DISPONIVEIS NA PRATELEIRA
    relatorioEndereço = relatorioEndereço.sort_values(by='Necessidade p/repor', ascending=False,
                                                      ignore_index=True)  # escolher como deseja classificar
    relatorioEndereço.fillna('-', inplace=True)
    relatorioEndereço = relatorioEndereço[relatorioEndereço['DisponivelPrateleira'] != '-']

# ETAPA 8 : REALIZA UM LEVANTAMENTO DOS PEDIDOS X SKU QUE ESTAO PENDENTES DE RESERVA E DEPOIS E FEITO UM GROUP BY DOS SKU

    if natureza == '5':

        consulta = """
        select codpedido, produto as codreduzido from "Reposicao".pedidossku p
        where p.necessidade > 0 and p.reservado = 'nao' and 
                p.codpedido in (
                select
	codigopedido
from
	"Reposicao"."Reposicao".filaseparacaopedidos f
where
	codtiponota <> '64'
                )
        """

        pedidos = pd.read_sql(consulta,conn)
        pedidos = pedidos.groupby('codreduzido')['codpedido'].agg(', '.join).reset_index()
    elif natureza == '7':

        consulta = """
        select codpedido, produto as codreduzido from "Reposicao".pedidossku p
        where p.necessidade > 0 and p.reservado = 'nao' and 
                p.codpedido in (
                select
	codigopedido
from
	"Reposicao"."Reposicao".filaseparacaopedidos f
where
	codtiponota = '64'
                )
        """

        pedidos = pd.read_sql(consulta, conn)
        pedidos = pedidos.groupby('codreduzido')['codpedido'].agg(', '.join).reset_index()


    else:
        pedidos = pd.read_sql('select codpedido, produto as codreduzido from "Reposicao".pedidossku p '
                              "where p.necessidade > 0 and p.reservado = 'nao' ",conn)
        pedidos = pedidos.groupby('codreduzido')['codpedido'].agg(', '.join).reset_index()


# ETAPA 9: REALIZA O MERGE DOS CODIGOS REDUZIDO
    relatorioEndereço = pd.merge(relatorioEndereço, pedidos, on='codreduzido', how='left')
    relatorioEndereço.fillna('-', inplace=True)

# ETAOA 10: É FEITO UM LEVANTAMENTO INDIVIDUALIZADO DOS PEDIDOS E PRODUTOS QUE NECESSITAM SER TRATADOS ESPECIALMENTE COMO SUBSTITUTOS
    DataFramePedidosEspeciais = """
    select p.codpedido as pedido, produto from "Reposicao"."Reposicao".pedidossku p 
inner join "Reposicao"."Reposicao"."Tabela_Sku" ts on ts.codreduzido = p.produto 
where engenharia ||cor in (
select t.engenharia||cor from "Reposicao"."Reposicao".tagsreposicao t 
        where t.resticao  like '%||%')
    """
    DataFramePedidosEspeciais = pd.read_sql(DataFramePedidosEspeciais,conn)

# ETAPA 11: É FEITO UMA ITERACAO LINHA A LINHA DO RELATORIO DE PRODUTOS DISPONIVEIS PARA TENTAR FAZER UMA RESERVA DE ENDERECO
    for i in range(relatorioEndereço['codpedido'].count()):
        pedido = relatorioEndereço.loc[i, 'codpedido'].split(', ')[0]
        produto = relatorioEndereço['codreduzido'][i]

        if pedido == '-':
            print('-')
        else:
            # ETAPA 11.1 Verificar se o pedido é de cor/engenharia especial E CASO FOR, DISTRIBUI USANDO OS ENDERECOS ESPECIAIS NA FUNCAO *** DistribuirPedidosEspeciais
            avaliar = DataFramePedidosEspeciais[
                (DataFramePedidosEspeciais['pedido'] == pedido) & (DataFramePedidosEspeciais['produto'] == produto)]
            if not avaliar.empty and natureza == '5':
                DistribuirPedidosEspeciais(pedido,str(produto),natureza)


            else:
                Redistribuir(pedido,str(produto),natureza)

    conn.close()#Fecha a conexao com o Postgre

    data = {

        '1- Detalhamento das Necessidades ': relatorioEndereço.to_dict(orient='records')
    }
    return [data]


def Redistribuir(pedido, produto, natureza):
    conn = ConexaoPostgreMPL.conexao()

    query = """
    select ce.codendereco as endereco , ce."SaldoLiquid"  , '1-normal' as status
    from "Reposicao"."Reposicao"."calculoEndereco" ce
    where ce.natureza = %s and ce.produto = %s and ce."SaldoLiquid" > 0 
    and codendereco not in 
        (select t."Endereco" from "Reposicao"."Reposicao".tagsreposicao t 
        where t.resticao  like %s )
    order by status, "SaldoLiquid" desc 
    """

    EnderecosDisponiveis = pd.read_sql(query,conn,params=(natureza, produto,'%||%'))

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


def DistribuirPedidosEspeciais(pedido, produto, natureza):
    query = """
        select ce.codendereco as endereco , ce."SaldoLiquid", resticao as status
    from "Reposicao"."Reposicao"."calculoEndereco" ce
    inner join (select "Endereco", max(resticao) as resticao from "Reposicao"."Reposicao".tagsreposicao t  group by "Endereco")data2 on data2."Endereco" = ce.codendereco  
    where ce.natureza = %s and ce.produto = %s and ce."SaldoLiquid" > 0 
    and codendereco in 
        (select t."Endereco" from "Reposicao"."Reposicao".tagsreposicao t 
        where t.resticao like %s )
        order by status, "SaldoLiquid" desc 
    """
    conn = ConexaoPostgreMPL.conexao()
    EnderecosDisponiveis = pd.read_sql(query,conn,params=(natureza, produto,'%||%'))
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



def PedidosEspeciais(codpedido, produto):

    consulta = """
    select p.codpedido, produto from "Reposicao"."Reposicao".pedidossku p 
inner join "Reposicao"."Reposicao"."Tabela_Sku" ts on ts.codreduzido = p.produto 
where engenharia ||cor in (
select t.engenharia||cor from "Reposicao"."Reposicao".tagsreposicao t 
        where t.resticao not like '||') and codpedido = %s and produto = %s 
    """

    conn = ConexaoPostgreMPL.conexao()
    consulta = pd.read_sql(consulta, conn, params=(codpedido, produto,))
    conn.close()

    if consulta.empty:
        return False
    else:
        return True
