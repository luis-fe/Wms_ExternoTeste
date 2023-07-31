import pandas as pd
import ConexaoPostgreMPL
import numpy
import time


# CLASSE COM AS FUNÇOES PARA INTERAGIR COM AS APIS DE ACESSO DA "REPOSICAO"
def relatorioEndereços ():
    conn = ConexaoPostgreMPL.conexao()
    relatorioEndereço = pd.read_sql('select "Endereco","codreduzido" ,"engenharia" , count(codbarrastag) as saldo, "descricao", cor , tamanho     from "Reposicao".tagsreposicao t   '
                                    'group by "Endereco", "codreduzido" , "engenharia" ,"descricao", cor , tamanho   ',conn)
    conn.close()
    return relatorioEndereço

def relatorioFila ():
    conn = ConexaoPostgreMPL.conexao()
    relatorioFila = pd.read_sql('select "numeroop", count(codbarrastag) as Saldo, engenharia, descricao from "Reposicao".filareposicaoportag t '
                                'group by "numeroop", "engenharia", "descricao" ',conn)
    conn.close()
    return relatorioFila



def relatorioTotalFila():
    conn = ConexaoPostgreMPL.conexao()
    query = pd.read_sql('SELECT numeroop, COUNT(codbarrastag) AS Saldo '
        'FROM "Reposicao".filareposicaoportag t' 
        ' GROUP BY "numeroop" ',conn)

    query2 = pd.read_sql('select *, 1 as contagem from "Reposicao".pedidossku p'
                        " where endereco = 'Não Reposto' and necessidade > 0 and qtdepecasconf = 0",conn)

    query3 = pd.read_sql('select *, 1 as contagem from "Reposicao".pedidossku p'
                        " where endereco <> 'Não Reposto' and necessidade > 0 and qtdepecasconf = 0",conn)

    Inventario = pd.read_sql('select codreduzido  from "Reposicao".tagsreposicao_inventario ti' ,conn)
    Reposto = pd.read_sql('select codreduzido  from "Reposicao".tagsreposicao ti' ,conn)

    query['saldo'] = query['saldo'].sum()
    query2['contagem'] = query2['contagem'].sum()
    query3['contagem'] = query3['contagem'].sum()
    Inventario['codreduzido'] = Inventario['codreduzido'].count()
    Reposto['codreduzido'] = Reposto['codreduzido'].count()
    total =  query3['contagem'][0] +  query2['contagem'][0]

    Percentual = query3['contagem'][0] / total
    Percentual = round(Percentual, 2) * 100
    totalPecas = query["saldo"][0] + Reposto["codreduzido"][0]+Inventario["codreduzido"][0]
    # Aplicando a formatação para exibir como "100.000"
    query['saldo'] = query['saldo'].apply(lambda x: "{:,.0f}".format(x))
    saldo_str= str(query["saldo"][0])
    saldo_str = saldo_str.replace(',', '.')
    totalPecas = "{:,.0f}".format(totalPecas)
    totalPecas = str(totalPecas)
    totalPecas = totalPecas.replace(',', '.')
    total = "{:,.0f}".format(total)
    total = str(total)
    total2 = total.replace(',', '.')


    conn.close()
    data = {
        '1.0':' Informacoes Gerais do Estoque',
        '1.1-Total de Peças Nat. 5':   f'{totalPecas} pçs',
        '1.2-Saldo na Fila':   f'{saldo_str} pçs',
        '1.3-Peçs Repostas':   f'{Reposto["codreduzido"][0]} pçs',
        '1.4-Peçs em Inventario':   f'{Inventario["codreduzido"][0]} pçs',
        '2.0':' Informacoes dos pedidos',
        '2.1- Total de Skus nos Pedidos em aberto ': f'{total2} pçs',
        '2.2-Qtd de Enderecos Nao Reposto em Pedido': f'{query2["contagem"][0]}',
        '2.3-Qtd de Enderecos OK Reposto nos Pedido': f'{query3["contagem"][0]}',
        '2.4- Percentual Reposto':f'{Percentual}%'
    }
    return [data]


def RelatorioNecessidadeReposicao():
    conn = ConexaoPostgreMPL.conexao()
    relatorioEndereço = pd.read_sql('select produto as codreduzido , sum(necessidade) as necessidade_Pedidos, count(codpedido) as Qtd_Pedidos  from "Reposicao".pedidossku p '
                                    "where necessidade > 0 and endereco = 'Não Reposto' "
                                    " group by produto ",conn)
    relatorioEndereçoEpc = pd.read_sql('select codreduzido , max(epc) as epc_Referencial, engenharia from "Reposicao".filareposicaoportag f '
                                       'where epc is not null and engenharia is not null '
                                       'group by codreduzido, engenharia',conn)

    relatorioEndereço = pd.merge(relatorioEndereço,relatorioEndereçoEpc,on='codreduzido',how='left')
    # Clasificando o Dataframe para analise
    relatorioEndereço = relatorioEndereço.sort_values(by='necessidade_pedidos', ascending=False, ignore_index=True)  # escolher como deseja classificar
                                    
    conn.close()
    data = {
                
                '1- Detalhamento das Necessidades ':relatorioEndereço.to_dict(orient='records')
            }
    return [data]
def InformacaoPedidoViaTag(codbarras):
    conn = ConexaoPostgreMPL.conexao()

    Informa = pd.read_sql('Select codpedido, usuario, dataseparacao  from "Reposicao".tags_separacao '
                          'where codbarrastag = '+"'"+codbarras+"'",conn)

    Informa2 = pd.read_sql('select codigopedido as codpedido, codcliente, desc_cliente, desc_tiponota  from "Reposicao".filaseparacaopedidos',conn)

    Informa = pd.merge(Informa,Informa2,on='codpedido',how='left')

    return Informa


def EnderecosDisponiveis():
    conn = ConexaoPostgreMPL.conexao()
    relatorioEndereço = pd.read_sql(
        'select codendereco, contagem as saldo from "Reposicao"."enderecosReposicao" '
        'where contagem = 0 ', conn)
    relatorioEndereço2 = pd.read_sql(
        'select codendereco, contagem as saldo from "Reposicao"."enderecosReposicao" '
        ' ', conn)
    TaxaOcupacao = 1-(relatorioEndereço["codendereco"].size/relatorioEndereço2["codendereco"].size)
    TaxaOcupacao = round(TaxaOcupacao, 2) * 100
    tamanho = relatorioEndereço["codendereco"].size
    tamanho2 = relatorioEndereço2["codendereco"].size
    tamanho2 = "{:,.0f}".format(tamanho2)
    tamanho2 = str(tamanho2)
    tamanho2 = tamanho2.replace(',', '.')
    tamanho = "{:,.0f}".format(tamanho)
    tamanho = str(tamanho)
    tamanho = tamanho.replace(',', '.')

    conn.close()
    data = {

        '1- Total de Enderecos Naturarez 5': tamanho2,
        '2- Total de Enderecos Disponiveis': tamanho,
        '3- Taxa de Oculpaçao dos Enderecos': f'{TaxaOcupacao} %',
        '4- Enderecos disponiveis ': relatorioEndereço.to_dict(orient='records')
    }
    return [data]

def RelatorioSeparadores(itensPag, pagina):

    conn = ConexaoPostgreMPL.conexao()
    relatorio = pd.read_sql('select * from "Reposicao".tags_separacao order by dataseparacao desc',conn)
    Usuarios = pd.read_sql('Select codigo as usuario, nome from "Reposicao".cadusuarios ', conn)
    Usuarios['usuario'] = Usuarios['usuario'].astype(str)
    relatorio = pd.merge(relatorio, Usuarios, on='usuario', how='left')
    final = pagina * itensPag
    inicial = (pagina - 1) * itensPag
    relatorio = relatorio.iloc[inicial:final]
    relatorio = pd.to_datetime(relatorio['horario'], format='%H:%M:%S')


    return relatorio