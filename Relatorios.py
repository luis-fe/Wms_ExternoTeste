import pandas as pd
import ConexaoPostgreMPL


# CLASSE COM AS FUNÇOES PARA INTERAGIR COM AS APIS DE ACESSO DA "REPOSICAO"
def relatorioEndereços ():
    conn = ConexaoPostgreMPL.conexao()
    relatorioEndereço = pd.read_sql('select "Endereco","codreduzido" ,"engenharia" , count(codbarrastag) as saldo, "descricao", cor , tamanho, natureza     from "Reposicao".tagsreposicao t   '
                                    'group by "Endereco", "codreduzido" , "engenharia" ,"descricao", cor , tamanho, natureza   ',conn)
    conn.close()
    relatorioEndereço.fillna('5', inplace=True)

    return relatorioEndereço

def relatorioFila ():
    conn = ConexaoPostgreMPL.conexao()
    relatorioFila = pd.read_sql('select "numeroop", count(codbarrastag) as Saldo, engenharia, descricao, codnaturezaatual as natureza, codempresa as empresa from "Reposicao".filareposicaoportag t '
                                'group by "numeroop", "engenharia", "descricao", codnaturezaatual, codempresa ',conn)
    conn.close()
    return relatorioFila

def EnderecosDisponiveis(natureza, empresa):
    conn = ConexaoPostgreMPL.conexao()
    relatorioEndereço = pd.read_sql(
        'select codendereco, contagem as saldo from "Reposicao"."enderecosReposicao" '
        'where contagem = 0 and natureza = %s ', conn, params=(natureza,))
    relatorioEndereço2 = pd.read_sql(
        'select codendereco, contagem as saldo from "Reposicao"."enderecosReposicao" where natureza = %s'
        ' ', conn, params=(natureza,))
    TaxaOcupacao = 1-(relatorioEndereço["codendereco"].size/relatorioEndereço2["codendereco"].size)
    TaxaOcupacao = round(TaxaOcupacao, 2) * 100
    TaxaOcupacao = "{:,.0f}".format(TaxaOcupacao)
    TaxaOcupacao = str(TaxaOcupacao)
    TaxaOcupacao = TaxaOcupacao.replace(',', '.')


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

        '1- Total de Enderecos Natureza ': tamanho2,
        '2- Total de Enderecos Disponiveis': tamanho,
        '3- Taxa de Oculpaçao dos Enderecos': f'{TaxaOcupacao} %',
        '4- Enderecos disponiveis ': relatorioEndereço.to_dict(orient='records')
    }
    return [data]




