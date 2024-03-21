import ConexaoPostgreMPL
import pandas as pd


def SubstitutosPorOP(filtro = ''):
   if  filtro == '':
        conn = ConexaoPostgreMPL.conexao()

        consultar = pd.read_sql('Select categoria as "1-categoria", numeroop as "2-numeroOP", codproduto as "3-codProduto", cor as "4-cor", databaixa_req as "5-databaixa", '
                                '"coodigoPrincipal" as "6-codigoPrinc", '
                                'nomecompontente as "7-nomePrinc",'
                                '"coodigoSubs" as "8-codigoSub",'
                                'nomesub as "9-nomeSubst", aplicacao as "10-aplicacao", considera from "Reposicao"."SubstitutosSkuOP" ', conn)

        conn.close()

        consultar.fillna('-',inplace=True)

        # Fazer a ordenacao
        consultar = consultar.sort_values(by=['considera','5-databaixa'], ascending=False)  # escolher como deseja classificar
        consultar = consultar.drop_duplicates()

        return consultar
   else:
       conn = ConexaoPostgreMPL.conexao()

       consultar = pd.read_sql('Select categoria as "1-categoria", numeroop as "2-numeroOP", codproduto as "3-codProduto", cor as "4-cor", databaixa_req as "5-databaixa", '
                               '"coodigoPrincipal" as "6-codigoPrinc", '
                               'nomecompontente as "7-nomePrinc",'
                               '"coodigoSubs" as "8-codigoSub",'
                               'nomesub as "9-nomeSubst",aplicacao as "10-aplicacao",  considera from "Reposicao"."SubstitutosSkuOP" where categoria = %s ', conn, params=(filtro,))

       conn.close()

       # Fazer a ordenacao
       consultar = consultar.sort_values(by=['considera', '5-databaixa'],
                                         ascending=False)  # escolher como deseja classificar
       consultar.fillna('-', inplace=True)

       consultar = consultar.drop_duplicates()

       return consultar

def ObterCategorias():
    conn = ConexaoPostgreMPL.conexao()

    consultar = pd.read_sql('Select distinct categoria from "Reposicao"."SubstitutosSkuOP" ', conn)

    conn.close()


    return consultar

def UpdetaConsidera(arrayOP , arraycor, arraydesconsidera):
    conn = ConexaoPostgreMPL.conexao()

    indice = 0
    for i in range(len(arrayOP)):
        indice = 1 + indice
        op = arrayOP[i]
        cor = arraycor[i]
        considera = arraydesconsidera[i]

        update = 'update "Reposicao"."SubstitutosSkuOP" set considera = %s where numeroop = %s and "cor" = %s'

        cursor = conn.cursor()
        cursor.execute(update,(considera, op, cor,))
        conn.commit()
        cursor.close()


    conn.close()
    return pd.DataFrame([{'Mensagem':'Salvo com sucesso'}])


def PesquisaEnderecoSubstitutoVazio():
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql('select c.codendereco , saldo from "Reposicao"."Reposicao".enderecoporsku sle '
                        'right join "Reposicao"."Reposicao".cadendereco c on c.codendereco = sle.codendereco '
                            "where c.endereco_subst = 'sim' and saldo is null ", conn)

    conn.close()

    consulta['saldo'] = 0

    return consulta


def SugerirEnderecoRestrito(numeroop, ):
    sugestaoEndereco = PesquisaEnderecoSubstitutoVazio()

    if SugerirEnderecoRestrito.empty:

        return pd.DataFrame([{'mensagem':'Atencao! OP selecionada  como "SUBSTUICAO". ',
                            'EnderecoRepor':'Solicitar para Supervisor "endereco de substituto"'}])
    else:


        return pd.DataFrame([{'mensagem':'Atencao! OP selecionada  como "SUBSTUICAO". ',
                            'EnderecoRepor':'Solicitar para Supervisor "endereco de substituto"'}])

