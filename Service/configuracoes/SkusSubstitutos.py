import ConexaoPostgreMPL
import pandas as pd


def SubstitutosPorOP(filtro = ''):
   if  filtro == '':
        conn = ConexaoPostgreMPL.conexao()

        consultar = pd.read_sql('Select categoria, numeroop, codproduto, cor, databaixa_req as databaixa, '
                                '"coodigoPrincipal" as "codigoPrinc", '
                                'nomecompontente as "nomePrinc",'
                                '"coodigoSubs" as "codigoSub",'
                                'nomesub as "nomeSubst", aplicacao, considera from "Reposicao"."SubstitutosSkuOP" ', conn)

        conn.close()

        consultar['cor'].fillna('-',inplace=True)

        # Fazer a ordenacao
        consultar = consultar.sort_values(by=['considera','databaixa'], ascending=False)  # escolher como deseja classificar

        return consultar
   else:
       conn = ConexaoPostgreMPL.conexao()

       consultar = pd.read_sql('Select categoria, numeroop, codproduto, cor, databaixa_req as databaixa, '
                               '"coodigoPrincipal" as "codigoPrinc", '
                               'nomecompontente as "nomePrinc",'
                               '"coodigoSubs" as "codigoSub",'
                               'nomesub as "nomeSubst",aplicacao,  considera from "Reposicao"."SubstitutosSkuOP" where categoria = %s ', conn, params=(filtro,))

       conn.close()

       # Fazer a ordenacao
       consultar = consultar.sort_values(by=['considera', 'databaixa'],
                                         ascending=False)  # escolher como deseja classificar
       consultar['cor'].fillna('-', inplace=True)

       return consultar

def ObterCategorias():
    conn = ConexaoPostgreMPL.conexao()

    consultar = pd.read_sql('Select distinct categoria from "Reposicao"."SubstitutosSkuOP" ', conn)

    conn.close()


    return consultar

def UpdetaConsidera(arrayOP , arrayCompSub, arrayconsidera):
    conn = ConexaoPostgreMPL.conexao()

    indice = 0
    for i in range(len(arrayOP)):
        indice = 1 + indice
        op = arrayOP[i]
        compSub = arrayCompSub[i]
        considera = arrayconsidera[i]

        update = 'update "Reposicao"."SubstitutosSkuOP" set considera = %s where numeroop = %s and "coodigoSubs" = %s'

        cursor = conn.cursor()
        cursor.execute(update,(considera, op, compSub,))
        conn.commit()
        cursor.close()





    conn.close()
    return pd.DataFrame([{'Mensagem':'Salvo com sucesso'}])
