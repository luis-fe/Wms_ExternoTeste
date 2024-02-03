import pandas as pd
import ConexaoPostgreMPL
import ConexaoCSW


def ConsultaEnderecoReposto(natureza, codreduzido = '-', codengenharia = '-', numeroOP = '-', endereco = '-', limit = 100):
    conn = ConexaoPostgreMPL.conexao()

    if codreduzido != '-' :
        consulta = 'Select  "Endereco", codreduzido, t.engenharia , count(codbarrastag) as saldo from "Reposicao".tagsreposicao t '  \
                   'where natureza = %s '\
                'and codreduzido = %s ' \
                   'group by "Endereco", codreduzido,  engenharia ' \
                   'order by "Endereco" asc  limit  %s '

        InformacoesAdicionais = 'select codreduzido, descricao, tamanho, cor  from "Reposicao".tagsreposicao  t' \
                                ' where codreduzido = %s '

        consulta = pd.read_sql(consulta, conn,params=(natureza,codreduzido,limit,))
        InformacoesAdicionais = pd.read_sql(InformacoesAdicionais, conn , params=(codreduzido,))

        consulta = pd.merge(consulta, InformacoesAdicionais,on='codreduzido',how='left')



    elif codreduzido == '-' and codengenharia != '-' :
        consulta = 'Select  "Endereco", codreduzido, t.engenharia , count(codbarrastag) as saldo from "Reposicao".tagsreposicao t ' \
                   ' where natureza = %s '\
                'and engenharia = %s ' \
                   'group by "Endereco", codreduzido,  engenharia ' \
                   'order by "Endereco" asc  limit  %s '

        InformacoesAdicionais = 'select codreduzido, descricao, tamanho, cor  from "Reposicao".tagsreposicao  ' \
                                ' where engenharia = %s '

        consulta = pd.read_sql(consulta, conn,params=(natureza,codreduzido,limit,))


        InformacoesAdicionais = pd.read_sql(InformacoesAdicionais, conn , params=(codreduzido,))

        consulta = pd.merge(consulta, InformacoesAdicionais,on='codreduzido',how='left')


    else:
        consulta = 'Select  "Endereco", codreduzido, t.engenharia , count(codbarrastag) as saldo from "Reposicao".tagsreposicao t ' \
                   ' where natureza = %s '\
                'and natureza = %s ' \
                   'group by "Endereco", codreduzido, engenharia ' \
                   'order by "Endereco" asc  limit  %s '

        consulta = pd.read_sql(consulta, conn,params=(natureza,natureza,limit,))

    conn.close()
    consulta.fillna('-', inplace=True)

    return consulta