import pandas as pd
import ConexaoPostgreMPL
import ConexaoCSW


def ConsultaEnderecoReposto(natureza, codreduzido = '-', codengenharia = '-', numeroOP = '-', endereco = '-', limit = 100):
    conn = ConexaoPostgreMPL.conexao()

    if codreduzido != '-' and codengenharia == '-' and numeroOP == '-' and endereco == '-':
        consulta = 'Select "Endereco", codreduzido, numeroop, codengenharia, count(codbarrastag) pçs from "Reposicao".tagsreposicao ' \
                   'where natureza = %s '\
                'and codreduzido = %s ' \
                   'group by "Endereco", codreduzido, numeroop, codengenharia'

        consulta = pd.read_sql(consulta, conn,params=(codreduzido,))

    elif codreduzido == '-' and codengenharia != '-' and numeroOP == '-' and endereco == '-':
        consulta = 'Select "Endereco", codreduzido, numeroop, codengenharia, count(codbarrastag) pçs from "Reposicao".tagsreposicao ' \
                   'where natureza = %s '\
                'and codengenharia = %s ' \
                   'group by "Endereco", codreduzido, numeroop, codengenharia'

        consulta = pd.read_sql(consulta, conn,params=(codreduzido,))

    else:
        consulta = 'Select  "Endereco", codreduzido, numeroop, t.engenharia , count(codbarrastag) as saldo from "Reposicao".tagsreposicao t ' \
                   'where natureza = %s '\
                'and natureza = %s ' \
                   'group by "Endereco", codreduzido, numeroop, engenharia ' \
                   'order by "Endereco" asc  limit = %s '

        consulta = pd.read_sql(consulta, conn,params=(natureza,natureza,limit,))

    conn.close()

    return consulta