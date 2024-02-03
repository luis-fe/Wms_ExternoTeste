import pandas as pd
import ConexaoPostgreMPL
import ConexaoCSW


def ConsultaEnderecoReposto(natureza, codreduzido = '-', codengenharia = '-', numeroOP = '-', endereco = '-', limit = 100):
    conn = ConexaoPostgreMPL.conexao()

    totalFila = pd.read_sql('select count(codbarrastag) as SaldoPecas from "Reposicao".filareposicaoportag '
                'where codnaturezaatual = %s ', conn, params=(natureza,))

    if codreduzido != '-' :
        consulta = 'Select  "Endereco", codreduzido, t.engenharia , count(codbarrastag) as saldo from "Reposicao".tagsreposicao t '  \
                   'where natureza = %s '\
                'and codreduzido = %s ' \
                   'group by "Endereco", codreduzido,  engenharia ' \
                   'order by "Endereco" asc  limit  %s '

        InformacoesAdicionais = 'select distinct codreduzido, descricao, tamanho, cor  from "Reposicao".tagsreposicao  t' \
                                ' where codreduzido = %s '

        consulta = pd.read_sql(consulta, conn,params=(natureza,codreduzido,limit,))
        InformacoesAdicionais = pd.read_sql(InformacoesAdicionais, conn , params=(codreduzido,))

        consulta = pd.merge(consulta, InformacoesAdicionais,on='codreduzido',how='left')

        totalFila = pd.read_sql('select count(codbarrastag) as SaldoPecas from "Reposicao".filareposicaoportag '
                                'where codnaturezaatual = %s and codreduzido = %s ', conn, params=(natureza,codreduzido,))



    elif codreduzido == '-' and codengenharia != '-' :
        consulta = 'Select  "Endereco", codreduzido, t.engenharia , count(codbarrastag) as saldo from "Reposicao".tagsreposicao t ' \
                   ' where natureza = %s '\
                'and engenharia = %s ' \
                   'group by "Endereco", codreduzido,  engenharia ' \
                   'order by "Endereco" asc  limit  %s '

        InformacoesAdicionais = 'select distinct codreduzido, descricao, tamanho, cor  from "Reposicao".tagsreposicao  ' \
                                ' where engenharia = %s '

        consulta = pd.read_sql(consulta, conn,params=(natureza,codengenharia,limit,))


        InformacoesAdicionais = pd.read_sql(InformacoesAdicionais, conn , params=(codengenharia,))

        consulta = pd.merge(consulta, InformacoesAdicionais,on='codreduzido',how='left')

        totalFila = pd.read_sql('select count(codbarrastag) as SaldoPecas from "Reposicao".filareposicaoportag '
                                'where codnaturezaatual = %s and engenharia = %s ', conn, params=(natureza,codengenharia,))


    else:
        consulta = 'Select  "Endereco", codreduzido, t.engenharia , count(codbarrastag) as saldo from "Reposicao".tagsreposicao t ' \
                   ' where natureza = %s '\
                'and natureza = %s ' \
                   'group by "Endereco", codreduzido, engenharia ' \
                   'order by "Endereco" asc  limit  %s '

        consulta = pd.read_sql(consulta, conn,params=(natureza,natureza,limit,))

    conn.close()
    data = {

        '1- Fila Reposicao ': f'{totalFila["saldopecas"][0]} pçs',
        '2- Em estoque': 'tamanho',
        '3- Detalhamento ': consulta.to_dict(orient='records')
    }
    return pd.DataFrame([data])




