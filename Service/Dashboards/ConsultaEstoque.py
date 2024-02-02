import pandas as pd
import ConexaoPostgreMPL
import ConexaoCSW


def ConsultaEnderecoReposto(natureza, codreduzido = '-', codengenharia = '-', numeroOP = '-'):
    conn = ConexaoPostgreMPL.conexao()

    if codreduzido != '-':
        consulta = 'Select "Endereco", codreduzido, numeroop, codengenharia where natureza = %s '\
                'and codreduzido = %s '

        consulta = pd.read_sql(consulta, conn,params=(codreduzido,))

    elif codengenharia != 'codengenharia':
        consulta = 'Select "Endereco", codreduzido, numeroop, codengenharia where natureza = %s '\
                'and codengenharia = %s '
        consulta = pd.read_sql(consulta, conn,params=(codengenharia,))

    else:
        consulta




    conn.close()