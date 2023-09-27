import ConexaoPostgreMPL
import pandas as pd


def detalhaSku(codreduzido, empresa,natureza):
    conn = ConexaoPostgreMPL.conexao()
    df_op2 = pd.read_sql('select "Endereco", "codreduzido", "descricao", count("codreduzido") as saldo, natureza '
                   'from "Reposicao"."tagsreposicao" frt where "codreduzido" = ' +"'"+  codreduzido +"' and natureza = '"+natureza+"'"
                   ' group by "Endereco", "codreduzido", "descricao", natureza ', conn)
    if df_op2.empty:
        return pd.DataFrame({'Mensagem':[f'O reduzido {codreduzido} ainda nao foi reposto ou esta com as prateleiras vazias ']})
    else:
        return df_op2

def DetalhaTag(codbarras):
    conn = ConexaoPostgreMPL.conexao()

    consulta1 = pd.read_sql('Select  codbarrastag, codreduzido, descricao, natureza, "Endereco", '
                            " 'reposto' as situacao "
                            'from "Reposicao".tagsreposicao_inventario  '
                            'where codbarrastag = %s ', conn, params=(codbarras,))

    if not consulta1.empety:
        return consulta1
    elif consulta1.empty:
        consulta2 = pd.read_sql("Select  codbarrastag, codreduzido, descricao, codnaturezaatual  as natureza, 'na fila' as situacao "
                                ' from "Reposicao".filareposicaoportag'
                                'where codbarrastag = %s ', conn, params=(codbarras,))
        if not consulta2.empty:
            return consulta2
        elif consulta2.empty:
            consulta3 = pd.read_sql('Select  codbarrastag, codreduzido, descricao, natureza, "Endereco", '
                                    " 'reposto' as situacao "
                                    'from "Reposicao".tagsreposicao_inventario  '
                                    'where codbarrastag = %s ', conn, params=(codbarras,))
            return consulta3
        else:
            return pd.DataFrame([{'Mensagem':'Tag nao encontrada em nenhum local'}])
