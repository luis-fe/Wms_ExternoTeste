import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd


def detalhaSku(codreduzido, empresa,natureza):
    conn = ConexaoPostgreMPL.conexao()
    df_op2 = pd.read_sql('select "Endereco", "codreduzido", "descricao", count("codreduzido") as saldo, natureza '
                   'from "Reposicao"."tagsreposicao" frt where "codreduzido" = ' +"'"+  codreduzido +"' "
                                                                                                     "and natureza = '"+natureza+"'"# Aguardando ate o Sergio arrumar
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

    if not consulta1.empty:
        return consulta1

    else:
        consulta2 = pd.read_sql("Select  codbarrastag, codreduzido, descricao, codnaturezaatual  as natureza, 'na fila' as situacao "
                                ' from "Reposicao".filareposicaoportag '
                                'where codbarrastag = %s ', conn, params=(codbarras,))
        if not consulta2.empty:
            return consulta2

        else:
            consulta3 = pd.read_sql('Select  codbarrastag, codreduzido, descricao, natureza, "Endereco", '
                                    " 'reposto' as situacao "
                                    'from "Reposicao".tagsreposicao_inventario  '
                                    ' where codbarrastag = %s ', conn, params=(codbarras,))
            if not consulta3.empty:
                return consulta3
            else:
                consulta4 = ConexaoCSW.pesquisaTagCSW(codbarras)
                if not consulta4.empty and consulta4['situacao'][0] == 3:
                    consulta4['Mensagem'] = 'A tag nao foi encontrada no WMS mais está certa no CSW.'
                    return consulta4
                elif consulta4['situacao'][0] == 999:
                    return pd.DataFrame([{'Mensagem1':'Nao foi possivel encontrar a tag no WMS',
                                          'Mensagem2':'No CSW a Conexao foi perida, nao foi possivel apurar onde ta a tag'}])

                else:

                    return pd.DataFrame([{'Mensagem':'Tag nao encontrada em nenhum local'}])
