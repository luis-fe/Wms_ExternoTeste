from src.WMS_Producao import ConexaoPostgreMPL
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