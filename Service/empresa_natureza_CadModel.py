import ConexaoPostgreMPL
import pandas as pd
def ObterNaturezas():
    conn = ConexaoPostgreMPL.conexao()
    qurey = pd.read_sql('select * from "Reposicao".configuracoes ',conn)

    return qurey