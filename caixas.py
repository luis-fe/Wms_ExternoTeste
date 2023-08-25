import pandas as pd
import ConexaoPostgreMPL

def Buscar_Caixas():
    conn = ConexaoPostgreMPL.conexao()
    query = pd.read_sql('select * from "Reposicao".caixas',conn)
    conn.close()

    return query