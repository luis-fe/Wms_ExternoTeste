import pandas as pd
import ConexaoPostgreMPL

def Get_escalaTrabalho():
    conn = ConexaoPostgreMPL
    query = pd.read_sql("select * from configuracoes.escala_trabalho",conn)
    return query
