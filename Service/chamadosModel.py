import pandas as pd
import ConexaoPostgreMPL

def Obter_chamados():
    conn = ConexaoPostgreMPL.conexao()
    query = pd.read_sql('select * from "Reposicao".chamados '
                        'order by data_chamado', conn)
    conn.close()
    return query
