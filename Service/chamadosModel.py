import pandas as pd
import ConexaoPostgreMPL

def Obter_chamados():
    conn = ConexaoPostgreMPL.conexao()
    query = pd.read_sql('select id_chamado, solicitante, data_chamado'
                        ' tipo_chamado, descricao_chamado, status_chamado, '
                        'data_finalizacao_chamado from "Reposicao".chamados '
                        'order by data_chamado', conn)
    query.fillna('-', inplace=True)

    conn.close()
    return query
