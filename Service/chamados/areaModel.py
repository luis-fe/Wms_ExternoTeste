import pandas as pd
import ConexaoPostgreMPL

def get_Areas(empresa):
    conn = ConexaoPostgreMPL.conexao()
    queue = pd.read_sql('select * from chamado.area '
                        'where empresa = %s ', conn,params=(empresa,))
    return queue