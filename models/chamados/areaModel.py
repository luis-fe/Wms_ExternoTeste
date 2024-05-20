import pandas as pd
import ConexaoPostgreMPL

def get_Areas(empresa):
    conn = ConexaoPostgreMPL.conexao()
    queue = pd.read_sql('select * from chamado.area '
                        'where empresa = %s ', conn,params=(empresa,))
    return queue

def Atribuir_por_Area(empresa, area):
    conn = ConexaoPostgreMPL.conexao()
    queue = pd.read_sql('select responsavel from chamado.area '
                        'where empresa = %s and area = %s', conn,params=(empresa,area,))
    return queue['responsavel'][0]


