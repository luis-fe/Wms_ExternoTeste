import ConexaoCSW
import pandas as pd
class ConfiguracaoRevisao():
    '''Classe criada para configruar os clientes que terão os seus pedidos revisados apos a separacao no wms'''
    def __init__(self, codEmpresa=None, diasRetroativo = 180, qtdPecasMinima = 1):

        self.codEmpresa = codEmpresa
        self.diasRetroativo = diasRetroativo
        self.qtdPecasMinima = qtdPecasMinima

    def obterConceitosCsw(self):
        '''Metodo que retorna os conceitos do Csw'''

        sql = """
        SELECT
                c.descricao 
            FROM
                FAT.ConceitoCred c
            WHERE
                c.codEmpresa = 1
        """
        with ConexaoCSW.Conexao() as conn:
            with conn.cursor() as cursor_csw:
                cursor_csw.execute(sql)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        return consulta