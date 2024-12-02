import pandas as pd
import ConexaoPostgreMPL


class CaixaOFF():
    '''Modulo utilizado para interagir com o objeto Caixa do Wms'''

    def __init__(self, Ncaixa = None , empresa = None):

        self.Ncaixa = Ncaixa
        self.empresa = empresa

    def caixasAbertas(self):
        ''' Metodo utilizado para consulta as caixas em aberto (com tags ainda nao entradas no estoque) '''

        sql = """
            select distinct 
                rq.caixa, 
                rq.usuario 
            from 
                "Reposicao"."off".reposicao_qualidade rq '
            where 
                rq.codempresa  = %s order by caixa
        """


        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.empresa,))

        # Obter os usuarios cadastrados para realizar o merge
        Usuarios = pd.read_sql('Select codigo as usuario, nome from "Reposicao".cadusuarios ', conn)
        Usuarios['usuario'] = Usuarios['usuario'].astype(str)
        consulta = pd.merge(consulta, Usuarios, on='usuario', how='left')

        #Obter as tag já bipadas na caixa
        BipadoSKU = pd.read_sql(
            'select numeroop, codreduzido, count(rq.codreduzido) as bipado_sku_OP from "Reposicao"."off".reposicao_qualidade rq  group by codreduzido, numeroop ',
            conn)
        consulta = pd.merge(consulta, BipadoSKU, on='codreduzido', how='left')
        consulta.fillna('-',inplace=True)

        return consulta

