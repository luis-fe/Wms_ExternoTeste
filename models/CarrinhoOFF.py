import pandas as pd
import ConexaoPostgreMPL

class Carrinho():
    """Classe que representa o cadastro de Carrinhos no WMS"""
    def __init__(self, NCarrinho = None, empresa = None):

        self.NCarrinho = NCarrinho
        self.empresa = empresa

    def consultarCarrinhos(self):
        '''Metodo criado para consultar todos os carrinhos disponivel por empresa'''

        consulta = """
        select 
            "NCarrinho"
        from 
            "off"."Carrinho"
        where
            empresa = %s
        """

        conn = ConexaoPostgreMPL.conexaoEngine()

        consulta = pd.read_sql(consulta,conn,params=(self.empresa,))

        return consulta
