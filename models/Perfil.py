import pandas as pd
import ConexaoPostgreMPL




class Perfil ():
    '''Classe para interagir com o perfil do usuario'''

    def __init__(self, codPerfil = None, nomePerfil = None, telasAcesso = None):

        self.codPerfil = codPerfil
        self.nomePerfil = nomePerfil
        self.telaAcesso = telasAcesso

    def consultaPerfil(self):
        '''metodo utilizado para consultar os perfis cadastrado'''

        sql1 = """
        select
	        "codPerfil",
	        "nomePerfil"
        from
	        "Reposicao"."Reposicao"."Pefil" p 
        """


        sql2 = """
        select
            "codPerfil",
            "nomeTela"
        from
            "Reposicao"."Reposicao"."TelaAcessoPerfil" p
                """


        conn = ConexaoPostgreMPL.conexaoEngine()
        sql1 = pd.read_sql(sql1,conn)

        sql2 = pd.read_sql(sql2,conn)

        merged = pd.merge(sql1, sql2, on='codPerfil', how='left')


        # Agrupa mantendo todas as colunas do DataFrame planos e transforma lotes e nomelote em arrays
        grouped = merged.groupby(['codPerfil', 'nomePerfil']).agg({
            'nomeTela': lambda x: list(x.dropna().astype(str).unique())
        }).reset_index()

        return grouped


    def inserirTelaAcessoAoPerfil(self):
        '''metodo utilizado para inserir tela de acesso ao perfil'''


    def cadastrarPerfil(self):
        '''Metodo utilizado para cadastrar Perfil '''


    def exclussaoDePerfil(self):
        '''Metodo utilizado para excluir o perfil caso ele nao esteja em uso'''


    def updatePerfil(self):
        '''Metodo utilizado para update do Perfil '''