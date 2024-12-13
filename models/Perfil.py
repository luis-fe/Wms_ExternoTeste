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


        delete = """
        delete from "Reposicao"."TelaAcessoPerfil" where "codPerfil" = %s
        
        """

        with ConexaoPostgreMPL.conexao() as conn:
            with conn.cursor() as curr:
                curr.execute(delete, (self.codPerfil,))
                conn.commit()

        for t in self.telaAcesso:

            self.nomeTela = t

            insert = """
            insert into 
                "Reposicao"."Reposicao"."TelaAcessoPerfil" ("codPerfil", "nomeTela")
                values (%s , %s )
            """

            with ConexaoPostgreMPL.conexao() as conn:
                with conn.cursor() as curr:
                    curr.execute(insert,(self.codPerfil, self.nomeTela))
                    conn.commit()


    def cadastrarOuAtualizarPerfil(self):
        '''Metodo utilizado para cadastrar Perfil '''

        verfica = self.consultarPerfilPorCodigo()

        if verfica.empty:


            insert = '''
            insert into 
                "Reposicao"."Pefil" ("codPerfil","nomePerfil")
            values ( %s, %s )
            '''

            with ConexaoPostgreMPL.conexao() as conn:
                with conn.cursor() as curr:
                    curr.execute(insert,(self.codPerfil, self.nomePerfil))
                    conn.commit()

            self.inserirTelaAcessoAoPerfil()

            return pd.DataFrame([{'status':True, 'Mensagagem':f'Perfil {self.codPerfil}-{self.nomePerfil} inserido com sucesso !'}])

        else:
            self.updatePerfil()
            self.inserirTelaAcessoAoPerfil()
            return pd.DataFrame([{'status':True, 'Mensagagem':f'Perfil {self.codPerfil}-{self.nomePerfil} atualziado com sucesso !'}])



    def exclussaoDePerfil(self):
        '''Metodo utilizado para excluir o perfil caso ele nao esteja em uso'''


    def updatePerfil(self):
        '''Metodo utilizado para update do Perfil '''

        update = """
        update 
            "Reposicao"."Pefil" 
        set 
            "nomePerfil" = %s
        where
            "codPerfil" = %s  
        """

        with ConexaoPostgreMPL.conexao() as conn:
            with conn.cursor() as curr:
                curr.execute(update,(self.nomePerfil, self.codPerfil))
                conn.commit()


    def consultarPerfilPorCodigo(self):
        '''Metodo utilizado para consultar o perfil pelo codigo '''

        sql = """
                select
	        "codPerfil",
	        "nomePerfil"
        from
	        "Reposicao"."Reposicao"."Pefil" p 
	    where 
	        "codPerfil" = %s
        """

        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql,conn,params=(self.codPerfil,))

        return consulta


    def descobrircodPerfil(self):
        ''' metodo utilizado para encontrar o nome do perfil '''

        sql = """
                        select
        	        "codPerfil",
        	        "nomePerfil"
                from
        	        "Reposicao"."Reposicao"."Pefil" p 
        	    where 
        	        "nomePerfil" = %s
                """

        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.nomePerfil,))

        if consulta.empty:
            self.codPerfil = None
        else:
            self.codPerfil = consulta['codPerfil'][0]

        return self.codPerfil





