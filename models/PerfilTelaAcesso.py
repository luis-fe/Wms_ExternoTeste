import pandas as pd
import ConexaoPostgreMPL




class TelaAcesso ():
    '''Classe para interagir com o perfil do usuario'''

    def __init__(self, urlTela = None, nomeTela = None, menu = None):

        self.urlTela = urlTela
        self.nomeTela = nomeTela
        self.menu = menu

    def consultaTelasAcesso(self):
        '''metodo utilizado para consultar as TelaAcesso cadastrado'''

        sql = """
        select
            "urlTela",
            "nomeTela",
            "menu"
        from
            "Reposicao"."TelaAcesso"
        """

        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql,conn)

        return consulta

    def cadastrarTelaAcesso(self):
        '''metodo utilizado para inserir tela de acesso ao TelaAcesso'''

        verifica = self.consultarNomeTelaAcesso()

        if verifica.empty:


            insert = """
            insert into 
                "Reposicao"."TelaAcesso" ("urlTela","nomeTela", "menu")
            values
                (%s, %s, %s)
            """

            with ConexaoPostgreMPL.conexao() as conn:
                with conn.cursor() as curr:
                    curr.execute(insert,(self.urlTela, self.nomeTela, self.menu))
                    conn.commit()

            return pd.DataFrame([{'status':True,"Mensagem":'Tela cadastrada com sucesso'}])

        else:
            return pd.DataFrame([{'status':False,"Mensagem":f'Tela {self.nomeTela }ja existe'}])



    def exclussaoDeTelaAcesso(self):
        '''Metodo utilizado para excluir a TelaAcesso caso ele nao esteja em uso'''

        sql = """
            select 
                * 
            from 
                "Reposicao"."TelaAcessoPerfil"
            where 
                "nomeTela" = %s
        """

        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.nomeTela,))


        if consulta.empty:
            delete = """
            DELETE FROM 
                "Reposicao"."TelaAcesso"
            WHERE 
                "nomeTela" = %s
            """

            with ConexaoPostgreMPL.conexao() as conn:
                with conn.cursor() as curr:
                    curr.execute(delete,(self.nomeTela,))
                    conn.commit()

            return pd.DataFrame([{'status':True,"Mensagem":'Tela excluida com sucesso'}])

        else:
            return pd.DataFrame([{'status':False,"Mensagem":f'Tela {self.nomeTela }esta vinculada a Perfil '}])





    def consultarNomeTelaAcesso(self):
        '''metodo utilizado para consultar as TelaAcesso cadastrado'''

        sql = """
        select
            "urlTela",
            "nomeTela",
            "menu"
        from
            "Reposicao"."TelaAcesso"
        where 
            "nomeTela" = %s
        """

        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.nomeTela,))

        return consulta

