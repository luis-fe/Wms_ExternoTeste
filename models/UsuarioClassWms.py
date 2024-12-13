import pandas as pd
from connection import WmsConnectionClass as conexao
import ConexaoPostgreMPL
class Usuario:
    """
    Classe que representa os usuários do sistema WMS.

    Atributos:
        codigo (str): Código ou matrícula do usuário.
        login (str): Login de acesso do usuário.
        nome (str): Nome completo do usuário.
        situacao (str): Situação atual do usuário (Ativo/Inativo).
        funcaoWMS (str): Função desempenhada no sistema WMS.
        senha (str): Senha de acesso do usuário.
    """

    def __init__(self, codigo=None, login=None, nome=None, situacao=None, funcaoWMS=None, senha=None, perfil = None):
        """
        Construtor da classe Usuario.

        Args:
            codigo (str, opcional): Código ou matrícula do usuário.
            login (str, opcional): Login do usuário.
            nome (str, opcional): Nome completo do usuário.
            situacao (str, opcional): Situação do usuário.
            funcaoWMS (str, opcional): Função do usuário no sistema WMS.
            senha (str, opcional): Senha de acesso do usuário.
        """
        self.codigo = codigo
        self.login = login
        self.nome = nome
        self.situacao = situacao
        self.funcaoWMS = funcaoWMS
        self.senha = senha
        self.perfil = perfil

    def getUsuarios(self):
        """
        Obtém todos os usuários cadastrados na plataforma.

        Returns:
            pd.DataFrame: DataFrame com os dados dos usuários.
        """
        sqlGetUsuarios = """
            SELECT
                *
            FROM
                "Reposicao"."cadusuarios"
            order by nome
        """
        try:
            with ConexaoPostgreMPL.conexao() as conn:
                with conn.cursor() as curr:
                    curr.execute(sqlGetUsuarios)
                    usuarios = curr.fetchall()
                    colunas = [desc[0] for desc in curr.description]
                    dataframe = pd.DataFrame(usuarios, columns=colunas)
                    dataframe.fillna('NAO', inplace=True)
            return dataframe
        except Exception as e:
            print(f"Erro ao obter usuários: {e}")
            return pd.DataFrame()

    def inserirUsuario(self):
        """
        Insere um novo usuário na plataforma WMS.

        Returns:
            bool: True se a inserção for bem-sucedida, False caso contrário.
        """
        insert = """
        INSERT INTO "Reposicao"."Reposicao".cadusuarios
            (codigo, funcao, nome, login, situacao, perfil, senha)
        VALUES
            (%s, %s, %s, %s, %s, %s)
        """
        try:
            with ConexaoPostgreMPL.conexao() as conn:
                with conn.cursor() as curr:
                    curr.execute(insert, (self.codigo, self.funcaoWMS, self.nome, self.login, 'ATIVO',self.perfil, self.senha))
                    conn.commit()
            return True
        except Exception as e:
            print(f"Erro ao inserir usuário: {e}")
            return False

    def consultaUsuarioSenha(self):
        """
        Consulta a existência de um usuário com base no código e senha.

        Returns:
            bool: True se o usuário e senha corresponderem, False caso contrário.
        """
        query = """
        SELECT 
            COUNT(*)
        FROM 
            "Reposicao"."Reposicao".cadusuarios us
        WHERE 
            codigo = %s AND senha = %s
        """
        try:
            with conexao.WmsConnectionClass().conectar() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (self.codigo, self.senha))
                    result = cursor.fetchone()[0]
            return result == 1
        except Exception as e:
            print(f"Erro ao consultar usuário e senha: {e}")
            return False

    def consultaUsuario(self):
        conn = ConexaoPostgreMPL.conexao()
        cursor = conn.cursor()
        codigo = int(self.codigo)
        cursor.execute("""
                select
                       codigo, 
                       nome, 
                       funcao, 
                       situacao, 
                       empresa, 
                       perfil, 
                       login 
                from 
                    "Reposicao"."cadusuarios" c'
                where 
                    codigo = %s
                       """, (codigo,))
        usuarios = cursor.fetchall()
        cursor.close()
        conn.close()
        if not usuarios:
            return 0, 0, 0, 0, 0
        else:
            return usuarios[0][1], usuarios[0][2], usuarios[0][3], usuarios[0][4], usuarios[0][5], usuarios[0][6], usuarios[0][7]

    def PesquisarSenha(self):
        '''Api usada para restricao de pesquisa de senha dos usuarios '''

        conn = ConexaoPostgreMPL.conexao()
        cursor = conn.cursor()
        cursor.execute('select codigo, nome, senha from "Reposicao"."cadusuarios" c')
        usuarios = cursor.fetchall()
        cursor.close()
        conn.close()

        return usuarios



