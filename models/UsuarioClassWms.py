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

    def __init__(self, codigo=None, login=None, nome=None, situacao=None, funcaoWMS=None, senha=None):
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
        INSERT INTO "WMS"."Wms".usuario
            (matricula, funcao, nome, login, situacao)
        VALUES
            (%s, %s, %s, %s, %s)
        """
        try:
            with conexao.WmsConnectionClass().conectar() as conn:
                with conn.cursor() as curr:
                    curr.execute(insert, (self.codigo, self.funcaoWMS, self.nome, self.login, 'Ativo'))
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
            "WMS"."Wms".usuario us
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
