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


    def inserirTelaAcessoAoPerfil(self):
        '''metodo utilizado para inserir tela de acesso ao perfil'''


    def cadastrarPerfil(self):
        '''Metodo utilizado para cadastrar Perfil '''


    def exclussaoDePerfil(self):
        '''Metodo utilizado para excluir o perfil caso ele nao esteja em uso'''


    def updatePerfil(self):
        '''Metodo utilizado para update do Perfil '''