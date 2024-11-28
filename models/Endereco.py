import pandas as pd

import ConexaoPostgreMPL


class Endereco ():
    '''Classe criada para entidade Endereco do WMS '''

    def __init__(self, endereco = None, empresa = None, rua = None, modulo = None, posicao = None, natureza = None ):

        self.endereco = endereco
        self.empresa = empresa
        self.rua = rua
        self.modulo = modulo
        self.posicao = posicao
        self.natureza = natureza

    def validaEndereco(self):
        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = 'select codendereco  FROM "Reposicao"."Reposicao".cadendereco c  ' \
                   ' where "codendereco" = %s  and empresa = %s'
        consulta = pd.read_sql(consulta, conn, params=(self.endereco,self.empresa))

        if consulta.empty:
            return pd.DataFrame([{'Mensagem': f'Erro! O endereco {self.endereco} nao esta cadastrado, contate o supervisor.',
                                  'status': False}])
        else:
            return pd.DataFrame([{'status': True,'Mensagem':'endereco existe !'}])
