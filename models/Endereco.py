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
        '''Metodo utilizado para validar se extite o endereco no WMS'''

        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = 'select codendereco  FROM "Reposicao"."Reposicao".cadendereco c  ' \
                   ' where "codendereco" = %s  and empresa = %s'
        consulta = pd.read_sql(consulta, conn, params=(self.endereco,self.empresa))

        if consulta.empty:
            return pd.DataFrame([{'Mensagem': f'Erro! O endereco {self.endereco} nao esta cadastrado, contate o supervisor.',
                                  'status': False}])
        else:
            return pd.DataFrame([{'status': True,'Mensagem':'endereco existe !'}])

    def cadEndereco(self):
        '''Metodo utilizado para cadastrar um novo endereco '''


        inserir = 'insert into "Reposicao".cadendereco ("codendereco","rua","modulo","posicao")' \
                  ' VALUES (%s,%s,%s,%s);'
        codenderco = self.rua + "-" + self.modulo + "-" + self.posicao

        conn = ConexaoPostgreMPL.conexao()
        cursor = conn.cursor()
        cursor.execute(inserir
                       , (codenderco, self.rua, self.modulo, self.posicao))

        # Obter o número de linhas afetadas
        numero_linhas_afetadas = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        return codenderco
