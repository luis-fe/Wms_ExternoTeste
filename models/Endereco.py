import pandas as pd
import ConexaoPostgreMPL


class Endereco ():
    '''Classe criada para entidade Endereco do WMS '''

    def __init__(self, endereco = None, empresa = None, rua = None, modulo = None, posicao = None, natureza = None ):

        self.endereco = endereco
        self.empresa = str(empresa)
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


        inserir = 'insert into "Reposicao".cadendereco ("codendereco","rua","modulo","posicao", "codempresa" )' \
                  ' VALUES (%s,%s,%s,%s,%s);'
        codenderco = self.rua + "-" + self.modulo + "-" + self.posicao

        conn = ConexaoPostgreMPL.conexao()
        cursor = conn.cursor()
        cursor.execute(inserir
                       , (codenderco, self.rua, self.modulo, self.posicao, self.empresa))

        # Obter o número de linhas afetadas
        numero_linhas_afetadas = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        return codenderco

    def obeterEnderecos(self):
        '''Metodo que consulta todos os enderecos disponiveis no WMS '''
        conn = ConexaoPostgreMPL.conexaoEngine()
        endercos = pd.read_sql(
            ' select codempresa, natureza, endereco, tipo from "Reposicao"."cadendereco" ce   where codempresa = %s', conn, params=(self.empresa,))

        endercos.fillna('-',inplace=True)

        return endercos

    def enderecosDisponiveis(self):
        '''Metodo que avalia a disponibilidade dos enderecos em estoque'''

        conn = ConexaoPostgreMPL.conexaoEngine()

        # 1. Aqui carrego os enderecos do banco de dados, por uma view chamado enderecosReposicao,
        # essa view mostra o saldo de cada endereco cadastrado na plataforma, por empresa e por natureza

        # 1.1 Carregando somente os enderecos com saldo = 0
        relatorioEndereco = pd.read_sql(
            'select codendereco, contagem as saldo from "Reposicao"."enderecosReposicao" '
            'where contagem = 0 and natureza = %s and codempresa = %s ', conn, params=(self.natureza,self.empresa))

        # 1.2 Carregando todos os enderecos
        relatorioEndereco2 = pd.read_sql(
            'select codendereco, contagem as saldo from "Reposicao"."enderecosReposicao" where natureza = %s and codempresa = %s '
            ' ', conn, params=(self.natureza,self.empresa))

        # Calculando a Taxa de Ocupacao
        TaxaOcupacao = 1 - (relatorioEndereco["codendereco"].size / relatorioEndereco2["codendereco"].size)
        TaxaOcupacao = round(TaxaOcupacao, 2) * 100

        # Calculando o "tamanho"  de cada uma das consultas
        tamanho = relatorioEndereco["codendereco"].size
        tamanho2 = relatorioEndereco2["codendereco"].size
        tamanho2 = "{:,.0f}".format(tamanho2)
        tamanho2 = str(tamanho2)
        tamanho2 = tamanho2.replace(',', '.')
        tamanho = "{:,.0f}".format(tamanho)
        tamanho = str(tamanho)
        tamanho = tamanho.replace(',', '.')


        # Exibindo as informacoes para o Json
        data = {

            '1- Total de Enderecos Natureza ': tamanho2,
            '2- Total de Enderecos Disponiveis': tamanho,
            '3- Taxa de Oculpaçao dos Enderecos': f'{TaxaOcupacao} %',
            '4- Enderecos disponiveis ': relatorioEndereco.to_dict(orient='records')
        }
        return [data]

    def deletar_Endereco(self):
        '''Metodo utilizado para excluir um endereco do WMS'''


        conn = ConexaoPostgreMPL.conexao()
        # Validar se existe Restricao Para excluir o endereo
        Validar = pd.read_sql(
            'select "Endereco" from "Reposicao".tagsreposicao '
            'where "Endereco" = %s and codempresa = %s ', conn, params=(self.endereco, self.empresa))


        if not Validar.empty:
            conn.close()
            return pd.DataFrame({'Mensagem': [f'Endereco com saldo, nao pode ser excluido'], 'Status': False})

        else:
            delatar = 'delete from "Reposicao".cadendereco ' \
                      'where codendereco = %s '
            # Execute a consulta usando a conexão e o cursor apropriados
            cursor = conn.cursor()
            cursor.execute(delatar, (Endereco,))
            conn.commit()

            conn.close()
            return pd.DataFrame({'Mensagem': [f'Endereco excluido!'], 'Status': True})


