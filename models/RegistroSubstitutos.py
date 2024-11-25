
import pandas as pd
import pytz
import datetime
import ConexaoPostgreMPL


class RegistroSubstitutos():
    '''Classe que registra os item substitutos , que tem restricoes na armazenagem do WMS e na separacao de Pedidos'''

    def __init__(self, empresa = None, usuario = None, numeroop = None, categoria = None, aplicarFiltroCategoria = None, consideraSubs = None):

        self.empresa = empresa
        self.usurio = usuario
        self.numeroop = numeroop
        self.categoria = categoria
        self.aplicarFiltroCategoria = aplicarFiltroCategoria
        self.consideraSubs = consideraSubs

    def consultaSubstitutosPorOPCSW(self):
        '''Método que consulta no CSW os substitutos'''

        if self.aplicarFiltroCategoria  == '':

            conn = ConexaoPostgreMPL.conexao()

            consultar = pd.read_sql(
                'Select categoria as "1-categoria", numeroop as "2-numeroOP", codproduto as "3-codProduto", cor as "4-cor", databaixa_req as "5-databaixa", '
                '"coodigoPrincipal" as "6-codigoPrinc", '
                'nomecompontente as "7-nomePrinc",'
                '"coodigoSubs" as "8-codigoSub",'
                'nomesub as "9-nomeSubst", aplicacao as "10-aplicacao", considera from "Reposicao"."SubstitutosSkuOP" ',
                conn)

            conn.close()

            consultar.fillna('-', inplace=True)

            # Fazer a ordenacao
            consultar = consultar.sort_values(by=['considera', '5-databaixa'],
                                              ascending=False)  # escolher como deseja classificar
            consultar = consultar.drop_duplicates()

            return consultar
        else:
            conn = ConexaoPostgreMPL.conexao()

            consultar = pd.read_sql(
                'Select categoria as "1-categoria", numeroop as "2-numeroOP", codproduto as "3-codProduto", cor as "4-cor", databaixa_req as "5-databaixa", '
                '"coodigoPrincipal" as "6-codigoPrinc", '
                'nomecompontente as "7-nomePrinc",'
                '"coodigoSubs" as "8-codigoSub",'
                'nomesub as "9-nomeSubst",aplicacao as "10-aplicacao",  considera from "Reposicao"."SubstitutosSkuOP" where categoria = %s ',
                conn, params=(self.aplicarFiltroCategoria,))

            conn.close()

            # Fazer a ordenacao
            consultar = consultar.sort_values(by=['considera', '5-databaixa'],
                                              ascending=False)  # escolher como deseja classificar
            consultar.fillna('-', inplace=True)

            consultar = consultar.drop_duplicates()

            return consultar

    def registrarSubstituto(self,arrayOP, arraycor, arraydesconsidera):
        conn = ConexaoPostgreMPL.conexao()

        indice = 0
        for i in range(len(arrayOP)):
            indice = 1 + indice
            self.numeroop = arrayOP[i]
            self.cor = arraycor[i]
            self.consideraSubs = arraydesconsidera[i]
            dataHora = self.obterHoraAtual()

            insert = 'insert into "Reposicao"."RegistroSubstituto" (numeroop, cor, usuario, "dataHoraRegistro", empresa) values (%s, %s, %s, %s, %s)'

            cursor = conn.cursor()

            if self.consideraSubs == 'sim' or self.consideraSubs == 'SIM':
                cursor.execute(insert, (self.numeroop , self.cor, self.usurio, dataHora, self.empresa))
                conn.commit()

            else:
                delete = '''delete from "Reposicao"."RegistroSubstituto" where "numeroop" = %s and empresa = %s and cor = %s  '''
                cursor.execute(delete, (self.numeroop , self.empresa, self.cor))
                conn.commit()


            cursor.close()

        conn.close()
        return pd.DataFrame([{'Mensagem': 'Salvo com sucesso'}])

    def obterHoraAtual(self):
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.datetime.now(fuso_horario)
        hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
        return hora_str

