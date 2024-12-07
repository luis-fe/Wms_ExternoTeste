import gc
import pandas as pd
import pytz
import datetime
import ConexaoCSW
import ConexaoPostgreMPL
import psycopg2


class Reposicao():
    '''classe criado para a entidade reposicao, utilizada no processo de Reposicao de Pçs na prateleira no WMS'''

    def __init__(self, codbarrastag = None, endereco = None, empresa = None, usuario = None, natureza = None, Ncaixa = None ):
        '''Contrutor da classe'''

        self.codbarrastag = codbarrastag
        self.endereco = endereco
        self.empresa = empresa
        self.usuario = usuario
        self.natureza = natureza
        self.Ncaixa = Ncaixa
        self.DataHora = self.obterHoraAtual()
        self.detalhaErro = ''
        self.situacaoTagCsw = ''

    def avalicaoOcupacaoEndereco(self):
        '''Metodo utilizado para avaliar a oculpacao do endereco '''

        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = 'select distinct codreduzido from "Reposicao"."Reposicao".tagsreposicao ' \
                   ' where "Endereco" = %s '
        consulta = pd.read_sql(consulta, conn, params=(self.endereco,))

        ## avaliando se está vazio:
        if not consulta.empty:

            contagem = consulta['codreduzido'].count()

            if contagem == 1:
                self.codreduzido = str(consulta["codreduzido"][0])

                return pd.DataFrame(
                    [{'Mensagem': f'Endereco está cheio, com o sequinte sku {consulta["codreduzido"][0]}', 'status': True,
                      'codreduzido': self.codreduzido}])
            else:
                return pd.DataFrame(
                    [{'Mensagem': f'Endereco está cheio, com o sequinte sku {consulta["codreduzido"][0]}', 'status': False,
                      'codreduzido': 'varios'}])

        else:
            return pd.DataFrame([{'status': 'OK! Pronto para usar', 'codreduzido': '-'}])

    def validarSituacaoTags(self,dataframTAG):
        '''Metodo que avalia a situacao das tags junto ao ERP CSW, returnando True caso esteja ok'''

        # transformando o data frame em string separado por ','
        resultado = '({})'.format(', '.join(["'{}'".format(valor) for valor in dataframTAG['codbarrastag']]))
        emp = self.empresa

        sql = """
        SELECT 
            p.codBarrasTag as codbarrastag , 
            p.situacao , 
            p.codNaturezaAtual  
        FROM 
            Tcr.TagBarrasProduto p 
        where 
            codempresa = """ + emp + """
            and codBarrasTag in """ + resultado


        with ConexaoCSW.Conexao() as conn:
            with conn.cursor() as cursor_csw:
                cursor_csw.execute(sql)
                colunas = [desc[0] for desc in cursor_csw.description]
                # Busca todos os dados
                rows = cursor_csw.fetchall()
                # Cria o DataFrame com as colunas
                consulta = pd.DataFrame(rows, columns=colunas)
                del rows
                gc.collect()


        # AGRUPANDO as situacoes
        consulta['ocorrencia'] = 1
        totalCaixa = consulta['ocorrencia'].sum()
        consultaSituacao = consulta.groupby(['situacao']).agg({
            # 'usuario':'first',
            'ocorrencia': 'count'}).reset_index()
        consultaSituacao = consultaSituacao[consultaSituacao['situacao'] == 3]
        totalCaixaSit3 = consultaSituacao['ocorrencia'].sum()
        consulta2 = consulta[consulta['situacao'] != 3].reset_index()
        resultado2 = '({})'.format(', '.join(["'{}'".format(valor) for valor in consulta2['codbarrastag']]))

        if totalCaixa == totalCaixaSit3:
            for _, row in consulta.iterrows():
                self.codbarrastag = row['codbarrastag']
                self.situacaoTagCsw = row['situacao']
                self.natureza = row['codNaturezaAtual']

                self.delete_registroPendenciasRecarregarEndereco()

            return pd.DataFrame([{'status': True}])
        else:

            for _, row in consulta.iterrows():
                self.codbarrastag = row['codbarrastag']
                self.situacaoTagCsw = row['situacao']
                self.natureza = row['codNaturezaAtual']

                self.put_registroPendenciasRecarregarEndereco()

            return pd.DataFrame([{'status': False,
                                  'Mensagem': f'Erro! As Tags {resultado2} nao  estao na situacao 3 em estoque, verificar junto ao supervisor'}])


    def get_registroPendenciasRecarregarEndereco(self):
        '''metodo criado para registrar as pendencias de tags do endereco recarregado para chekList do supervisor '''

        sql = """
        select 
            codbarrastag,
            usuario,
            empresa,
            endereco as "enderecoTentativa",
            "dataHora"
            "situacaoTagCsw",
            "natureza",
            "detalhaErro"
        from
            "Reposicao"."RegistroPendencia" 
        where 
            empresa = %s
        """


        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.empresa))

        return consulta

    def put_registroPendenciasRecarregarEndereco(self):
        '''metodo criado para registrar as pendencias de tags do endereco recarregado para chekList do supervisor '''

        sql = """
            insert into "Reposicao"."RegistroPendencia" (codbarrastag, usuario, empresa, endereco, "dataHora", "situacaoTagCsw", "natureza", "detalhaErro"  )
            values (%s, %s, %s, %s, %s, %s, %s , %s) 
        """

        with ConexaoPostgreMPL.conexao() as conn:
            with conn.cursor() as curr:

                curr.execute(sql,(self.codbarrastag, self.usuario, self.empresa, self.endereco, self.DataHora, self.situacaoTagCsw, self.natureza, self.detalhaErro))
                conn.commit()


    def delete_registroPendenciasRecarregarEndereco(self):
        '''Metodo utilizado para "excluir" os registros que foram efetivados com sucesso '''

        delete = """
        delete from 
            "Reposicao"."RegistroPendencia" 
        where 
            codbarrastag = %s and empresa = %s
        """

        with ConexaoPostgreMPL.conexao() as conn:
            with conn.cursor() as curr:

                curr.execute(delete,(self.codbarrastag, self.empresa))
                conn.commit()




    def obterHoraAtual(self):
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.datetime.now(fuso_horario)
        hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
        return hora_str



    def verificarOpsEndereco(self):
        '''Metodo utilizado para verificar a OP ou as OPs que estao  no endereco'''

        sql = """
        select 
            distinct numeroop 
        from 
            "Reposicao".tagsreposicao
        where
            codempresa = %s and "Endereco" = %s
        """

        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql,conn,params=(self.empresa, self.endereco))

        tamanho = consulta['numeroop'].count()

        if tamanho == 1:
            return consulta['numeroop'][0]

        else:
            #Transformando  o dataFrame em um array
            resultado2 = '({})'.format(', '.join(["'{}'".format(valor) for valor in consulta['numeroop']]))
            return resultado2

    def consultaCswEPC(self, dataframTAG):
        '''Metodo utilizado para consultar no ERP CSW o codigo EPC pelas tags'''
        # Transformando  o dataFrame em um array
        resultado = '({})'.format(', '.join(["'{}'".format(valor) for valor in dataframTAG['codbarrastag']]))


        sql = """
        select
	        t.codbarrastag as codbarrastag , SUBSTRING(epc.ID,23,88) as epc
        FROM
	        tcr.TagBarrasProduto t
        join 
	        Tcr_Rfid.NumeroSerieTagEPC epc 
	    on
		    epc.codTag = 
		    codbarrastag
        WHERE
	        t.codEmpresa = 1
	        and t.codBarrasTag in """ + resultado

        with ConexaoCSW.Conexao() as conn:
            with conn.cursor() as cursor_csw:
                cursor_csw.execute(sql)
                colunas = [desc[0] for desc in cursor_csw.description]
                # Busca todos os dados
                rows = cursor_csw.fetchall()
                # Cria o DataFrame com as colunas
                consulta = pd.DataFrame(rows, columns=colunas)
                del rows
                gc.collect()

        return consulta

    def registrarTagsNoEndereco(self, dataframe):
        '''Metodo que salva as informacoes no endereco '''

        #1. encontrando os EPC's das tags aprovadas
        epc = self.consultaCswEPC(dataframe)
        dataframe = pd.merge(dataframe, epc,on='codbarrastag',how='left')
        dataframe.fillna('-',inplace=True)
        try:
            conn = ConexaoPostgreMPL.conexao()
            insert = """ 
                insert into 
                    "Reposicao".tagsreposicao 
                        ("Endereco","codbarrastag","codreduzido",
                        "engenharia","descricao","natureza","codempresa","cor","tamanho","numeroop","usuario", 
                        "proveniencia","DataReposicao", usuario_carga, datahora_carga, epc, resticao)
                    values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s , %s, %s, %s, %s )"""
            dataframe['proveniencia'] = 'Veio da Caixa: ' + dataframe['caixa'][0]

            cursor = conn.cursor()  # Crie um cursor para executar a consulta SQL

            dataframe['usuario_carga'] = self.usuario
            dataframe['data_hora_carga'] = self.DataHora
            values = [(self.endereco, row['codbarrastag'], row['codreduzido'], row['engenharia'], row['descricao']
                       , row['natureza'], row['codempresa'], row['cor'], row['tamanho'], row['numeroop'],
                       row['usuario'], row['proveniencia'], row['DataReposicao'], row['usuario_carga'],
                       row['data_hora_carga'],row['epc'], row['resticao']) for index, row in dataframe.iterrows()]

            cursor.executemany(insert, values)
            conn.commit()  # Faça o commit da transação
            cursor.close()  # Feche o cursor

            self.excluirCaixaValidada()

            return dataframe


        except psycopg2.Error as e:
            if 'duplicate key value violates unique constraint' in str(e):

                dataframe['mensagem'] = "codbarras ja existe em outra prateleira"
                self.detalhaErro = "codbarras ja existe em outra prateleira"
                self.put_registroPendenciasRecarregarEndereco()

                return dataframe

            else:
                # Lidar com outras exceções que não são relacionadas à chave única
                print("Erro inesperado:", e)
                dataframe['mensagem'] = "Erro inesperado:", e
                self.detalhaErro = e
                self.put_registroPendenciasRecarregarEndereco()


                return dataframe


    def excluirCaixaValidada(self):
        '''Metodo chamado para excluir as caixas validadas da reposicao OFF '''

        delete = '''
        delete from "off"."reposicao_qualidade"
        where caixa = %s
        '''

        with ConexaoPostgreMPL.conexao() as conn :
            with conn.cursor() as curr:
                curr.execute(delete,(self.Ncaixa,))
                conn.commit()






