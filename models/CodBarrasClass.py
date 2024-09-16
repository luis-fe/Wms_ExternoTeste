import ConexaoPostgreMPL
import datetime
import pytz
from models import SkuClass

class codBarrasTag():
    '''Classe que define os metodos e atributos de cobBarrasTag no WMS'''
    def __init__(self, codigo, empresa = None, op= None,
                 codSku= None, naturezaEstoque= None, enderecoWMS= None, status= None, epc = None, codProduto = None, descricao =None , cor = None):
        self.codigo = codigo
        self.empresa = empresa
        self.op = op
        self.codSku = codSku
        self.naturezaEstoque = naturezaEstoque
        self.enderecoWMS = enderecoWMS
        self.status = status
        self.epc = epc

        if codProduto == None:
            self.codProduto = codProduto
        else:
            self.codProduto = codProduto


        self.descricao = descricao
        self.cor = cor

    def reporTagViaReposicaoOP(self, codUsuarioMovimento):
        '''Metodo para Repor a Tag no estoque , via ReposicaoOP'''
        # insere os dados da reposicao
        Insert = """INSERT INTO "Reposicao"."tagsreposicao" ("usuario","codbarrastag","Endereco","DataReposicao","codreduzido","Engenharia","descricao", 
                 "cor", "epc" )
                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
        dataHora = self.obterHoraAtual('en')
        with ConexaoPostgreMPL.conexao() as conn:
            with conn.cursor() as cursor:
                cursor.execute(Insert
                               , (codUsuarioMovimento, self.codigo, self.enderecoWMS, dataHora, self.codSku, self.codProduto, self.descricao, self.cor, self.epc))

                # Obter o número de linhas afetadas
                conn.commit()
    #def reporCaixaViaReposicaoOFF(self,codUsuarioMovimento):


    #def transferirTagEndereco(self, codUsuarioMovimento,enderecoNovo):


    #def inventariarTag(self, codUsuarioMovimento):


    #def separarTagPedido(self,codUsuarioMovimento, codPedido, enderecoSugerido):



    #def consultarStatusTag(self):


    #def consultarEnderecoWMSTag(self):


    def obterHoraAtual(self,padrao = 'br'):
            fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
            agora = datetime.datetime.now(fuso_horario)

            if padrao =='br':
                hora_str = agora.strftime('%d/%m/%Y %H:%M:%S')
                return hora_str

            else:
                hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
                return hora_str
