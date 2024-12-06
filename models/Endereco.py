import pandas as pd
import ConexaoPostgreMPL
from reportlab.lib.units import cm
import qrcode
from reportlab.graphics import barcode
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import landscape
import cups
import tempfile

class Endereco ():
    '''Classe criada para entidade Endereco do WMS '''

    def __init__(self, endereco = None, empresa = None, rua = None, modulo = None, posicao = None, natureza = None, ruaLimite = None,
                 moduloLimite = None, posicaoLimite = None, tipoEndereco = None ):

        self.endereco = endereco
        self.empresa = str(empresa)
        self.rua = rua
        self.modulo = modulo
        self.posicao = posicao
        self.natureza = str(natureza)
        self.ruaLimite = ruaLimite
        self.moduloLimite = moduloLimite
        self.posicaoLimite = posicaoLimite
        self.tipoEndereco = tipoEndereco

    def validaEndereco(self):
        '''Metodo utilizado para validar se extite o endereco no WMS'''

        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = 'select codendereco  FROM "Reposicao"."Reposicao".cadendereco c  ' \
                   ' where "codendereco" = %s  and codempresa = %s'
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
            ' select codempresa, natureza, "codendereco", tipo from "Reposicao"."cadendereco" ce   where codempresa = %s', conn, params=(self.empresa,))

        endercos.fillna('-',inplace=True)

        return endercos

    def enderecosDisponiveis(self):
        '''Metodo que avalia a disponibilidade dos enderecos em estoque'''

        conn = ConexaoPostgreMPL.conexaoEngine()

        # 1. Aqui carrego os enderecos do banco de dados, por uma view chamado enderecosReposicao,
        # essa view mostra o saldo de cada endereco cadastrado na plataforma, por empresa e por natureza

        # 1.1 Carregando somente os enderecos com saldo = 0
        relatorioEndereco = pd.read_sql(
            """select 
                    codendereco, 
                    contagem as saldo 
                from 
                    "Reposicao"."enderecosReposicao"
                where 
                    contagem = 0 
                    and natureza = %s 
                    and codempresa = %s """, conn, params=(self.natureza,self.empresa))

        # 1.2 Carregando todos os enderecos
        relatorioEndereco2 = pd.read_sql(
            'select codendereco, contagem as saldo from "Reposicao"."enderecosReposicao" where natureza = %s and codempresa = %s '
            , conn, params=(self.natureza,self.empresa))

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

    def gerarVariosEnderecos(self,imprimir):

        '''Metodo utilizado para gerar e imprimir varios enderecos de acordo com os parametros de rua (ini e fim) x modulo (ini e fim) x posicao (ini x fim) '''

        conn = ConexaoPostgreMPL.conexao()


        # query que serao utilizadas para updade e insert
        query = 'insert into "Reposicao".cadendereco (codendereco, rua, modulo, posicao, codempresa, natureza) ' \
                'values (%s, %s, %s, %s, %s, %s )'

        update = 'update "Reposicao".cadendereco ' \
                 ' set codendereco = %s , rua = %s , modulo = %s , posicao = %s , codempresa = %s , natureza = %s ' \
                 ' where  codendereco = %s '

        r = int(self.rua)
        self.ruaLimite = int(self.ruaLimite) + 1

        m = int(self.modulo)
        self.moduloLimite = int(self.moduloLimite) + 1

        p = int(self.posicao)
        self.posicaoLimite = int(self.posicaoLimite) + 1

        # enquando a rua for menor que o limite
        while r < self.ruaLimite:
            ruaAtual = self.Acres_0(r)
            while m < self.moduloLimite:
                moduloAtual = self.Acres_0(m)
                while p < self.posicaoLimite:
                    posicaoAtual = self.Acres_0(p)


                    ## Atualizado os atributos do objeto endereco ENDEREO X RUA X MODULO X POSICAO dentro do WHILE
                    self.endereco = ruaAtual + '-' + moduloAtual + "-" + posicaoAtual
                    self.rua = ruaAtual
                    self.modulo = moduloAtual
                    self.posicao = posicaoAtual


                    cursor = conn.cursor()
                    select = pd.read_sql('select codendereco from "Reposicao".cadendereco where codendereco = %s and codempresa = %s ',
                                         conn,
                                         params=(self.endereco,self.empresa))
                    if imprimir == True:

                        self.imprimirEtiquetaPrateleira('teste.pdf')
                        self.imprimir_pdf('teste.pdf')
                    else:
                        print(f'sem imprimir')


                    # Caso o endereco nao seja encontrado faz o cadastro dele
                    if select.empty:
                        cursor.execute(query, (
                        self.endereco, self.rua, self.modulo, self.posicao, self.empresa, self.natureza))
                        conn.commit()
                        cursor.close()

                    # Caso o endereco seja encontrado faz o update
                    else:

                        cursor.execute(update, (
                        self.endereco, self.rua, self.modulo, self.posicao, self.empresa, self.natureza,
                        self.endereco))
                        conn.commit()

                        cursor.close()
                        print(f'{self.endereco} ja exite')
                    p += 1
                p = int(self.posicao)
                m += 1
            m = int(self.modulo)
            r += 1

    def Acres_0(self,valor):
        if valor < 10:
            valor = str(valor)
            valor = '0' + valor
            return valor
        else:
            valor = str(valor)
            return valor

    def imprimirEtiquetaPrateleira(self, saida_pdf):
        '''Metodo utilizado para imprimir as etiquetas da prateleira '''



        # Configurações das etiquetas e colunas
        label_width = 7.5 * cm
        label_height = 1.8 * cm

        # Criar o PDF e ajustar o tamanho da página para paisagem com tamanho personalizado
        custom_page_size = landscape((label_width, label_height))
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_qr_file:
            qr_filename = temp_qr_file.name

            c = canvas.Canvas(saida_pdf, pagesize=custom_page_size)

            # Título centralizado
            title = self.rua
            c.setFont("Helvetica-Bold", 23)
            c.drawString(0.3 * cm, 0.75 * cm, title)

            c.setFont("Helvetica-Bold", 9)
            c.drawString(0.5 * cm, 1.5 * cm, 'Rua.')

            title = self.modulo
            c.setFont("Helvetica-Bold", 23)
            c.drawString(1.8 * cm, 0.75 * cm, title)

            c.setFont("Helvetica-Bold", 9)
            c.drawString(1.7 * cm, 1.5 * cm, 'Quadra.')

            title = self.posicao
            c.setFont("Helvetica-Bold", 23)
            c.drawString(3.3 * cm, 0.75 * cm, title)

            c.setFont("Helvetica-Bold", 9)
            c.drawString(3.2 * cm, 1.5 * cm, 'Posicao.')

            c.setFont("Helvetica-Bold", 7)
            c.drawString(5.2 * cm, 0.15 * cm, 'Natureza:')

            c.setFont("Helvetica-Bold", 7)
            c.drawString(6.4 * cm, 0.15 * cm, self.natureza)

            qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
            qr.add_data(self.endereco)  # Substitua pelo link desejado
            qr.make(fit=True)
            qr_img = qr.make_image(fill_color="black", back_color="white")
            qr_img.save(qr_filename)  # Salvar a imagem do QR code no arquivo temporário
            c.drawImage(qr_filename, 5.2 * cm, 0.43 * cm, width=1.45 * cm, height=1.30 * cm)

            c.setFont("Helvetica-Bold", 9)
            barcode_value = self.endereco  # Substitua pelo valor do código de barras desejado
            barcode_code128 = barcode.code128.Code128(barcode_value, barHeight=15, barWidth=0.75, humanReadable=False)
            # Desenhar o código de barras diretamente no canvas
            barcode_code128.drawOn(c, 0.07 * cm, 0.1 * cm)

            c.save()

    def imprimir_pdf(self,pdf_file):
        conn = cups.Connection()
        # printers = conn.getPrinters()
        # printer_name = list(printers.keys())[0]
        printer_name = "ZM400"  # Aqui teremos que criar uma funcao para imprimir as etiquetas de cianorte
        job_id = conn.printFile(printer_name, pdf_file, "Etiqueta",
                                {'PageSize': 'Custom.10x0.25cm', 'FitToPage': 'True', 'Scaling': '100',
                                 'Orientation': '3'})
        print(f"ID {job_id} enviado para impressão")

    def deletarVariosEnderecos(self):

        '''Metodo utilizado para deletar varios enderecos de acordo com rua ini x rua fim , modulo ini x modulo fim, posica ini x posicao fim '''

        conn = ConexaoPostgreMPL.conexao()
        query = 'delete from "Reposicao".cadendereco ' \
                'where rua = %s and modulo = %s and posicao = %s and codempresa = %s '

        r = int(self.rua)
        self.ruaLimite = int(self.ruaLimite) + 1

        m = int(self.modulo)
        self.moduloLimite = int(self.moduloLimite) + 1

        p = int(self.posicao)
        self.posicaoLimite = int(self.posicaoLimite) + 1

        while r < self.ruaLimite:
            ruaAtual = self.Acres_0(r)
            while m < self.moduloLimite:
                moduloAtual = self.Acres_0(m)
                while p < self.posicaoLimite:
                    posicaoAtual = self.Acres_0(p)
                    self.endereco = ruaAtual + '-' + moduloAtual + "-" + posicaoAtual
                    cursor = conn.cursor()
                    select = pd.read_sql('select "Endereco" from "Reposicao".tagsreposicao where "Endereco" = %s ',
                                         conn,
                                         params=(self.endereco,))
                    if select.empty:
                        cursor.execute(query, (ruaAtual, moduloAtual, posicaoAtual, self.empresa))
                        conn.commit()
                        cursor.close()
                    else:
                        cursor.close()
                        print(f'{self.endereco} nao pode ser excluido ')
                    p += 1
                p = int(self.posicao)
                m += 1
            m = int(self.modulo)
            r += 1

    def obterTipoPrateleira(self):
        conn = ConexaoPostgreMPL.conexaoEngine()
        qurey = pd.read_sql('select tipo from "Reposicao"."configuracaoTipo" ', conn)

        return qurey



