import pandas as pd
import ConexaoPostgreMPL
import math
from reportlab.lib.pagesizes import landscape
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas
import tempfile
import qrcode
import cups


class CaixaOFF():
    '''Modulo utilizado para interagir com o objeto Caixa do Wms'''

    def __init__(self, Ncaixa = None , empresa = None, usuario = None, qtdImpressao = 0):

        self.Ncaixa = Ncaixa
        self.empresa = empresa
        self.usuario = usuario
        self.qtdeImpressao = qtdImpressao # atributo em int com a quantidade de caixas a serem impressas

    def caixasAbertas(self):
        ''' Metodo utilizado para consulta as caixas em aberto (com tags ainda nao entradas no estoque) '''

        sql = """
            select distinct 
                rq.caixa, 
                rq.usuario,
                codreduzido,
                numeroop ,
                descricao
            from 
                "Reposicao"."off".reposicao_qualidade rq 
            where 
                rq.codempresa  = %s order by caixa
        """


        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.empresa,))

        # Obter os usuarios cadastrados para realizar o merge
        Usuarios = pd.read_sql('Select codigo as usuario, nome from "Reposicao".cadusuarios ', conn)
        Usuarios['usuario'] = Usuarios['usuario'].astype(str)
        consulta = pd.merge(consulta, Usuarios, on='usuario', how='left')

        #Obter as tag já bipadas na caixa
        BipadoSKU = pd.read_sql(
            'select numeroop, codreduzido, count(rq.codreduzido) as bipado_sku_OP from "Reposicao"."off".reposicao_qualidade rq  group by codreduzido, numeroop ',
            conn)
        consulta = pd.merge(consulta, BipadoSKU, on=['codreduzido','numeroop'], how='left')
        consulta.fillna('-',inplace=True)

        return consulta

    def caixasAbertasUsuario(self):
        '''Metodo utilizado para consultar as caixas abertas para um usuario em especifico'''


        sql = """
            select distinct 
                rq.caixa, 
                rq.usuario,
                codreduzido,
                numeroop ,
                descricao
            from 
                "Reposicao"."off".reposicao_qualidade rq 
            where 
                rq.codempresa  = %s and usuario = %s order by caixa
        """


        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql,
            conn, params=(self.empresa, self.usuario,)
        )

        # Realizando o agrupamento no Pandas
        consulta['n_bipado'] = 1  # Adicionando uma coluna para contar as ocorrências
        consulta = consulta.groupby(['caixa', 'numeroop', 'codreduzido', 'descricao']).count().reset_index()

        BipadoSKU = pd.read_sql(
            'select numeroop, codreduzido, count(rq.codreduzido) as bipado_sku_OP from "Reposicao"."off".reposicao_qualidade rq  '
            'where numeroop in (select distinct numeroop from "Reposicao"."off".reposicao_qualidade rq where rq.usuario = %s) '
            'group by codreduzido, numeroop ', conn, params=(self.usuario,))

        consulta = pd.merge(consulta, BipadoSKU, on=['codreduzido', 'numeroop'], how='left')

        if not consulta.empty:
            consulta['usuario'] = str(self.usuario)
            Usuarios = pd.read_sql('Select codigo as usuario, nome from "Reposicao".cadusuarios ', conn)
            Usuarios['usuario'] = Usuarios['usuario'].astype(str)
            consulta = pd.merge(consulta, Usuarios, on='usuario', how='left')

            #consulta, boleano = Get_quantidadeOP_Sku(consulta, empresa, '0')
            consulta['1 - status'] = consulta["bipado_sku_op"].astype(str)
            consulta['1 - status'] = consulta['1 - status'] + '/' + consulta["total_pcs"].astype(str)
        else:
            print("Usuario ainda nao comeco a repor")
            consulta['1 - status'] = "0/0"

        conn.close()
        return consulta

    def gerarCaixas(self, salvaEtiqueta):
        '''Metodo utilizado para gerar e imprimir as caixas da reposicao off'''


        # Verificando a quantidade de caixas a serem impressas
        quantidade = int(self.qtdeImpressao)
        n_impressoes = math.ceil(quantidade / 3)


        # Obtendo a ultima caixa cadastrada
        sql = """
            select 
                sc.codigo::INTEGER 
            from 
                "off".seq_caixa sc 
            order by 
                codigo desc 
        """

        conn = ConexaoPostgreMPL.conexaoEngine()
        inicial = pd.read_sql(sql, conn)

        inicial = inicial['codigo'][0]
        inicial2 = int(inicial)

        # 3 - Obtendo no for os codigos das novas caixas
        for i in range(n_impressoes):
            codigo1 = inicial2 + 1
            codigo2 = inicial2 + 2
            codigo3 = inicial2 + 3
            inicial2 = codigo3

            codigo1 = '' + str(codigo1)
            codigo2 = '' + str(codigo2)
            codigo3 = '' + str(codigo3)

            # Deseja registrar a impresao do codigo de barras
            if salvaEtiqueta == False:
                nometeste = 'caixa_'+str('')+".pdf"
            else:
                nometeste = 'caixa_' + str(i) + ".pdf"

                self.imprimirSeqCaixa(nometeste, codigo1, codigo2, codigo3)

                insert = 'insert into "off".seq_caixa (codigo, usuario) values ( %s, %s )'
                cursor = conn.cursor()
                cursor.execute(insert, (codigo1, self.usuario,))
                conn.commit()

                cursor.execute(insert, (codigo2, self.usuario,))
                conn.commit()
                cursor.execute(insert, (codigo3, self.usuario,))
                conn.commit()

                cursor.close()
                self.imprimir_pdf(nometeste)

            conn.close()

    def imprimirSeqCaixa(self, saida_pdf, codigo1, codigo2='0', codigo3='0'):
        # Configurações das etiquetas e colunas
        label_width = 7.5 * cm
        label_height = 1.8 * cm
        # Criar o PDF e ajustar o tamanho da página para paisagem com tamanho personalizado
        custom_page_size = landscape((label_width, label_height))
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_qr_file:
            qr_filename = temp_qr_file.name

            c = canvas.Canvas(saida_pdf, pagesize=custom_page_size)

            # qrcode 1
            qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
            qr.add_data(codigo1)  # Substitua pelo link desejado
            qr.make(fit=True)
            qr_img = qr.make_image(fill_color="black", back_color="white")
            qr_img.save(qr_filename)  # Salvar a imagem do QR code no arquivo temporário
            c.drawImage(qr_filename, 0.3 * cm, 0.43 * cm, width=1.45 * cm, height=1.30 * cm)

            c.setFont("Helvetica-Bold", 5)
            c.drawString(0.3 * cm, 0.2 * cm, 'NºCx:')

            c.setFont("Helvetica-Bold", 5)
            c.drawString(0.9 * cm, 0.2 * cm, '' + codigo1)

            # qrcode 2:

            if codigo2 == '0':
                print('sem seq')
            else:
                with tempfile.NamedTemporaryFile(delete=False, suffix="2.png") as temp_qr_file2:
                    qr_filename2 = temp_qr_file2.name

                    qr2 = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
                    qr2.add_data(codigo2)
                    qr2.make(fit=True)
                    qr_img2 = qr2.make_image(fill_color="black", back_color="white")
                    qr_img2.save(qr_filename2)
                    c.drawImage(qr_filename2, 2.8 * cm, 0.43 * cm, width=1.45 * cm, height=1.30 * cm)

                    c.setFont("Helvetica-Bold", 5)
                    c.drawString(2.8 * cm, 0.2 * cm, 'NºCx:')

                    c.setFont("Helvetica-Bold", 5)
                    c.drawString(3.4 * cm, 0.2 * cm, '' + codigo2)

            if codigo3 == '0':
                print('sem seq')
            else:
                with tempfile.NamedTemporaryFile(delete=False, suffix="3.png") as temp_qr_file3:
                    qr_filename3 = temp_qr_file3.name

                    qr3 = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
                    qr3.add_data(codigo3)
                    qr3.make(fit=True)
                    qr_img3 = qr3.make_image(fill_color="black", back_color="white")
                    qr_img3.save(qr_filename3)
                    c.drawImage(qr_filename3, 5.3 * cm, 0.43 * cm, width=1.45 * cm, height=1.30 * cm)

                    c.setFont("Helvetica-Bold", 5)
                    c.drawString(5.3 * cm, 0.2 * cm, 'NºCx:')

                    c.setFont("Helvetica-Bold", 5)
                    c.drawString(5.8 * cm, 0.2 * cm, '' + codigo3)

            c.save()

    def imprimir_pdf(self, pdf_file):
        conn = cups.Connection()
        # printers = conn.getPrinters()
        # printer_name = list(printers.keys())[0]
        printer_name = "ZM400"  # Aqui teremos que criar uma funcao para imprimir as etiquetas de cianorte
        job_id = conn.printFile(printer_name, pdf_file, "Etiqueta",
                                {'PageSize': 'Custom.10x0.25cm', 'FitToPage': 'True', 'Scaling': '100',
                                 'Orientation': '3'})
        print(f"ID {job_id} enviado para impressão")


