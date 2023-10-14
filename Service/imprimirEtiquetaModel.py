import cups
import os
from reportlab.lib.pagesizes import landscape
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas
import tempfile
from reportlab.graphics import barcode
import qrcode
import math
import ConexaoPostgreMPL
import pandas as pd


def criar_pdf(saida_pdf, titulo, cliente, pedido, transportadora, separador, agrupamento):
    # Configurações das etiquetas e colunas
    label_width = 7.5 * cm
    label_height = 1.8 * cm

    # Criar o PDF e ajustar o tamanho da página para paisagem com tamanho personalizado
    custom_page_size = landscape((label_width, label_height))

    # Criar um arquivo temporário para salvar o QR code
    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_qr_file:
        qr_filename = temp_qr_file.name

        c = canvas.Canvas(saida_pdf, pagesize=custom_page_size)

        # Título centralizado
        c.setFont("Helvetica-Bold", 9)
        title = titulo
        c.drawString(0.3 * cm, 1.5 * cm, title)


        qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
        qr.add_data(cliente)  # Substitua pelo link desejado
        qr.make(fit=True)
        qr_img = qr.make_image(fill_color="black", back_color="white")
        qr_img.save(qr_filename)  # Salvar a imagem do QR code no arquivo temporário
        c.drawImage(qr_filename, 5.2 * cm, 0.43 * cm, width=1.45 * cm, height= 1.30 * cm)

        #barcode_value = cliente  # Substitua pelo valor do código de barras desejado
        #barcode_code128 = barcode.code128.Code128(barcode_value, barHeight=15, humanReadable=True)
        # Desenhar o código de barras diretamente no canvas
        #barcode_code128.drawOn(c, 3.0 * cm, 0.3 * cm)

        c.setFont("Helvetica", 9)
        c.drawString(0.3 * cm, 1.1 * cm, "Nº Cliente:")
        c.drawString(0.3 * cm, 0.8 * cm, "Nº Pedido:")
        c.drawString(0.3 * cm, 0.50 * cm, transportadora)

        c.setFont("Helvetica-Bold", 8)
        c.drawString(0.3 * cm, 0.1 * cm, separador)


        c.drawString(2.0 * cm, 1.1 * cm, cliente)
        c.drawString(2.0 * cm, 0.8 * cm, pedido)
        c.setFont("Helvetica-Bold", 5)
        c.drawString(2.5 * cm, 0.1 * cm, agrupamento)

        c.save()

def imprimir_pdf(pdf_file):
    conn = cups.Connection()
    printers = conn.getPrinters()
    printer_name = list(printers.keys())[0]
    job_id = conn.printFile(printer_name,pdf_file,"Etiqueta",{'PageSize': 'Custom.10x0.25cm', 'FitToPage': 'True', 'Scaling': '100','Orientation':'3'})
    print(f"ID {job_id} enviado para impressão")

def EtiquetaPrateleira(saida_pdf,endereco, rua,modulo,posicao, natureza):
    # Configurações das etiquetas e colunas
    label_width = 7.5 * cm
    label_height = 1.8 * cm

    # Criar o PDF e ajustar o tamanho da página para paisagem com tamanho personalizado
    custom_page_size = landscape((label_width, label_height))
    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_qr_file:
        qr_filename = temp_qr_file.name

        c = canvas.Canvas(saida_pdf, pagesize=custom_page_size)

        # Título centralizado
        title = rua
        c.setFont("Helvetica-Bold", 23)
        c.drawString(0.3 * cm, 0.75 * cm, title)

        c.setFont("Helvetica-Bold", 9)
        c.drawString(0.5 * cm, 1.5 * cm, 'Rua.')

        title = modulo
        c.setFont("Helvetica-Bold", 23)
        c.drawString(1.8 * cm, 0.75 * cm, title)

        c.setFont("Helvetica-Bold", 9)
        c.drawString(1.7 * cm, 1.5 * cm, 'Quadra.')

        title = posicao
        c.setFont("Helvetica-Bold", 23)
        c.drawString(3.3 * cm, 0.75 * cm, title)

        c.setFont("Helvetica-Bold", 9)
        c.drawString(3.2 * cm, 1.5 * cm, 'Posicao.')

        c.setFont("Helvetica-Bold", 8)
        c.drawString(5.2 * cm, 0.2 * cm, 'Natureza:')

        c.setFont("Helvetica-Bold", 8)
        c.drawString(6.4 * cm, 0.2 * cm, natureza)


        qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
        qr.add_data(endereco)  # Substitua pelo link desejado
        qr.make(fit=True)
        qr_img = qr.make_image(fill_color="black", back_color="white")
        qr_img.save(qr_filename)  # Salvar a imagem do QR code no arquivo temporário
        c.drawImage(qr_filename, 5.2 * cm, 0.43 * cm, width=1.45 * cm, height= 1.30 * cm)

        c.setFont("Helvetica-Bold", 9)
        barcode_value = endereco  # Substitua pelo valor do código de barras desejado
        barcode_code128 = barcode.code128.Code128(barcode_value, barHeight=15, barWidth=0.75 ,humanReadable=False)
        # Desenhar o código de barras diretamente no canvas
        barcode_code128.drawOn(c,0.07 * cm, 0.1 * cm)

        c.save()

def ImprimirSeqCaixa(saida_pdf,codigo1, codigo2 ='0', codigo3='0'):
    # Configurações das etiquetas e colunas
    label_width = 7.5 * cm
    label_height = 1.8 * cm
    # Criar o PDF e ajustar o tamanho da página para paisagem com tamanho personalizado
    custom_page_size = landscape((label_width, label_height))
    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_qr_file:
        qr_filename = temp_qr_file.name

        c = canvas.Canvas(saida_pdf, pagesize=custom_page_size)


        #qrcode 1
        qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
        qr.add_data(codigo1)  # Substitua pelo link desejado
        qr.make(fit=True)
        qr_img = qr.make_image(fill_color="black", back_color="white")
        qr_img.save(qr_filename)  # Salvar a imagem do QR code no arquivo temporário
        c.drawImage(qr_filename, 0.3 * cm, 0.43 * cm, width=1.45 * cm, height=1.30 * cm)

        c.setFont("Helvetica-Bold", 5)
        c.drawString(0.3 * cm, 0.2 * cm, 'NºCx:')

        c.setFont("Helvetica-Bold", 5)
        c.drawString(0.9 * cm, 0.2 * cm, '00' + codigo1)




        # qrcode 2:

        if codigo2 == '0' :
            print('sem seq')
        else:

            qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
            qr.add_data(codigo2)  # Substitua pelo link desejado
            qr.make(fit=True)
            qr_img = qr.make_image(fill_color="black", back_color="white")
            qr_img.save(qr_filename)  # Salvar a imagem do QR code no arquivo temporário
            c.drawImage(qr_filename, 2.8 * cm, 0.43 * cm, width=1.45 * cm, height=1.30 * cm)

            c.setFont("Helvetica-Bold", 5)
            c.drawString(2.8 * cm, 0.2 * cm, 'NºCx:')

            c.setFont("Helvetica-Bold", 5)
            c.drawString(3.4 * cm, 0.2 * cm, '00' + codigo2)

        if codigo3 == '0' :
            print('sem seq')
        else:

            # qrcode 3:
            qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
            qr.add_data(codigo3)  # Substitua pelo link desejado
            qr.make(fit=True)
            qr_img = qr.make_image(fill_color="black", back_color="white")
            qr_img.save(qr_filename)  # Salvar a imagem do QR code no arquivo temporário
            c.drawImage(qr_filename, 5.3 * cm, 0.43 * cm, width=1.45 * cm, height=1.30 * cm)

            c.setFont("Helvetica-Bold", 5)
            c.drawString(5.3 * cm, 0.2 * cm, 'NºCx:')

            c.setFont("Helvetica-Bold", 5)
            c.drawString(5.8 * cm, 0.2 * cm, '00' + codigo3)

        c.save()
def QuantidadeImprimir(quantidade):
    quantidade = int(quantidade)
    n_impressoes = math.ceil(quantidade / 3)

    conn = ConexaoPostgreMPL.conexao()
    inicial = pd.read_sql('select sc.codigo from "off".seq_caixa sc order by codigo desc ',conn)

    inicial = inicial['codigo'][0]
    inicial = int(inicial)

    for i in range(n_impressoes):
        codigo1 = inicial+1
        codigo2=inicial+2
        codigo3 = inicial+3
        inicial = inicial+3

        codigo1 = '00'+str(codigo1)
        codigo2 = '00'+str(codigo2)
        codigo3 = '00'+str(codigo3)
        ImprimirSeqCaixa('caixa.pdf',codigo1,codigo2,codigo3)


