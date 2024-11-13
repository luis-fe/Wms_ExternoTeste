import cups
from reportlab.lib.pagesizes import landscape
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas
import tempfile
import qrcode
import ConexaoPostgreMPL
import pandas as pd

class Carrinho():
    """Classe que representa o cadastro de Carrinhos no WMS"""
    def __init__(self, NCarrinho = None, empresa = None):

        self.NCarrinho = NCarrinho
        self.empresa = empresa

    def consultarCarrinhos(self):
        '''Metodo criado para consultar todos os carrinhos disponivel por empresa'''

        consulta = """
        select 
            "NCarrinho"
        from 
            "off"."Carrinho"
        where
            empresa = %s
        """

        conn = ConexaoPostgreMPL.conexaoEngine()

        consulta = pd.read_sql(consulta,conn,params=(self.empresa,))

        return consulta

    def cadastrarCarrinho(self):
        '''Metodo criado para inserir um novo carrinho'''

        # Pesquisando se o carrinho ja existe
        verificar = self.pesquisarCarrinhoEspecifico()

        if not verificar.empty:
            return pd.DataFrame([{'status':False,'mensagem':'Carrinho ja existe'}])

        else:

            inserir = """
            insert into "off"."Carrinho" ("NCarrinho","empresa") values (%s , %s)
            """

            #Inserindo
            with ConexaoPostgreMPL.conexao() as conn:
                with conn.cursor() as curr:
                    curr.execute(inserir,(self.NCarrinho, self.empresa))
                    conn.commit()

                    return pd.DataFrame([{'status': True, 'mensagem': 'Carrinho criado com sucesso'}])

    def pesquisarCarrinhoEspecifico(self):
        '''Metodo que pesquisa um carrinho'''

        consulta = """
        select 
            "NCarrinho"
        from 
            "off"."Carrinho"
        where
            empresa = %s and "NCarrinho" = %s
        """

        conn = ConexaoPostgreMPL.conexaoEngine()

        consulta = pd.read_sql(consulta, conn, params=(str(self.empresa), self.NCarrinho))

        return consulta

    def excluirCarrinho(self):

        #Valida se o carrinho ta atribuido a uma caixa
        validando = self.validandoCarrinho()

        if not validando.empty:

            return pd.DataFrame([{'status':False,'mensagem':'Carrinho possue relacao com caixas'}])

        else:
            delete = """
            delete 
            from 
                "off"."Carrinho"
            where
            empresa = %s and "NCarrinho" = %s 
            """

            with ConexaoPostgreMPL.conexao() as conn:
                with conn.cursor() as curr:
                    curr.execute(delete,(self.empresa, self.NCarrinho))
                    conn.commit()

                    return pd.DataFrame([{'status': True, 'mensagem': 'Carrinho excluido com sucesso'}])



    def validandoCarrinho(self):
        '''Metodo utilizado para verificar se o carrinho esta atribuido a caixas'''

        sql = """
        select
            "Ncarrinho" ,
            caixa        
        from
            "off".reposicao_qualidade rq
        where
            rq."Ncarrinho" = %s and rq.codempresa = %s and (rq."statusNCarrinho" <> 'liberado' or rq."statusNCarrinho" is null)
        """


        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql(sql,conn, params=(self.NCarrinho, self.empresa))

        return consulta

    def gerarEtiquetaCarrinho(self,saida_pdf):
        # Configurações das etiquetas e colunas
        label_width = 7.5 * cm
        label_height = 1.8 * cm

        # Criar o PDF e ajustar o tamanho da página para paisagem com tamanho personalizado
        custom_page_size = landscape((label_width, label_height))
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_qr_file:
            qr_filename = temp_qr_file.name

            c = canvas.Canvas(saida_pdf, pagesize=custom_page_size)

            # Título centralizado
            title = 'Carrinho:'
            c.setFont("Helvetica-Bold", 10)
            c.drawString(0.1 * cm, 1.5 * cm, title)


            title = str(self.NCarrinho)
            c.setFont("Helvetica-Bold", 48)
            c.drawString(0.15 * cm, 0.09 * cm, title)



            qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
            qr.add_data(str(self.NCarrinho))  # Substitua pelo link desejado
            qr.make(fit=True)
            qr_img = qr.make_image(fill_color="black", back_color="white")
            qr_img.save(qr_filename)  # Salvar a imagem do QR code no arquivo temporário
            c.drawImage(qr_filename, 5.4 * cm, 0.09 * cm, width=1.55 * cm, height= 1.55 * cm)


            c.save()

    def imprimir_pdfCarrinho(self,pdf_file):
        conn = cups.Connection()
        # printers = conn.getPrinters()
        # printer_name = list(printers.keys())[0]
        printer_name = "ZM400"  # Aqui teremos que criar uma funcao para imprimir as etiquetas de cianorte
        job_id = conn.printFile(printer_name, pdf_file, "Etiqueta",
                                {'PageSize': 'Custom.10x0.25cm', 'FitToPage': 'True', 'Scaling': '100',
                                 'Orientation': '3'})
        print(f"ID {job_id} enviado para impressão")









