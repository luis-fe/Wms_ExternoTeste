import pandas as pd
import ConexaoPostgreMPL


class CaixaOFF():
    '''Modulo utilizado para interagir com o objeto Caixa do Wms'''

    def __init__(self, Ncaixa = None , empresa = None, usuario = None):

        self.Ncaixa = Ncaixa
        self.empresa = empresa
        self.usuario = usuario

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

