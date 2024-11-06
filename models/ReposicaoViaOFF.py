import gc
from datetime import datetime
import pandas as pd
import pytz
import ConexaoCSW
import ConexaoPostgreMPL

class ReposicaoViaOFF():
    """Classe do WMS responsavel pela reposicao via OFF (antes das tag entrar em estoque), atribuindo tag a um Ncaixa e a NCarrinho """
    def __init__(self, codbarrastag, Ncaixa = None, empresa = None, usuario = None, natureza = None, estornar = False, Ncarrinho = ''):
        self.codbarrastag = str(codbarrastag)
        self.codbarrasPesquisa = "'" + self.codbarrastag + "'"

        self.Ncaixa = Ncaixa
        self.Ncarrinho = Ncarrinho
        self.empresa = str(empresa)
        self.usuario = usuario
        self.natureza = natureza
        self.estornar = estornar


    def apontarTagCaixa(self):
        '''Metodo criado para apontar a tag x Caixa x Ncarrinho '''

        usuario = self.usuario.strip()

        #2 - Validar se a Tag ja está sincronizada com o banco WMS
        pesquisa = self.consultaTagOFFWMS()


        #3 - retornando if de acordo com a respostas:

        ## Caso nao for encontrado tag, é feito uma pesquisada direto do CSW para recuperar a tag , porem ela deve estar nas situacoes 0 ou 9:
        if pesquisa.empty and self.estornar == False:
            conn2 = ConexaoCSW.Conexao()
            consultaCsw = self.buscarTagCsw()

            if not consultaCsw.empty:

                    consultaCsw['usuario'] = usuario
                    consultaCsw['caixa'] = self.Ncaixa
                    consultaCsw['natureza'] = self.natureza
                    consultaCsw['DataReposicao'] = self.dataHora()
                    consultaCsw['resticao'] = 'veio csw'
                    consultaCsw['Ncarrinho'] = self.Ncarrinho

                    self.InculirTagCaixa(consultaCsw)
                    conn2.close()


                    return pd.DataFrame([{'status': True, 'Mensagem': 'tag inserido !'}])

            else:

                    conn2.close()
                    print('tag nao existe na tablea filareposicaooff ')

                    return pd.DataFrame([{'status': False, 'Mensagem': f'tag {self.codbarrastag} nao encontrada !'}])

        else:
            # Caso a tag for encontrada na fila de reposicao da qualidade:

                ## Aqui complementamos no DataFrame "pesquisa" as informacoes de usuario, Ncaixa, caixa e Data e Hora
                pesquisa['usuario'] = usuario
                pesquisa['caixa'] = self.Ncaixa
                pesquisa['natureza'] = self.natureza
                pesquisa['DataReposicao'] = self.dataHora()
                pesquisa['Ncarrinho'] = self.Ncarrinho

                VerificandoExitenciaCaixa = self.PesquisarSeTagJaFoiBipada()  # Nessa etapa é conferida se a Tag ja foi ou nao bipada

                # Caso a tag ainda nao esteja bipada, aprova a insercao !:
                if VerificandoExitenciaCaixa == 1 and self.estornar == False:
                    self.InculirTagCaixa(pesquisa)  #
                    return pd.DataFrame([{'status': True, 'Mensagem': 'tag inserido !'}])

                # Caso a tag ja tenha sido bipado, avisa ao usuario :
                elif VerificandoExitenciaCaixa == 2 and self.estornar == False:
                    return pd.DataFrame(
                        [{'status': False, 'Mensagem': f'tag {self.codbarrastag} ja bipado nessa caixa, deseja estornar ?'}])
                elif self.estornar == False and VerificandoExitenciaCaixa != 2:
                    return pd.DataFrame(
                        [{'status': False,
                          'Mensagem': f'tag {self.codbarrastag} ja bipado em outra  caixa de n°{VerificandoExitenciaCaixa}, deseja estornar ?'}])
                else:
                    estorno = self.EstornarTag()
                    return estorno

    def consultaTagOFFWMS(self):
        # Estabelece a conexão com o banco de dados
        engine = ConexaoPostgreMPL.conexaoEngine()


        # Realiza a consulta SQL de maneira segura, usando parâmetros para evitar SQL Injection
        query = '''
               SELECT * 
               FROM "Reposicao".off.filareposicaoof 
               WHERE codbarrastag = %s
               AND codempresa = %s
           '''
        pesquisa = pd.read_sql(query,engine, params=(self.codbarrastag, self.empresa))

        # Retorna os dados consultados
        return pesquisa

    def buscarTagCsw(self):
        '''Metodo utilizado para buscar a tag direto do Csw'''


        consulta = """
            SELECT 
                p.codBarrasTag as codbarrastag , 
                p.codReduzido as codreduzido, 
                p.codEngenharia as engenharia,
                (select i.nome from cgi.Item i WHERE i.codigo = p.codReduzido) as descricao, situacao, codNaturezaAtual as natureza, 
                codEmpresa as codempresa,
                (select s.corbase||'-'||s.nomecorbase  from tcp.SortimentosProduto s WHERE s.codempresa = 1 and s.codproduto = p.codEngenharia and s.codsortimento = p.codSortimento)
                as cor, 
                (select t.descricao from tcp.Tamanhos t WHERE t.codempresa = 1 and t.sequencia = p.seqTamanho ) as tamanho, p.numeroOP as numeroop
            from 
                Tcr.TagBarrasProduto p 
            WHERE 
                p.codEmpresa = '""" + self.empresa + """' and situacao in (0, 9) and codbarrastag = """+ self.codbarrasPesquisa

        with ConexaoCSW.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(consulta)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
        del rows
        gc.collect()

        return consulta

    def dataHora(self):
        '''Metodo que retorna a data e hora atual'''

        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')

        return hora_str


## Funcao que insere os dados na tabela "Reposicao".off.reposicao_qualidade , persistindo os dados com as tags bipada na caixa
    def InculirTagCaixa(self, dataframe):

            ## Removendo duplicatas do dataframe:
            dataframe = dataframe.drop_duplicates(subset=['codbarrastag'])  ## Elimando as possiveis duplicatas

            conn = ConexaoPostgreMPL.conexao()

            cursor = conn.cursor()  # Crie um cursor para executar a consulta SQL
            insert = """
                    insert into off.reposicao_qualidade 
                        (
                        codbarrastag, 
                        codreduzido, 
                        engenharia, 
                        descricao, 
                        natureza, 
                        codempresa, 
                        cor, 
                        tamanho, 
                        numeroop, 
                        caixa, 
                        usuario, 
                        "DataReposicao", 
                        resticao, 
                        "Ncarrinho")
                     values 
                        ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"""

            values = [(row['codbarrastag'], row['codreduzido'], row['engenharia'], row['descricao']
                       , row['natureza'], row['codempresa'], row['cor'], row['tamanho'], row['numeroop'], row['caixa'],
                       row['usuario'], row['DataReposicao'], row['resticao'], row['Ncarrinho']) for index, row in dataframe.iterrows()]
            cursor.executemany(insert, values)
            conn.commit()  # Faça o commit da transação
            cursor.close()  # Feche o cursor

            conn.close()


    def PesquisarSeTagJaFoiBipada(self):
        '''Funcao que retorna se a tag ja foi ou não bipada em alguma caixa'''

        conn = ConexaoPostgreMPL.conexao()
        consulta = pd.read_sql('select caixa  from "off".reposicao_qualidade rq'
                               ' where rq.codbarrastag = ' + self.codbarrasPesquisa, conn)
        conn.close()

        if consulta.empty:
            return 1  # Optei por retorna valores ao inves de booleano
        else:
            caixaAntes = consulta['caixa'][0]  # Aqui retornamos o numero da caixa que essa tag foi bipada

            # Caso 1 : a caixa anterior é a mesma caixa que ele está tendando bipar
            if caixaAntes == str(self.Ncaixa):
                return 2  # Optei por retorna valores ao inves de booleano
            # Caso 2 : a caixa anterior nao é a mesma que o usuario está tentando bipar:
            else:
                return consulta['caixa'][0]  # Retorna a Nova Caixa

    def EstornarTag(self):
        conn = ConexaoPostgreMPL.conexao()
        delete = 'delete from "off".reposicao_qualidade ' \
                 'where codbarrastag  = ' + self.codbarrasPesquisa
        cursor = conn.cursor()
        cursor.execute(delete, )
        conn.commit()
        cursor.close()
        conn.close()

        return pd.DataFrame([{'status': True, 'Mensagem': 'tag estornada! '}])
