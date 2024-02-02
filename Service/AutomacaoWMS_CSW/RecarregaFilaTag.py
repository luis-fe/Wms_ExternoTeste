import ConexaoCSW
import pandas as pd
import numpy
import datetime
import pytz


def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str
def RecarregarTagFila(codbarras):
    valor = ConexaoCSW.pesquisaTagCSW(codbarras)

    if valor['stauts conexao'][0]==True:
        conn = ConexaoCSW.Conexao()
        codbarras = "'" + codbarras + "'"

        df_tags = pd.read_sql(
            "SELECT  codBarrasTag as codbarrastag, codNaturezaAtual as codnaturezaatual , codEngenharia , codReduzido as codreduzido,(SELECT i.nome  FROM cgi.Item i WHERE i.codigo = t.codReduzido) as descricao , numeroop as numeroop,"
            " (SELECT i2.codCor||'-'  FROM cgi.Item2  i2 WHERE i2.Empresa = 1 and  i2.codItem  = t.codReduzido) || "
            "(SELECT i2.descricao  FROM tcp.SortimentosProduto  i2 WHERE i2.codEmpresa = 1 and  i2.codProduto  = t.codEngenharia  and t.codSortimento  = i2.codSortimento) as cor,"
            " (SELECT tam.descricao  FROM cgi.Item2  i2 join tcp.Tamanhos tam on tam.codEmpresa = i2.Empresa and tam.sequencia = i2.codSeqTamanho  WHERE i2.Empresa = 1 and  i2.codItem  = t.codReduzido) as tamanho, codEmpresa as codempresa "
            " from tcr.TagBarrasProduto t WHERE situacao = 3 and codBarrasTag = "+ codbarras,
            conn)

        numeroOP = df_tags['numeroop'][0]
        numeroOP = "'%" + numeroOP + "%'"

        df_opstotal = pd.read_sql('SELECT top 200000 numeroOP as numeroop , totPecasOPBaixadas as totalop  '
                                  'from tco.MovimentacaoOPFase WHERE codEmpresa = 1 and codFase = 236  '
                                  'and numeroOP like '+ numeroOP, conn)
        conn.close()
        df_tags = pd.merge(df_tags, df_opstotal, on='numeroop', how='left')
        df_tags['totalop'] = df_tags['totalop'].replace('', numpy.nan).fillna('0')
        df_tags['codnaturezaatual'] = df_tags['codnaturezaatual'].astype(str)
        df_tags['totalop'] = df_tags['totalop'].astype(int)
        epc = LerEPC(codbarras)
        df_tags = pd.merge(df_tags, epc, on='codbarrastag', how='left')
        df_tags.rename(columns={'codbarrastag': 'codbarrastag', 'codEngenharia': 'engenharia'
            , 'numeroop': 'numeroop'}, inplace=True)
        df_tags['epc'] = df_tags['epc'].str.extract('\|\|(.*)').squeeze()
        dataHora = obterHoraAtual()
        df_tags['DataHora'] = dataHora

        return df_tags
    else:
        return pd.DataFrame([{'mensagem':False}])



def LerEPC(codbarras):
    conn = ConexaoCSW.Conexao()
    codbarras = codbarras

    consulta = pd.read_sql('select epc.id as epc, t.codBarrasTag as codbarrastag from tcr.SeqLeituraFase  t '
                           'join Tcr_Rfid.NumeroSerieTagEPC epc on epc.codTag = t.codBarrasTag '
                           'WHERE codBarrasTag = '+ codbarras ,conn)
    conn.close()

    return consulta


def InserirTagAvulsa(codbarras, codnaturezaatual, engenharia, codreduzido, descricao,
                     numeroop, cor , tamanho, epc, DataHora, totalop, codempresa):
    conn = ConexaoCSW.Conexao()

    insert = 'insert into "Reposicao".filareposicaoportag f ' \
             '(codbarras, codnaturezaatual, engenharia, codreduzido, descricao, numeroop, cor , tamanho, epc, DataHora, totalop, codempresa) ' \
             'values ' \
             '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

    cursor = conn.cursor()  # Crie um cursor para executar a consulta SQL
    cursor.execute(insert, (
    codbarras, codnaturezaatual, engenharia, codreduzido, descricao, numeroop, cor, tamanho, epc, DataHora, totalop,
    codempresa))
    conn.commit()  # Faça o commit da transação
    cursor.close()  # Feche o cursor

    conn.close()  # Feche a conexão com o banco de dados




    conn.close()
    return pd.DataFrame([{'Mensagem': 'A Tag Foi inserida no WMS'}])



