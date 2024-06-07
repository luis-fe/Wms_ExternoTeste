import gc

import ConexaoCSW
import pandas as pd
import numpy
import datetime
import pytz

import ConexaoPostgreMPL
from models.AutomacaoWMS_CSW import controle


def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str
def RecarregarTagFila(codbarras):
    valor = ConexaoCSW.pesquisaTagCSW(codbarras)

    if valor['stauts conexao'][0]==True:
        conn = ConexaoCSW.Conexao() # Abrir conexao com o csw
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
        conn.close()# Encerrar conexao com o csw
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
    conn = ConexaoCSW.Conexao()#abrir conexao o csw
    codbarras = codbarras

    consulta = pd.read_sql('select epc.id as epc, t.codBarrasTag as codbarrastag from tcr.SeqLeituraFase  t '
                           'join Tcr_Rfid.NumeroSerieTagEPC epc on epc.codTag = t.codBarrasTag '
                           'WHERE codBarrasTag = '+ codbarras ,conn)
    conn.close()#encerrar conexao com o csw

    return consulta


def InserirTagAvulsa(codbarras, codnaturezaatual, engenharia, codreduzido, descricao,
                     numeroop, cor , tamanho, epc, DataHora, totalop, codempresa):
    conn = ConexaoPostgreMPL.conexao()# Abrir conexao com o Postgre

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

def BuscarTagsGarantia(rotina, ip, datahoraInicio, emp):

    query = """
    SELECT 
        p.codBarrasTag as codbarrastag, 
        p.codReduzido as codreduzido, 
        p.codEngenharia as engenharia,
        (SELECT i.nome FROM cgi.Item i WHERE i.codigo = p.codReduzido) as descricao, 
        situacao, 
        codNaturezaAtual as natureza, 
        codEmpresa as codempresa,
        (SELECT s.corbase || '-' || s.nomecorbase FROM tcp.SortimentosProduto s WHERE s.codempresa = 1 AND s.codproduto = p.codEngenharia AND s.codsortimento = p.codSortimento) as cor, 
        (SELECT t.descricao FROM tcp.Tamanhos t WHERE t.codempresa = 1 AND t.sequencia = p.seqTamanho) as tamanho, 
        p.numeroOP as numeroop
    FROM 
        Tcr.TagBarrasProduto p 
    WHERE 
        p.codEmpresa = ? AND 
        p.numeroOP IN (
            SELECT numeroOP  
            FROM tco.OrdemProd o 
            WHERE codEmpresa = ? AND codFaseAtual IN (210, 320, 56, 432, 441, 452, 423, 433, 437, 429, 365) AND situacao = 3
        )
    """

    with ConexaoCSW.Conexao() as conn:
        with conn.cursor() as cursor_csw:
            # Executa a consulta e armazena os resultados
            cursor_csw.execute(query, (emp, emp))
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            consulta = pd.DataFrame(rows, columns=colunas)
            del rows

    # Salva o status da etapa 1
    etapa1 = controle.salvarStatus_Etapa1(rotina, ip, datahoraInicio, 'etapa csw Tcr.TagBarrasProduto p')

    # Busca restrições e substitutos
    restringe = BuscaResticaoSubstitutos()

    if restringe['numeroop'][0] != 'vazio':
        consulta.fillna('-', inplace=True)
        consulta = pd.merge(consulta, restringe, on=['numeroop', 'cor'], how='left')
        consulta['resticao'].fillna('-', inplace=True)
    else:
        consulta['resticao'] = '-'
        consulta['considera'] = '-'

    # Salva o status da etapa 2
    etapa2 = controle.salvarStatus_Etapa2(rotina, ip, etapa1, 'Adicionando os substitutos selecionados no wms')

    return consulta, etapa2

def SalvarTagsNoBancoPostgre(rotina, ip, datahoraInicio, empresa):
    consulta, etapa2 = BuscarTagsGarantia(rotina, ip, datahoraInicio, empresa)
    ConexaoPostgreMPL.Funcao_InserirOFF(consulta, consulta.size, 'filareposicaoof', 'replace')
    etapa3 = controle.salvarStatus_Etapa3(rotina, ip, etapa2, 'Adicionar as tags ao wms')
    del consulta, etapa2
    gc.collect()

def BuscaResticaoSubstitutos():
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql("select numeroop , codproduto||'||'||cor||'||'||numeroop  as resticao,  "
                            'cor, considera  from "Reposicao"."Reposicao"."SubstitutosSkuOP"  '
                           "sso where sso.considera = 'sim' ",conn)

    conn.close()

    if consulta.empty:

        return pd.DataFrame([{'numeroop':'vazio'}])

    else:

        return consulta