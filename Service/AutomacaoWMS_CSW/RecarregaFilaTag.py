import ConexaoCSW
import pandas as pd
import numpy


def RecarregarTagFila(codbarras):
    valor = ConexaoCSW.pesquisaTagCSW(codbarras)

    if valor['stauts conexao'][0]==True:
        conn = ConexaoCSW.Conexao2()
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

        df_tags = pd.merge(df_tags, df_opstotal, on='numeroop', how='left')
        df_tags['totalop'] = df_tags['totalop'].replace('', numpy.nan).fillna('0')
        df_tags['codnaturezaatual'] = df_tags['codnaturezaatual'].astype(str)
        df_tags['totalop'] = df_tags['totalop'].astype(int)

        return df_tags
    else:
        return pd.DataFrame([{'mensagem':False}])
