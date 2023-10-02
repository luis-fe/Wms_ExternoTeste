import ConexaoPostgreMPL
import ConexaoCSW
import pandas as pd


def ObterTipoNota(empresa):
    try:
        conn = ConexaoPostgreMPL.conexao()
        query = pd.read_sql('select tiponota, desc_tipo_nota from "Reposicao".conftiponotacsw '
                            ' where empresa = %s ',conn, params=(empresa,))

        conn.close()
        return query
    except:
        return pd.DataFrame([{'Total Faturado':f'Conexao CSW perdida'}])


def obter_notaCsw():
    conn = ConexaoCSW.Conexao()
    data = pd.read_sql(" select t.codigo ,t.descricao  from Fat.TipoDeNotaPadrao t ", conn)
    conn.close()

    return data


def Faturamento(empresa, dataInicio, dataFim, detalhar = False):

    try:
        tipo_nota = ObterTipoNota(empresa)
        conn = ConexaoCSW.Conexao()
        dataframe = pd.read_sql('select n.codTipoDeNota as tiponota, n.dataEmissao, sum(n.vlrTotal) as faturado'
                                '  FROM Fat.NotaFiscal n '
                                'where n.codEmpresa = '+empresa+' and n.codPedido >= 0 and n.dataEmissao >= '+"'"+dataInicio+"'"+' '
                                'and n.dataEmissao <= '+"'"+dataFim+"'"+'and situacao = 2 '
                                'group by n.dataEmissao , n.codTipoDeNota ',conn)

        retornaCsw = pd.read_sql("SELECT  i.codPedido, e.vlrSugestao, sum(i.qtdePecasConf) as conf , sum(i.qtdeSugerida) as qtde  FROM ped.SugestaoPed e "
                                " inner join ped.SugestaoPedItem i on i.codEmpresa = e.codEmpresa and i.codPedido = e.codPedido "
                                ' WHERE e.codEmpresa ='+empresa+
                                " and e.dataGeracao > '2023-01-01' and situacaoSugestao = 2"
                            " group by i.codPedido, e.vlrSugestao ", conn )

        retornaCsw2 = retornaCsw[retornaCsw['conf']==0]
        retorna = retornaCsw2['vlrSugestao'].sum()
        pecas = retornaCsw2['qtde'].sum()

        pecas = "{:,.0f}".format(pecas)
        pecas = pecas.replace(',', ".")

        retorna = "{:,.2f}".format(retorna)
        retorna = 'R$ ' + str(retorna)
        retorna = retorna.replace('.', ";")
        retorna = retorna.replace(',',".")
        retorna = retorna.replace(';', ",")

        conn.close()
        dataframe['tiponota'] = dataframe['tiponota'].astype(str)
        dataframe = pd.merge(dataframe,tipo_nota,on="tiponota")

        faturado = dataframe['faturado'].sum()
        faturado = "{:,.2f}".format(faturado)

        faturado = 'R$ '+str(faturado)
        faturado = faturado.replace('.', ";")
        faturado = faturado.replace(',',".")
        faturado = faturado.replace(';', ",")
        if detalhar == False:
         return pd.DataFrame([{'Total Faturado':f'{faturado}','No Retorna':f'{retorna}','Pcs Retorna':f'{pecas} pçs'}])
        else:
            return retornaCsw
    except:
        return pd.DataFrame([{'Total Faturado':f'Conexao CSW perdida'}])



