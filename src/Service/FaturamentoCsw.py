import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd
import locale

def ObterTipoNota(empresa):
    conn = ConexaoPostgreMPL.conexao()
    query = pd.read_sql('select tiponota, desc_tipo_nota from "Reposicao".conftiponotacsw '
                        ' where empresa = %s ',conn, params=(empresa,))

    conn.close()
    return query


def obter_notaCsw():
    conn = ConexaoCSW.Conexao()
    data = pd.read_sql(" select t.codigo ,t.descricao  from Fat.TipoDeNotaPadrao t ", conn)
    conn.close()

    return data


def Faturamento(empresa, dataInicio, dataFim):
    def format_with_separator(value):
        return locale.format('%0.0f', value, grouping=True)


    tipo_nota = ObterTipoNota(empresa)
    conn = ConexaoCSW.Conexao()
    dataframe = pd.read_sql('select n.codTipoDeNota as tiponota, n.dataEmissao, sum(n.vlrTotal) as faturado  FROM Fat.NotaFiscal n '
                            'where n.codEmpresa = '+empresa+' and n.codPedido >= 0 and n.dataEmissao >= '+"'"+dataInicio+"'"+' '
                            'and n.dataEmissao <= '+"'"+dataFim+"'"+'and situacao = 2 '
                            'group by n.dataEmissao , n.codTipoDeNota ',conn)
    conn.close()
    dataframe['tiponota'] = dataframe['tiponota'].astype(str)
    dataframe = pd.merge(dataframe,tipo_nota,on="tiponota")
    faturado = dataframe['faturado'].sum()
    faturado = "{:,.2f}".format(faturado)
    faturado = format_with_separator(faturado)
    faturado = 'R$ '+str(faturado)
    faturado = faturado.replace(',',".")
    return pd.DataFrame({'Total Faturado':[faturado]})



