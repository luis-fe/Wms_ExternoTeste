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


def Faturamento(empresa, dataInicio, dataFim, detalhar):

   try:
        tipo_nota = ObterTipoNota(empresa)
        conn = ConexaoCSW.Conexao()
        dataframe = pd.read_sql('select n.codTipoDeNota as tiponota, n.dataEmissao, sum(n.vlrTotal) as faturado'
                                '  FROM Fat.NotaFiscal n '
                                'where n.codEmpresa = '+empresa+' and n.codPedido >= 0 and n.dataEmissao >= '+"'"+dataInicio+"'"+' '
                                'and n.dataEmissao <= '+"'"+dataFim+"'"+'and situacao = 2 '
                                'group by n.dataEmissao , n.codTipoDeNota ',conn)

        retornaCsw = pd.read_sql("SELECT  i.codPedido, e.vlrSugestao, sum(i.qtdePecasConf) as conf , sum(i.qtdeSugerida) as qtde,  i.codSequencia,  "
                                 " (SELECT codTipoNota  FROM ped.Pedido p WHERE p.codEmpresa = i.codEmpresa and p.codpedido = i.codPedido) as codigo "
                                 " FROM ped.SugestaoPed e "
                                " inner join ped.SugestaoPedItem i on i.codEmpresa = e.codEmpresa and i.codPedido = e.codPedido "
                                ' WHERE e.codEmpresa ='+empresa+
                                " and e.dataGeracao > '2023-01-01' and situacaoSugestao = 2"
                            " group by i.codPedido, e.vlrSugestao,  i.codSequencia ", conn )

        tipoNota = obter_notaCsw()
        tipoNota['codigo'] = tipoNota['codigo'].astype(str)
        retornaCsw = pd.merge(retornaCsw,tipoNota, on='codigo' )


        retornaCsw["codPedido"]=retornaCsw["codPedido"]+'-'+retornaCsw["codSequencia"]

        # Retirando as bonificacoes
        retornaCswSB = retornaCsw[retornaCsw['codigo'] != 39]
        retornaCswMPLUS = retornaCsw[retornaCsw['codigo'] == 39]

        prontaEntrega = retornaCsw[
            (retornaCsw['codigo'] == 66) | (retornaCsw['codigo'] == 67) | (retornaCsw['codigo'] == 236) | (
                        retornaCsw['codigo'] == 237)]

        retornaCswSB = retornaCswSB[retornaCswSB['conf']==0]
        retornaCswMPLUS = retornaCswMPLUS[retornaCswMPLUS['conf']==0]
        prontaEntrega = prontaEntrega[retornaCswMPLUS['conf'] == 0]


        retorna = retornaCswSB['vlrSugestao'].sum()
        ValorRetornaMplus = retornaCswMPLUS['vlrSugestao'].sum()
        prontaEntrega = prontaEntrega['vlrSugestao'].sum()

        pecasSB = retornaCswSB['qtde'].sum()

        pecasSB = "{:,.0f}".format(pecasSB)
        pecasSB = pecasSB.replace(',', ".")

        pecasMplus = retornaCswMPLUS['qtde'].sum()

        pecasMplus = "{:,.0f}".format(pecasMplus)
        pecasMplus = pecasMplus.replace(',', ".")

        retorna = "{:,.2f}".format(retorna)
        retorna = 'R$ ' + str(retorna)
        retorna = retorna.replace('.', ";")
        retorna = retorna.replace(',',".")
        retorna = retorna.replace(';', ",")

        ValorRetornaMplus = "{:,.2f}".format(ValorRetornaMplus)
        ValorRetornaMplus = 'R$ ' + str(ValorRetornaMplus)
        ValorRetornaMplus = ValorRetornaMplus.replace('.', ";")
        ValorRetornaMplus = ValorRetornaMplus.replace(',',".")
        ValorRetornaMplus = ValorRetornaMplus.replace(';', ",")

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
            return pd.DataFrame([{'Total Faturado':f'{faturado}','No Retorna':f'{retorna}','Pcs Retorna':f'{pecasSB} pçs','No Retorna MPlus':f'{ValorRetornaMplus}',
                                  'Pcs Retorna Mplus':f'{pecasMplus} pçs','Retorna ProntaEntrega':f'{prontaEntrega}'}])
        else:
            return retornaCsw
   except:
        return pd.DataFrame([{'Total Faturado':f'Conexao CSW perdida','No Retorna':f'conexao perdida','Pcs Retorna':f'conexao perdida','No Retorna MPlus':f'conexao perdida',
                                  'Pcs Retorna Mplus':f'conexao perdida'}])



