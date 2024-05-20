import ConexaoCSW
import pandas as pd
import models.configuracoes.empresaConfigurada


def Confronto():
    emp = models.configuracoes.empresaConfigurada.EmpresaEscolhida()
    conn = ConexaoCSW.Conexao() # Abrir conexao com o Csw

    posicao = pd.read_sql("SELECT d.codItem as reduzido, d.estoqueAtual as posicao_estoque FROM est.DadosEstoque d "
                          "where d.codNatureza = 5 and codEmpresa = "+emp+" and estoqueAtual > 0 ", conn)

    em_Conferencia = pd.read_sql("SELECT codReduzido as reduzido, COUNT(codBarrasTag) as em_conferencia FROM tcr.TagBarrasProduto t "
                                 "WHERE t.codEmpresa = "+emp+" and t.situacao = 4 and codNaturezaAtual = 5 "
                                 "group by codReduzido ", conn)

    wms = pd.read_sql("SELECT codReduzido as reduzido, COUNT(codBarrasTag) as situacao3 FROM tcr.TagBarrasProduto t "
                                 "WHERE t.codEmpresa = "+emp+ " and t.situacao = 3 and codNaturezaAtual = 5 "
                                 "group by codReduzido ", conn)


    conn.close() # Encerrar a conexao com o csw

    if not em_Conferencia.empty:
        em_Conferencia = em_Conferencia
        totalConferido = em_Conferencia['em_conferencia'].sum()
        totalConferido = totalConferido.round(0)
    else:
        totalConferido = em_Conferencia['em_conferencia'].sum()

    emEstoque = wms['situacao3'].sum()
    emEstoque = emEstoque.round(0)





    posicaoEstoque = posicao['posicao_estoque'].sum()



    posicao['posicao_estoque'] = posicao['posicao_estoque'].astype(int)
    em_Conferencia['em_conferencia'] = em_Conferencia['em_conferencia'].astype(int)
    wms['situacao3'] = wms['situacao3'].astype(int)

    consulta = pd.merge(posicao, em_Conferencia, on="reduzido", how="left")

    consulta = consulta.sort_values(by='posicao_estoque', ascending=False,
                                ignore_index=True)


    consulta = pd.merge(consulta, wms, on="reduzido", how="right")
    consulta.fillna(0, inplace=True)
    consulta['diferenca'] = consulta['posicao_estoque'] - (consulta['em_conferencia'] + consulta['situacao3'])
    consulta = consulta.sort_values(by='diferenca', ascending=True,
                                ignore_index=True)

    consulta = consulta[consulta['diferenca'] != 0]

    totalWMS = emEstoque  + totalConferido

    emEstoque = "{:,.0f}".format(emEstoque)
    posicaoEstoque = "{:,.0f}".format(posicaoEstoque)

    emEstoque = str(emEstoque)
    emEstoque = emEstoque.replace(',', '.')
    posicaoEstoque = str(posicaoEstoque)
    posicaoEstoque = posicaoEstoque.replace(',', '.')
    totalWMS = str(totalWMS)
    totalWMS = totalWMS.replace(',', '.')

    data = {

        '1.1- Tags em Conferencia ': totalConferido ,
        '1.2 - Tags Em estoque:':emEstoque,
        '2 - No PosicaoCSW': posicaoEstoque,
        '1.3 - Total no WMS':totalWMS,
        '4- Detalhamento ': consulta.to_dict(orient='records')
    }
    return pd.DataFrame([data])





