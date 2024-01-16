import ConexaoCSW
import pandas as pd
import Service.configuracoes.empresaConfigurada


def Confronto():
    emp = Service.configuracoes.empresaConfigurada.EmpresaEscolhida()
    conn = ConexaoCSW.Conexao()

    posicao = pd.read_sql("SELECT d.codItem as reduzido, d.estoqueAtual as posicao_estoque FROM est.DadosEstoque d "
                          "where d.codNatureza = 5 and codEmpresa = "+emp+" and estoqueAtual > 0 ", conn)

    em_Conferencia = pd.read_sql("SELECT codReduzido as reduzido, COUNT(codBarrasTag) as em_conferencia FROM tcr.TagBarrasProduto t "
                                 "WHERE t.codEmpresa = 4 and t.situacao = 4 and codNaturezaAtual = 5 "
                                 "group by codReduzido ", conn)

    wms = pd.read_sql("SELECT codReduzido as reduzido, COUNT(codBarrasTag) as situacao3 FROM tcr.TagBarrasProduto t "
                                 "WHERE t.codEmpresa = 4 and t.situacao = 3 and codNaturezaAtual = 5 "
                                 "group by codReduzido ", conn)


    conn.close()

    if not em_Conferencia.empty:
        em_Conferencia = em_Conferencia
        totalConferido = em_Conferencia['em_conferencia'].sum()
        totalConferido = totalConferido.round(0)
    else:
        totalConferido = em_Conferencia['em_conferencia'].sum()

    emEstoque = wms['situacao3'].sum()
    emEstoque = emEstoque.round(0)
    emEstoque = "{:,.0f}".format(emEstoque)
    emEstoque = str(emEstoque)
    emEstoque = emEstoque.replace(',', '.')


    posicaoEstoque = posicao['posicao_estoque'].sum()
    posicaoEstoque = "{:,.0f}".format(posicaoEstoque)
    posicaoEstoque = str(posicaoEstoque)
    posicaoEstoque = posicaoEstoque.replace(',', '.')


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

    data = {

        '1- Tags em Conferencia ': totalConferido ,
        '2 - Tags Em estoque:':emEstoque,
        '3 - No PosicaoCSW': posicaoEstoque,
        '4- Detalhamento ': consulta.to_dict(orient='records')
    }
    return pd.DataFrame([data])





