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

    wms = pd.read_sql("SELECT codReduzido as reduzido, COUNT(codBarrasTag) as em_estoque FROM tcr.TagBarrasProduto t "
                                 "WHERE t.codEmpresa = 4 and t.situacao = 3 and codNaturezaAtual = 5 "
                                 "group by codReduzido ", conn)


    conn.close()

    consulta = pd.merge(posicao, em_Conferencia, on="reduzido", how="left")

    consulta = consulta.sort_values(by='posicao_estoque', ascending=False,
                                ignore_index=True)
    consulta.fillna('-', inplace=True)

    consulta = pd.merge(consulta, wms, on="reduzido", how="left")

    return consulta





