import pandas as pd
import ConexaoPostgreMPL


def EmpresaEscolhida():
    conn = ConexaoPostgreMPL.conexao()
    empresa = pd.read_sql('Select codempresa from "Reposicao".configuracoes.empresa ',conn)
    conn.close()

    return empresa['codempresa'][0]

def RegraDeEnderecoParaSubstituto():
    conn = ConexaoPostgreMPL.conexao()
    empresa = pd.read_sql('Select implenta_endereco_subs from "Reposicao".configuracoes.empresa ',conn)
    conn.close()

    return empresa['implenta_endereco_subs'][0]