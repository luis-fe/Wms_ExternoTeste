import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd

def ApontarTag(codbarras, Ncaixa, empresa):
    conn = ConexaoCSW.Conexao()
    codbarras = "'"+codbarras+"'"

    pesquisa = pd.read_sql('select p.codBarrasTag as codbarrastag , p.codReduzido as codreduzido, p.codEngenharia as engenharia, '
                           ' (select i.nome from cgi.Item i WHERE i.codigo = p.codReduzido) as descricao, situacao, codNaturezaAtual as natureza, codEmpresa as codempresa, '
                           " (select s.corbase||'-'||s.nomecorbase  from tcp.SortimentosProduto s WHERE s.codempresa = 1 and s.codproduto = p.codEngenharia and s.codsortimento = p.codSortimento)" 
                           ' as cor, (select t.descricao from tcp.Tamanhos t WHERE t.codempresa = 1 and t.sequencia = p.seqTamanho ) as tamanho, p.numeroOP '
                           ' FROM Tcr.TagBarrasProduto p'
                           ' WHERE p.codBarrasTag = '+codbarras+' and p.codempresa ='+empresa,conn)
    conn.close()
    InculirDados(pesquisa)
    return pesquisa


def InculirDados(dataframe):
    conn = ConexaoPostgreMPL.conexao()

    # Certifique-se de que o nome da tabela no banco de dados corresponda ao seu requisito
    tabela = 'reposicao_qualidade'

    # Usando Pandas para inserir os dados no banco de dados
    dataframe.to_sql(tabela, conn, if_exists='append', index=False)

    conn.close()
