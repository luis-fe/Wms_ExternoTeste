import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd

def ApontarTag(codbarras, Ncaixa, empresa, usuario):
    conn = ConexaoCSW.Conexao()
    codbarras = "'"+codbarras+"'"

    pesquisa = pd.read_sql('select p.codBarrasTag as codbarrastag , p.codReduzido as codreduzido, p.codEngenharia as engenharia, '
                           ' (select i.nome from cgi.Item i WHERE i.codigo = p.codReduzido) as descricao, situacao, codNaturezaAtual as natureza, codEmpresa as codempresa, '
                           " (select s.corbase||'-'||s.nomecorbase  from tcp.SortimentosProduto s WHERE s.codempresa = 1 and s.codproduto = p.codEngenharia and s.codsortimento = p.codSortimento)" 
                           ' as cor, (select t.descricao from tcp.Tamanhos t WHERE t.codempresa = 1 and t.sequencia = p.seqTamanho ) as tamanho, p.numeroOP as numeroop '
                           ' FROM Tcr.TagBarrasProduto p'
                           ' WHERE p.codBarrasTag = '+codbarras+' and p.codempresa ='+empresa,conn)
    conn.close()
    pesquisa['usuario'] = usuario
    pesquisa['caixa'] = Ncaixa
    InculirDados(pesquisa)
    return pesquisa




def InculirDados(dataframe):
        conn = ConexaoPostgreMPL.conexao()

        cursor = conn.cursor()  # Crie um cursor para executar a consulta SQL
        insert =  'insert into off.reposicao_qualidade (codbarrastag, codreduzido, engenharia, descricao, natureza, codempresa, cor, tamanho, numeroop, caixa, usuario)' \
                  ' values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )'


        values = [(row['codbarrastag'], row['codreduzido'], row['engenharia'],row['descricao']
                   ,row['natureza'],row['codempresa'],row['cor'],row['tamanho'],row['numeroop'], row['caixa'],row['usuario']) for index, row in dataframe.iterrows()]

        cursor.executemany(insert, values)
        conn.commit()  # Faça o commit da transação
        cursor.close()  # Feche o cursor



        conn.close()
def EncontrarEPC(caixa):
    #Passo1: Pesquisar em outra funcao um dataframe que retorna a coluna numeroop
    caixaNova = ConsultaCaixa(caixa)

    #Passo2: Retirar do dataframe somente a coluna numeroop
    ops1 = caixaNova[['numeroop']]
    ops1 = ops1.drop_duplicates(subset=['numeroop'])

    # Passo 3: Transformar o dataFrame em lista
    resultado = '({})'.format(', '.join(["'{}'".format(valor) for valor in ops1['numeroop']]))

    conn = ConexaoCSW.Conexao()

    # Use parâmetro de substituição na consulta SQL
   # epc = pd.read_sql('SELECT t.codBarrasTag AS codbarrastag, numeroOP, (SELECT epc.id FROM Tcr_Rfid.NumeroSerieTagEPC epc WHERE epc.codTag = t.codBarrasTag) AS epc ' \
    #        'FROM tcr.SeqLeituraTagOPFase t WHERE t.codempresa = 1 and t.numeroOP IN '+resultado,conn)





    return pd.DataFrame({'mensagem':f'{resultado}'})

def ConsultaCaixa(NCaixa):
    conn = ConexaoPostgreMPL.conexao()
    consultar = pd.read_sql('select rq.codbarrastag , rq.numeroop  from "off".reposicao_qualidade rq  '
                            "where rq.caixa = %s ",conn,params=(NCaixa,))
    conn.close()

    return consultar
