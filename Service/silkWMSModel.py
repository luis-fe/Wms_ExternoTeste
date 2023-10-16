import ConexaoPostgreMPL
import ConexaoCSW
import pandas as pd


def PesquisaEnderecos(Coluna, Operador, Nome):
    # Estabeleça uma conexão com o banco de dados
    conn = ConexaoPostgreMPL.conexao()

    # Crie a consulta SQL com base nos argumentos
    consulta = f'SELECT "Referencia", "Endereco" FROM silk."enderecamento" WHERE "{Coluna}" {Operador} %s'
    valor = (Nome,)

    # Execute a consulta e obtenha os resultados
    cursor = conn.cursor()
    cursor.execute(consulta, valor)
    resultados = cursor.fetchall()

    cursor.close()
    conn.close()

    # Crie um DataFrame Pandas com base nos resultados
    df = pd.DataFrame(resultados, columns=['Referencia', 'Endereco'])

    return df





def Funcao_Deletar (Endereco,Produto):

    conn = ConexaoPostgreMPL.conexao()
    cursor =conn.cursor()

    sql= 'DELETE FROM silk."enderecamento" where "Endereco" = %s and "Referencia" = %s  '
    VALORES = (Endereco, Produto,)
    cursor.execute(sql, VALORES)
    conn.commit()
    cursor.close()

    conn.close()
    print('REALIZADO DELETE DE ENDEREÇO DO SILK NO CADASTRO')
    return True

def Funcao_Inserir (Referencia, Endereco):

    conn = ConexaoPostgreMPL.conexao()

    cursor =conn.cursor()

    sql= 'INSERT INTO silk."enderecamento" ("Referencia", "Endereco") VALUES (%s, %s)'
    VALORES = (Referencia, Endereco,)
    cursor.execute(sql, VALORES)
    conn.commit()
    cursor.close()

    conn.close()
    print('REALIZADO NOVA INSERÇÃO DE ENDEREÇO DO SILK NO CADASTRO')
    return True



def PesquisarReferencia(numeroOP):
    conn = ConexaoCSW.Conexao()

    numeroOP = "'%"+numeroOP[0:6]+"%'"
    pesquisar = pd.read_sql('SELECT op.codProduto as referencia FROM tco.OrdemProd op '
                            'WHERE op.codEmpresa =  1 and op.numeroOP like '+numeroOP,conn)

    pesquisar['referencia'] = pesquisar['referencia'].str[4:9]
    pesquisar['numeroOP'] = numeroOP
    return pesquisar


