import ConexaoPostgreMPL
import ConexaoCSW
import pandas as pd

def PesquisaEnderecos (Coluna,Operador,Nome):
    conn = ConexaoPostgreMPL.conexao()

    consulta = f'select "Referencia", "Endereco" from silk."enderecamento"  WHERE "{Coluna}" {Operador} %s'
    valor = (Nome,)
    cursor = conn.cursor()
    cursor.execute(consulta, valor)
    resultados = []
    for resultado in cursor.fetchall():
        resultados.append(resultado)

    cursor.close()
    conn.close()
    print('REALIZADO CONSULTA DE ENDEREÇO DAS TELAS DE SILK')
    return resultados





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
    numeroOP = "'%"+numeroOP+"%'"
    pesquisar = pd.read_sql('SELECT op.codProduto as referencia FROM tco.OrdemProd op '
                            'WHERE op.codEmpresa =  1 and op.numeroOP like '+numeroOP,conn)

    pesquisar['referencia'] = pesquisar['referencia'].str[4:9]
    return pesquisar


