import datetime
import pandas as pd
import ConexaoPostgreMPL


def obterHoraAtual():
    agora = datetime.datetime.now()
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str

def PesquisarLinhaPadrao():
    conn = ConexaoPostgreMPL.conexao()

    linhas = pd.read_sql('select * from "Reposicao".off."linhapadrado" c order by "Linha" asc',conn)

    conn.close()
    return linhas
def RetornarNomeLinha(linha):
    conn = ConexaoPostgreMPL.conexao()

    linhas = pd.read_sql('select * from "Reposicao".off."linhapadrado" c where "Linha" = %s ', conn, params=(linha,))

    conn.close()

    return pd.DataFrame([{'OperadoresLinha':linhas['operador1'][0]+'/'+linhas['operador2'][0]+'/'+linhas['operador3'][0]}])

def CadastrarLinha(nomeLinha, operador1, operador2, operador3):
    conn = ConexaoPostgreMPL.conexao()


    if operador3 != '-':
        insertInto = 'insert into "Reposicao".off."linhapadrado"' \
                 ' ("Linha",operador1, operador2, operador3) values (%s, %s, %s , %s)'
        cursor = conn.cursor()
        cursor.execute(insertInto, (nomeLinha, operador1, operador2, operador3))
        cursor.commit()
        cursor.close()
        conn.close()

        return pd.DataFrame([{'Mensagem':'Linha cadastrado com sucesso'}])


    elif operador3 == '-' and operador2 != '-':
        insertInto = 'insert into "Reposicao".off."linhapadrado" ' \
                     '("Linha", operador1, operador2) values (%s, %s, %s)'
        cursor = conn.cursor()
        cursor.execute(insertInto, (nomeLinha, operador1, operador2))
        cursor.commit()
        cursor.close()
        conn.close()
        return pd.DataFrame([{'Mensagem': 'Linha cadastrado com sucesso'}])

    elif operador2 == '-':
        insertInto = 'insert into "Reposicao".off."linhapadrado" ' \
                     '("Linha", operador1) values (%s, %s)'
        cursor = conn.cursor()
        cursor.execute(insertInto, (nomeLinha, operador1))
        cursor.commit()
        cursor.close()
        conn.close()


        return pd.DataFrame([{'Mensagem':'Linha cadastrado com sucesso'}])

