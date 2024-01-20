import datetime
import pandas as pd
import ConexaoPostgreMPL
from psycopg2 import errors


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
        conn.commit()
        cursor.close()
        conn.close()

        return pd.DataFrame([{'Mensagem':'Linha cadastrado com sucesso'}])


    elif operador3 == '-' and operador2 != '-':
        insertInto = 'insert into "Reposicao".off."linhapadrado" ' \
                     '("Linha", operador1, operador2) values (%s, %s, %s)'
        cursor = conn.cursor()
        try:
            cursor.execute(insertInto, (nomeLinha, operador1, operador2))
            conn.commit()
            cursor.close()
            conn.close()
            return pd.DataFrame([{'Mensagem': 'Linha cadastrado com sucesso'}])

        except errors.ForeignKeyViolation as e:
            cursor.close()
            conn.close()
            # Se uma exceção de violação de chave estrangeira ocorrer, imprima a mensagem de erro ou faça o que for necessário
            return pd.DataFrame([{'Mensagem': f'NÃO EXISTE O NOME DO OPERADOR 1: {operador1} no cadastro de usuarios do portal !'}])


        except Exception as e:
            # Lide com outras exceções aqui, se necessário
            cursor.close()
            conn.close()
            print(f"Erro inesperado: {e}")



    elif operador2 == '-':
        insertInto = 'insert into "Reposicao".off."linhapadrado" ' \
                     '("Linha", operador1) values (%s, %s)'
        cursor = conn.cursor()
        try:
            cursor.execute(insertInto, (nomeLinha, operador1))
            conn.commit()
            mensagem = pd.DataFrame([{'Mensagem': 'Linha cadastrado com sucesso'}])

        except errors.ForeignKeyViolation as e:
            # Se uma exceção de violação de chave estrangeira ocorrer, imprima a mensagem de erro ou faça o que for necessário
            cursor.close()
            conn.close()
            print(f"Erro inesperado nome operador1: {e}")

            mensagem = pd.DataFrame([{'Mensagem': f'NÃO EXISTE O NOME DO OPERADOR 1: {operador1} no cadastro de usuarios do portal !'}])


        except Exception as e:
            # Lide com outras exceções aqui, se necessário
            cursor.close()
            conn.close()
            print(f"Erro inesperado: {e}")
            mensagem = pd.DataFrame(
                [{'Mensagem': f'NÃO EXISTE O NOME DO OPERADOR 1: {operador1} no cadastro de usuarios do portal !'}])

        cursor.close()
        conn.close()
        return mensagem

