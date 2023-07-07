import ConexaoPostgreMPL
import pandas as pd


def Deletar_Endereco(Endereco):
    conn = ConexaoPostgreMPL.conexao()
    # Validar se existe Restricao Para excluir o endereo
    Validar = pd.read_sql(
        'select "Endereco" from "Reposicao".tagsreposicao '
        'where "Endereco" = '+"'"+Endereco+"'", conn)
    if not Validar.empety:
         return pd.DataFrame({'Mensagem': [f'Endereco com saldo, nao pode ser excluido'], 'Status':False})
    else:
        delatar = 'delete from "Reposicao".cadendereco ' \
                  'where codendereco = %s '
        # Execute a consulta usando a conexão e o cursor apropriados
        cursor = conn.cursor()
        cursor.execute(delatar, (Endereco))
        conn.commit()
        return pd.DataFrame({'Mensagem': [f'Endereco excluido!'], 'Status':True})

