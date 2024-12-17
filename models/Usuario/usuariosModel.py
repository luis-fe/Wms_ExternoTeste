
import ConexaoPostgreMPL



def AtualizarInformacoes(novo_nome, nova_funcao, nova_situacao,  codigo):
    conn = ConexaoPostgreMPL.conexao()
    cursor = conn.cursor()
    cursor.execute('UPDATE "Reposicao"."cadusuarios" SET nome=%s, funcao=%s, situacao= %s WHERE codigo=%s',(novo_nome,nova_funcao,nova_situacao, codigo))
    conn.commit()
    cursor.close()
    conn.close()
    return novo_nome

