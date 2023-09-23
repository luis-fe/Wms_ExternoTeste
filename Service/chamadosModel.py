import pandas as pd
import ConexaoPostgreMPL

def Obter_chamados():
    conn = ConexaoPostgreMPL.conexao()
    query = pd.read_sql('select id_chamado, solicitante, data_chamado, '
                        ' tipo_chamado, atribuido_para, descricao_chamado, status_chamado, '
                        'data_finalizacao_chamado from "Reposicao".chamados '
                        'order by data_chamado', conn)
    query.fillna('-', inplace=True)

    conn.close()
    return query

def novo_chamados(solicitante, data_chamado, tipo_chamado, atribuido_para, descricao_chamado, status_chamado, data_finalizacao_chamado):
    conn = ConexaoPostgreMPL.conexao()
    cursor = conn.cursor()
    cursor.execute('INSERT INTO "Reposicao"."chamados" (solicitante, data_chamado, tipo_chamado, atribuido_para, '
                   'descricao_chamado, status_chamado, data_finalizacao_chamado) '
                   'VALUES (%s, %s, %s, %s, %s, %s, %s)',(solicitante, data_chamado, tipo_chamado, atribuido_para, descricao_chamado, status_chamado, data_finalizacao_chamado))

    conn.commit()
    cursor.close()
    conn.close()
    return True

def encerrarchamado(id_chamado, data_finalizacao_chamado):
    conn = ConexaoPostgreMPL.conexao()
    cursor = conn.cursor()
    cursor.execute('UPDATE "Reposicao"."chamados" '
                   'SET data_finalizacao_chamado = %s'
                   ' WHERE id_chamado = %s')
    conn.commit()
    cursor.close()
    conn.close()
    return True