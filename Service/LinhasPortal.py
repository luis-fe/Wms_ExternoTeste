import datetime

import ConexaoPostgreMPL


def obterHoraAtual():
    agora = datetime.datetime.now()
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str

def PesquisarLinhaPadrao():
    conn = ConexaoPostgreMPL.conexao()
    cursor = conn.cursor()
    cursor.execute('select * from "Reposicao".off."linhapadrado" c order by "Linha" asc')
    linhas = cursor.fetchall()
    cursor.close()
    conn.close()
    return linhas
