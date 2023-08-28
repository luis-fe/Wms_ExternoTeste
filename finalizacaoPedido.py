import pandas as pd
import ConexaoPostgreMPL
import datetime
import pytz


def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    return hora_str


def VerificarExisteApontamento(codpedido, usuario):
    conn = ConexaoPostgreMPL.conexao()
    codpedido = str(codpedido)
    query = pd.read_sql('select codpedido from "Reposicao".tags_separacao '
                        ' where codpedido = %s '
                        , conn, params=(codpedido,))
    conn.close()

    if query.empty:
        conn = ConexaoPostgreMPL.conexao()
        select = pd.read_sql('select * from "Reposicao".finalizacao_pedido fp'
                             ' where codpedido = %s ', conn, params=(codpedido,))
        if select.empty:
            print('teste')
            insert = 'insert into "Reposicao".finalizacao_pedido (codpedido, usuario, "dataInicio") values (%s, %s, %s)'
            datahora = obterHoraAtual()
            cursor = conn.cursor()
            cursor.execute(insert, (codpedido, usuario, datahora))
            conn.commit()
            conn.close()

        else:
            update = 'update "Reposicao".finalizacao_pedido' \
                     ' set usuario = %s , "dataInicio" = %s ' \
                     ' where codpedido = %s'
            datahora = obterHoraAtual()
            cursor = conn.cursor()
            cursor.execute(update, (usuario, datahora, codpedido))
            conn.commit()
            conn.close()

    else:
        print('ok')

