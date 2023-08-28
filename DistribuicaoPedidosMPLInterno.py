import ConexaoPostgreMPL
import pandas as pd

import PediosApontamento
import datetime
import pytz


def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    return hora_str

def AtribuirPedido(usuario, pedidos, dataAtribuicao):
    tamanho = len(pedidos)
    pedidosNovo = []
    for i in range(tamanho):
       incr =  str(pedidos[i])
       pedidosNovo.append(incr)

    pedidosNovo = [p.replace(',', '/') for p in pedidosNovo]



    if tamanho >= 0:
        conn = ConexaoPostgreMPL.conexao()
        for i in range(tamanho):
            pedido_x = str(pedidosNovo[i])
            query = 'update "Reposicao".filaseparacaopedidos '\
                    'set cod_usuario = %s '\
                    'where codigopedido = %s'
            cursor = conn.cursor()
            cursor.execute(query, (usuario,pedido_x, ))
            conn.commit()
            cursor.close()
            consulta1 = pd.read_sql('select datageracao from "Reposicao".filaseparacaopedidos '
                                   ' where codigopedido = %s', conn, params=(pedido_x,))

            consulta2 =  pd.read_sql('select * from "Reposicao".finalizacao_pedido '
                                   ' where codpedido = %s', conn, params=(pedido_x,))
            dataatual = obterHoraAtual()
            if consulta2.empty:
                cursor2 = conn.cursor()

                insert = 'insert into "Reposicao".finalizacao_pedido (codpedido, datageracao, dataatribuicao) values (%s, %s, %s)'
                cursor2.execute(insert, (pedido_x, consulta1['datageracao'][0],dataatual))
                conn.commit()


                cursor2.close()
            else:
                cursor2 = conn.cursor()

                update = 'update "Reposicao".finalizacao_pedido ' \
                         'set datageracao = %s , dataatribuicao = %s ' \
                         'where codpedido = %s'
                cursor2.execute(update, (consulta1['datageracao'][0], dataatual,pedido_x))
                conn.commit()


                cursor2.close()
        conn.close()
    else:
        print('sem pedidos')

    data = {
        '1- Usuario:': usuario,
        '2- Pedidos para Atribuir:': pedidos,
        '3- dataAtribuicao:': dataAtribuicao
    }
    return [data]

def ClassificarFila(coluna, tipo):
    fila = PediosApontamento.FilaPedidos()
    fila['12-vlrsugestao'] = fila['12-vlrsugestao'].str.replace("R\$", "").astype(float)

    if tipo == 'desc':
        fila = fila.sort_values(by=coluna, ascending=False)
        fila['12-vlrsugestao'] = fila['12-vlrsugestao'] .astype(str)
        fila['12-vlrsugestao'] = 'R$ ' + fila['12-vlrsugestao']

        return fila

    else:
        fila['12-vlrsugestao'] = fila['12-vlrsugestao'] .astype(str)
        fila['12-vlrsugestao'] = 'R$ ' + fila['12-vlrsugestao']
        fila = fila.sort_values(by=coluna, ascending=True)
        return fila






