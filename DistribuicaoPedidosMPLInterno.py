import ConexaoPostgreMPL
import pandas as pd

import PediosApontamento


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

    if tipo == 'desc':
        return fila.sort_values(by=coluna, ascending=False)

    else:
        return fila.sort_values(by=coluna, ascending=True)






