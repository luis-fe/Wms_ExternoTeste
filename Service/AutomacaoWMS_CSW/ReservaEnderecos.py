import pandas as pd
import ConexaoPostgreMPL
from psycopg2 import sql

# Passo 1 : Criei uma View no banco de dados para mostrar o saldo reservado no nivel: sku, endereço


def EstornarReservasNaoAtribuidas():
    conn = ConexaoPostgreMPL.conexao()

    # Acessando os pedidos com enderecos reservados
    queue = pd.read_sql('select * from "Reposicao".pedidossku '
                        "where necessidade > 0 and reservado = 'sim' ",conn)

    # Acessando os pedidos NAO atribuidos
    queue2 = pd.read_sql('select codigopedido as codpedido, f.cod_usuario  from "Reposicao".filaseparacaopedidos f  '
                        "where cod_usuario is null ",conn)

    # Obtendo somente os enderecos reservados com pedidos nao atribuidos
    queue = pd.merge(queue,queue2,on='codpedido')
    tamanho = queue['codpedido'].size
    # transformando o codigo do pedido em lista
    # Obter os valores para a cláusula WHERE do DataFrame
    lista = queue['codpedido'].tolist()
    # Construir a consulta DELETE usando a cláusula WHERE com os valores do DataFrame

    query = sql.SQL('UPDATE  "Reposicao"."pedidossku" '
                    "SET reservado = 'nao' "
                    'WHERE codpedido IN ({})').format(
        sql.SQL(',').join(map(sql.Literal, lista))
    )

    if tamanho != 0:
        # Executar a consulta DELETE
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()

    cursor.close()
    conn.close()

    return pd.DataFrame([{'Mensagem': f'foram estornado a reserva de {tamanho} endereços'}])