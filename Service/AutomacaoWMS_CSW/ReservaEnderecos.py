import pandas as pd
import ConexaoPostgreMPL
from psycopg2 import sql




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
                    "set reservado = 'nao', endereco = 'Não Reposto' "
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
def LimparReservaPedido(pedido):
    conn = ConexaoPostgreMPL.conexao()
    # Acessando os pedidos com enderecos reservados
    queue = pd.read_sql('update "Reposicao".pedidossku '
                        " set reservado = 'nao', endereco = 'Não Reposto' "
                        " where codpedido = %s",conn, params=(pedido))

    cursor = conn.cursor()
    cursor.execute(queue,(pedido))
    conn.commit()

    conn.close()
    return pd.DataFrame([{'Mensagem': f'As reservas para o pedido {pedido} foram limpadas'}])

def AtribuirReserva(pedido, natureza):
    conn = ConexaoPostgreMPL.conexao()
    # Passo 1 :  obter os skus do pedido

    queue = pd.read_sql('select produto from "Reposicao".pedidossku '
                        "where necessidade > 0 and codpedido = %s ",conn,params=(pedido,))

    enderecosSku = pd.read_sql(
        ' select  codreduzido as produto, codendereco as codendereco2, "SaldoLiquid"  from "Reposicao"."calculoEndereco"  '
        ' where  natureza = %s  order by "SaldoLiquid" asc', conn, params=(natureza,))

    enderecosSku = pd.merge(enderecosSku,queue, on= 'produto')



    conn.close()
    return enderecosSku




