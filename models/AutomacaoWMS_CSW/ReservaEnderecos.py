import gc
from models.AutomacaoWMS_CSW import controle
import pandas as pd
import ConexaoPostgreMPL
from psycopg2 import sql
import datetime
import pytz
import ConexaoCSW
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str



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
    queue = 'update "Reposicao".pedidossku '\
                        " set reservado = 'nao', endereco = 'Não Reposto' "\
                        " where codpedido = %s"

    cursor = conn.cursor()
    cursor.execute(queue,(pedido,))
    conn.commit()

    conn.close()
    return pd.DataFrame([{'Mensagem': f'As reservas para o pedido {pedido} foram limpadas'}])

def AtribuirReserva(pedido, natureza):
    conn = ConexaoPostgreMPL.conexao()
    total = 0  # Para Totalizar o numer de atualizcoes
    inseridosDuplos = 0

    # Passo 1 :  obter os skus do pedido
    LimparReservaPedido(pedido)

    queue = pd.read_sql('select codpedido, produto, necessidade from "Reposicao".pedidossku '
                        "where necessidade > 0 and codpedido = %s ",conn,params=(pedido,))

    enderecosSku = pd.read_sql(
        ' select  codreduzido as produto, codendereco as codendereco2, "SaldoLiquid"  from "Reposicao"."calculoEndereco"  '
        ' where  natureza = %s  order by "SaldoLiquid" asc', conn, params=(natureza,))

    enderecosSku = pd.merge(enderecosSku,queue, on= 'produto')

    # Calculando a necessidade de cada endereco

    enderecosSku['repeticoessku'] = enderecosSku.groupby('produto').cumcount() + 1
    for i in range(3):
        pedidoskuIteracao = enderecosSku[enderecosSku['repeticoessku'] == (i + 1)]

        tamanho = pedidoskuIteracao['produto'].size
        pedidoskuIteracao = pedidoskuIteracao.reset_index(drop=False)

        for i in range(tamanho):
            necessidade = pedidoskuIteracao['necessidade'][i]
            saldoliq = pedidoskuIteracao['SaldoLiquid'][i]
            endereco = pedidoskuIteracao['codendereco2'][i]
            produto = pedidoskuIteracao['produto'][i]
            pedido = pedidoskuIteracao['codpedido'][i]

            if necessidade<= saldoliq:
                    update = 'UPDATE "Reposicao".pedidossku '\
                             'SET endereco = %s , reservado = %s'\
                             'WHERE codpedido = %s AND produto = %s and reservado = %s '

                    # Filtrar e atualizar os valores "a" para "aa"
                    pedidoskuIteracao.loc[(pedidoskuIteracao['codendereco2'] == endereco) &
                                          (pedidoskuIteracao['produto'] == produto), 'SaldoLiquid'] \
                        = pedidoskuIteracao['SaldoLiquid'][i] - pedidoskuIteracao['necessidade'][i]

                    cursor = conn.cursor()

                    # Executar a atualização na tabela "Reposicao.pedidossku"
                    cursor.execute(update,
                                   (endereco,'sim',
                                    pedido, produto,'nao')
                                    )

                    # Confirmar as alterações
                    conn.commit()

                    total = total + 1

            elif saldoliq >0 and necessidade > saldoliq:
                qtde_sugerida = pd.read_sql('select qtdesugerida from "Reposicao".pedidossku '
                                            "where reservado = 'nao' and codpedido = "+"'"+pedido+"' and produto ="
                                                                                                  " '"+produto+"'",conn)
                if not qtde_sugerida.empty:
                    qtde_sugerida = qtde_sugerida['qtdesugerida'][0]
                    qtde_sugerida2 = qtde_sugerida - saldoliq

                    insert = 'insert into "Reposicao".pedidossku (codpedido, datahora, endereco, necessidade, produto, qtdepecasconf, ' \
                             'qtdesugerida, reservado, status, valorunitarioliq) ' \
                             'select codpedido, datahora, %s, %s, produto, qtdepecasconf, ' \
                             '%s, %s, status, valorunitarioliq from "Reposicao".pedidossku ' \
                             'WHERE codpedido = %s AND produto = %s and reservado = %s ' \
                             ' limit 1;'
                    cursor = conn.cursor()

                    # Executar a atualização na tabela "Reposicao.pedidossku"
                    cursor.execute(insert,
                                   ('Não Reposto', qtde_sugerida2, qtde_sugerida2, 'nao',
                                    pedido, produto, 'nao')
                                   )

                    # Confirmar as alterações
                    conn.commit()

                    update = 'UPDATE "Reposicao".pedidossku ' \
                             'SET endereco = %s , qtdesugerida = %s , reservado = %s, necessidade = %s ' \
                             'WHERE codpedido = %s AND produto = %s and reservado = %s and qtdesugerida = %s'

                    # Filtrar e atualizar os valores "a" para "aa"
                    pedidoskuIteracao.loc[(pedidoskuIteracao['codendereco2'] == endereco) &
                                          (pedidoskuIteracao['produto'] == produto), 'SaldoLiquid'] = 0
                    cursor = conn.cursor()

                    # Executar a atualização na tabela "Reposicao.pedidossku"
                    cursor.execute(update,
                                   (endereco, saldoliq, 'sim', saldoliq,
                                    pedido, produto, 'nao', qtde_sugerida)
                                   )

                    # Confirmar as alterações
                    conn.commit()

                    inseridosDuplos = 1 + inseridosDuplos

            else:
                encerra = i
    datahora = obterHoraAtual()
    return pd.DataFrame([{'Mensagem': f'foram reservados  {total} pçs e incrementado {inseridosDuplos}'}])

# FUNCAO UTILIZADA PARA RESERVAR OS ENDERECOS DE ACORDO COM OS PEDIDOS DISPONIVEIS
def ReservaPedidosNaoRepostos(empresa, natureza, consideraSobra, ordem,repeticao,modelo):

    conn = ConexaoPostgreMPL.conexao() #Abrindo A Conexao com o CSW
    for i in range(repeticao): #Repeticao é o numero de ciclos para aplicar a distribuicao
    # ----------------------------------------------------------------------------------------------------------------------------------
        # 1 Filtra oa reserva de sku somente para os skus em pedidos:
        skuEmPedios = """
        select distinct produto from "Reposicao".pedidossku 
        where necessidade > 0 and reservado = 'nao'
        """
        queue2 = pd.read_sql(skuEmPedios,conn)

        # Etappa 2 Verifica se no Modelo de Calculo é para considerar uma distribuicao Normal x somente SUBSTITUOS X somente NAO-SUBSTITUTOS
    #-------------------------------------------------------------------------------------------------------------------------------------------
        if modelo == 'Substitutos' and ordem == 'asc':
            calculoEnderecos = """
            select  codreduzido as produto, codendereco as codendereco2, "SaldoLiquid"  from "Reposicao"."calculoEndereco" c
            where  natureza = %s and c.codendereco  in (select "Endereco" from "Reposicao"."Reposicao".tagsreposicao t where resticao  like %s ) and "SaldoLiquid" >0  
            order by "SaldoLiquid" asc
        """
            enderecosSku = pd.read_sql(calculoEnderecos, conn, params=(natureza,'%||%'))


        elif modelo == 'Substitutos' and ordem == 'desc':
            calculoEnderecos = """
            select  codreduzido as produto, codendereco as codendereco2, "SaldoLiquid"  from "Reposicao"."calculoEndereco" c
            where  natureza = %s and c.codendereco  in (select "Endereco" from "Reposicao"."Reposicao".tagsreposicao t where resticao  like %s ) and "SaldoLiquid" >0  
            order by "SaldoLiquid" desc
        """
            enderecosSku = pd.read_sql(calculoEnderecos, conn, params=(natureza,'%||%'))

        elif modelo =='Retirar Substitutos' and ordem == 'asc':
            calculoEnderecos = """select  codreduzido as produto, codendereco as codendereco2, "SaldoLiquid"  from "Reposicao"."calculoEndereco" c
            where  natureza = %s and c.codendereco  not in (select "Endereco" from "Reposicao"."Reposicao".tagsreposicao t where resticao  like %s) and "SaldoLiquid" >0  
            order by "SaldoLiquid" asc
        """
            enderecosSku = pd.read_sql(calculoEnderecos, conn, params=(natureza,'%||%'))


        elif modelo =='Retirar Substitutos' and ordem == 'desc':
            calculoEnderecos = """select  codreduzido as produto, codendereco as codendereco2, "SaldoLiquid"  from "Reposicao"."calculoEndereco" c
            where  natureza = %s and c.codendereco  not in (select "Endereco" from "Reposicao"."Reposicao".tagsreposicao t where resticao  like %s ) and "SaldoLiquid" >0  
            order by "SaldoLiquid" desc
        """
            enderecosSku = pd.read_sql(calculoEnderecos, conn, params=(natureza,'%||%'))

        elif ordem == 'asc':
            calculoEnderecos = """select  codreduzido as produto, codendereco as codendereco2, "SaldoLiquid"  from "Reposicao"."calculoEndereco"
            where  natureza = %s and "SaldoLiquid" >0  order by "SaldoLiquid" asc
        """
            enderecosSku = pd.read_sql(calculoEnderecos, conn, params=(natureza))

        else:
            calculoEnderecos = """
            select  codreduzido as produto, codendereco as codendereco2, "SaldoLiquid"  from "Reposicao"."calculoEndereco"
            where  natureza = %s and "SaldoLiquid" >0  order by "SaldoLiquid" desc
        """
            enderecosSku = pd.read_sql(calculoEnderecos, conn, params=(natureza))
    #--------------------------------------------------------------------------------------------------------------------------------------------------


    #Etapa 3: Conferir quantas vezes o sku aparece no dataframe, visto que podemos ter + de 1 endereco para o mesmo sku
    # ----------------------------------------------------------------------------------------------------------------------------------
        enderecosSku['repeticoesEndereco'] = enderecosSku['codendereco2'].map(enderecosSku['codendereco2'].value_counts())
        if consideraSobra == False:
            enderecosSku = enderecosSku[enderecosSku['repeticoesEndereco'] == 1]
        else:
            print('segue o baile')
        #Nessa etapa 3 o consideraSobra é aplicado somente para o caso em que  queremos distribuir o saldo daques enderecos que se repetem mais de 1 vez
    #--------------------------------------------------------------------------------------------------------------------------------------

    # Etapa 4: Realizamos um Merge para filtra somente os produtos e enderecos que queremos manter no calculo, e depois contamos quantas veses o produtoxendereco se repete
    #no dataFrame
        enderecosSku = pd.merge(enderecosSku,queue2, on= 'produto')
        enderecosSku['repeticoessku'] = enderecosSku.groupby('produto').cumcount() + 1

    # ----------------------------------------------------------------------------------------------------------------------------------
        # Consulto os skus que serao reservados, que sao aqueles com necessidade maior que 0 na tabela PEDIDOSSKU do banco de dados
        queue = pd.read_sql('select codpedido, produto, necessidade from "Reposicao".pedidossku '
                            "where necessidade > 0 and reservado = 'nao' ",conn)
    # ----------------------------------------------------------------------------------------------------------------------------------
        # Calculando a necessidade de cada endereco



        pedidoskuIteracao = enderecosSku[enderecosSku['repeticoessku'] == (i)]
        pedidoskuIteracao = pd.merge(queue, pedidoskuIteracao, on='produto')
        print(pedidoskuIteracao)
        pedidoskuIteracao['reptproduto'] = pedidoskuIteracao.groupby('produto').cumcount() + 1
        pedidoskuIteracao['NecessidadeAcumulada'] = pedidoskuIteracao.groupby('produto')['necessidade'].cumsum()
        pedidoskuIteracao['reserva'] = pedidoskuIteracao['SaldoLiquid']  - pedidoskuIteracao['NecessidadeAcumulada']

        pedidoskuIteracao2 = pedidoskuIteracao[pedidoskuIteracao['reserva'] >= 0]


        tamanho = pedidoskuIteracao2['codpedido'].size

        pedidoskuIteracao2 = pedidoskuIteracao2.reset_index(drop=True)

        cursor = conn.cursor()
        for n in range(tamanho):
                    endereco = pedidoskuIteracao2['codendereco2'][n]
                    produto =pedidoskuIteracao2['produto'][n]
                    codpedido =pedidoskuIteracao2['codpedido'][n]
                    reservado = 'sim'

                    update = 'update "Reposicao".pedidossku ' \
                             'set reservado= %s , endereco = %s ' \
                             'where codpedido = %s and produto = %s '


                    # Executar a atualização na tabela "Reposicao.pedidossku"
                    cursor.execute(update,
                                   (reservado, endereco, codpedido, produto)
                                   )

                    # Confirmar as alterações
                    conn.commit()


        cursor.close()
    conn.close()

def avaliacaoReposicao(rotina, datainicio, emp):
    try:
        # Conectar usando SQLAlchemy
        postgre_engine = ConexaoPostgreMPL.conexaoEngine()
        # Conexão CSW via jaydebeapi
        with ConexaoCSW.Conexao() as conn_csw:
            with conn_csw.cursor() as cursor_csw:
                query_csw = (
                    "select br.codBarrasTag as codbarrastag, 'estoque' as estoque "
                    f"from Tcr.TagBarrasProduto br WHERE br.codEmpresa = {emp} "
                    "and br.situacao in (3, 8) and codNaturezaAtual in (5, 7, 54)"
                )
                cursor_csw.execute(query_csw)
                rows = cursor_csw.fetchall()

                SugestoesAbertos = pd.DataFrame(rows, columns=['codbarrastag', 'estoque'])

        etapa1 = controle.salvarStatus_Etapa1(rotina, 'automacao', datainicio, 'etapa csw Tcr.TagBarrasProduto br')

        # Obter tags do WMS
        tagWms = pd.read_sql('select * from "Reposicao".tagsreposicao t', postgre_engine)
        tagWms = pd.merge(tagWms, SugestoesAbertos, on='codbarrastag', how='left')
        tagWms = tagWms[tagWms['estoque'] != 'estoque']
        tamanho = tagWms['codbarrastag'].size
        etapa2 = controle.salvarStatus_Etapa2(rotina, 'automacao', etapa1, 'comparando csw Tcr.TagBarrasProduto x WMS')

        # Obter os valores para a cláusula WHERE do DataFrame
        lista = tagWms['codbarrastag'].tolist()
        if lista:
            query = sql.SQL('DELETE FROM "Reposicao"."tagsreposicao" WHERE codbarrastag IN ({})').format(
                sql.SQL(',').join(map(sql.Literal, lista))
            )
            # Executar a consulta DELETE
            with ConexaoPostgreMPL.conexao() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                conn.commit()

            etapa3 = controle.salvarStatus_Etapa3(rotina, 'automacao', etapa2, f'excluindo tags fora WMS {tamanho} tags')
        else:
            etapa3 = controle.salvarStatus_Etapa3(rotina, 'automacao', etapa2, 'excluindo tags fora WMS 0 tags')

        # Remover grandes DataFrames explicitamente para liberar memória
        del SugestoesAbertos
        del tagWms
        del lista
        gc.collect()

    except Exception as e:
        print(f"Erro durante a avaliação de reposição: {e}")
        # Adicionar lógica adicional de tratamento de erros, se necessário

    finally:
        # Forçar coleta de lixo no final para garantir a liberação de memória
        gc.collect()

