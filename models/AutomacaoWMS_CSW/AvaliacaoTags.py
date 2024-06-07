"""
Nessa classe é feito uma avaliacao das Tag's saidas por fora do WMS, operações feitas isoladamente  no ERP e que terao
que ser refletidas no wms para melhor acuracia
"""
import gc
import ConexaoCSW
import pandas as pd
import ConexaoPostgreMPL
from models.AutomacaoWMS_CSW import controle
from psycopg2 import sql

# Funcao para avaliar a "FILA REPOSICAO DE TAGS"
def avaliacaoFila(rotina,datahoraInicio, xemp):

    xemp = "'"+xemp+"'" # CONVERTENDO O CARACTER EMPRESA EM "EMPRESA" PARA USAR O SQL
    with ConexaoCSW.Conexao() as conn:

        with conn.cursor() as cursor_csw:
            # Buscando no ERP as tag na situacao 3 e 8 e na natureza 5, 7 e 54
            sql1 = """select br.codBarrasTag as codbarrastag , 'estoque' as estoque  from Tcr.TagBarrasProduto br 
    WHERE br.codEmpresa in (%s) and br.situacao in (3, 8) and codNaturezaAtual in (5, 7, 54) """%xemp
            cursor_csw.execute(sql1)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            SugestoesAbertos = pd.DataFrame(rows, columns=colunas)

    # Finaliando a etapa 1
    etapa1 = controle.salvarStatus_Etapa1(rotina, 'automacao',datahoraInicio,'etapa csw Tcr.TagBarrasProduto p')


    # Etapa 2 : Buscar no banco do WMS as tags da filareposicaoportag
    postgre_engine = ConexaoPostgreMPL.conexaoEngine()
    tagWmsFila = pd.read_sql('select * from "Reposicao".filareposicaoportag t ', postgre_engine)
    tagWmsFila = pd.merge(tagWmsFila,SugestoesAbertos, on='codbarrastag', how='left')# Realizando o merge entre os 2 bancos
    tagWmsFila = tagWmsFila[tagWmsFila['estoque']!='estoque']


    #Finalizando a etapa 2
    etapa2 = controle.salvarStatus_Etapa2(rotina, 'automacao',etapa1,'etapa merge filatagsWMS+tagsProdutoCSW')

    # Etapa3: Buscar no banco do WMS as tags na tagsreposicao
    tagWmsReposicao = pd.read_sql('select * from "Reposicao".tagsreposicao t', postgre_engine)
    tagWmsReposicao = pd.merge(tagWmsReposicao, SugestoesAbertos, on='codbarrastag', how='left')
    tagWmsReposicao = tagWmsReposicao[tagWmsReposicao['estoque'] != 'estoque']

    #Finalizando a etapa 3
    etapa3 = controle.salvarStatus_Etapa2(rotina, 'automacao', etapa2, 'comparando csw Tcr.TagBarrasProduto x WMS')


    # Etapa 4: Filtrando somente as tags que nao constao mais em estoque
    tamanhoFila =tagWmsFila['codbarrastag'].size
    tamanhoReposicao = tagWmsReposicao['codbarrastag'].size

    # Obter os valores para a cláusula WHERE do DataFrame
    listaFila = tagWmsFila['codbarrastag'].tolist()
    listaReposicao = tagWmsFila['codbarrastag'].tolist()



    #bACKUP DAS TAGS QUE TIVERAM SAIDA FORA DO WMS NA FILA

    if tamanhoFila != 0:
        query1 = sql.SQL('DELETE FROM "Reposicao"."filareposicaoportag" WHERE codbarrastag IN ({})').format(
            sql.SQL(',').join(map(sql.Literal, listaFila))
        )

        # Executar a consulta DELETE
        with ConexaoPostgreMPL.conexao() as conn2:
            cursor = conn2.cursor()
            cursor.execute(query1)
            conn2.commit()

    if tamanhoReposicao != 0:
        queryReposicao =  sql.SQL('DELETE FROM "Reposicao"."tagsreposicao" WHERE codbarrastag IN ({})').format(
                sql.SQL(',').join(map(sql.Literal, listaReposicao))
            )

        # Executar a consulta DELETE
        with ConexaoPostgreMPL.conexao() as conn2:
            cursor = conn2.cursor()
            cursor.execute(queryReposicao)
            conn2.commit()

    else:
        # Finalizando a etapa 4
        print('sem dados')
    etapa4 = controle.salvarStatus_Etapa3(rotina, 'automacao',etapa3,'deletando saidas fora do WMS')
    # Remover grandes DataFrames explicitamente para liberar memória
    del SugestoesAbertos
    del tagWmsFila, tagWmsReposicao
    del listaFila, listaReposicao
    gc.collect()

