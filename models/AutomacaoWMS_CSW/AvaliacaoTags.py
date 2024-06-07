import ConexaoCSW
import pandas as pd
import ConexaoPostgreMPL
from models.AutomacaoWMS_CSW import controle
from psycopg2 import sql

def avaliacaoFila(rotina,datahoraInicio, xemp):
    xemp = "'"+xemp+"'"
    with ConexaoCSW.Conexao() as conn:
        with conn.cursor() as cursor_csw:

            sql1 = """select br.codBarrasTag as codbarrastag , 'estoque' as estoque  from Tcr.TagBarrasProduto br 
    WHERE br.codEmpresa in (%s) and br.situacao in (3, 8) and codNaturezaAtual in (5, 7, 54) """%xemp
            cursor_csw.execute(sql1)
            colunas = [desc[0] for desc in cursor_csw.description]
            # Busca todos os dados
            rows = cursor_csw.fetchall()
            # Cria o DataFrame com as colunas
            SugestoesAbertos = pd.DataFrame(rows, columns=colunas)



    etapa1 = controle.salvarStatus_Etapa1(rotina, 'automacao',datahoraInicio,'etapa csw Tcr.TagBarrasProduto p')

    conn2 = ConexaoPostgreMPL.conexao()

    tagWms = pd.read_sql('select * from "Reposicao".filareposicaoportag t ', conn2)
    tagWms = pd.merge(tagWms,SugestoesAbertos, on='codbarrastag', how='left')
    etapa2 = controle.salvarStatus_Etapa2(rotina, 'automacao',etapa1,'etapa merge filatagsWMS+tagsProdutoCSW')


    tagWms = tagWms[tagWms['estoque']!='estoque']
    tamanho =tagWms['codbarrastag'].size
    # Obter os valores para a cláusula WHERE do DataFrame
    lista = tagWms['codbarrastag'].tolist()
    # Construir a consulta DELETE usando a cláusula WHERE com os valores do DataFrame


    #bACKUP DAS TAGS QUE TIVERAM SAIDA FORA DO WMS NA FILA

    if tamanho != 0:
        query = sql.SQL('DELETE FROM "Reposicao"."filareposicaoportag" WHERE codbarrastag IN ({})').format(
            sql.SQL(',').join(map(sql.Literal, lista))
        )

        # Executar a consulta DELETE
        with conn2.cursor() as cursor:
            cursor.execute(query)
            conn2.commit()
    else:
        print('2.1.1 sem tags para ser eliminadas na Fila Tags Reposicao')
    etapa3 = controle.salvarStatus_Etapa3(rotina, 'automacao',etapa2,'deletando saidas fora do WMS')