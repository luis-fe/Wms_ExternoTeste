import gc
import jaydebeapi
from colorama import Fore
import ConexaoCSW
import pandas as pd
import ConexaoPostgreMPL
from models.AutomacaoWMS_CSW import controle

def SKU_CSW():
    consulta = """SELECT i.codItem as codSKU, i.codItemPai, i.codSortimento , i.codCor, i.codSeqTamanho,
(select i1.nome from cgi.Item i1 WHERE i1.codigo = i.codItem) as nomeSKU FROM cgi.Item2 i
WHERE i.Empresa = 1 and i.codSortimento > 0 and (i.codItemPai like '1%' or i.codItemPai like '2%'or i.codItemPai like '3%' or i.codItemPai like '55%' )
"""

    return consulta
def CadastroSKU(rotina, datainico):
    conn = None
    cursor_csw = None
    try:
        with ConexaoCSW.Conexao() as conn:
            with conn.cursor() as cursor_csw:
                cursor_csw.execute(SKU_CSW())
                colunas = [desc[0] for desc in cursor_csw.description]
                # Busca todos os dados
                rows = cursor_csw.fetchall()
                # Cria o DataFrame com as colunas
                sku = pd.DataFrame(rows, columns=colunas)
                sku.info()
                del rows
                gc.collect()
    except jaydebeapi.Error as e:
        print(Fore.RED + f'Erro de JayDeBeAPI: {e}')
    except Exception as e:
        print(Fore.RED + f'Erro durante a execução da consulta: {e}')

    finally:
        try:
            if cursor_csw:
                cursor_csw.close()
        except jaydebeapi.Error as e:
            print(Fore.RED + f'Erro ao fechar cursor: {e}')
        try:
            if conn:
                conn.close()
        except jaydebeapi.Error as e:
            print(Fore.RED + f'Erro ao fechar conexão: {e}')

    etapa1 = controle.salvarStatus_Etapa1(rotina, 'automacao', datainico, 'from cgi.item i')

    # Etapa Verificando e excluindo relacionamento na tabela
    ExcluindoRelacoes()
    etapa2 = controle.salvarStatus_Etapa2(rotina, 'automacao', etapa1, 'Verifica se existe chavePrimaria p/excluir')
    ConexaoPostgreMPL.Funcao_InserirPCP(sku, sku['codSKU'].size, 'SKU', 'replace')

    etapa3 = controle.salvarStatus_Etapa3(rotina, 'automacao', etapa2, 'inserir no Postgre o cadastroSKU')
    del sku
    gc.collect()

    ## Criando a chave primaria escolhendo a coluna codSKU
    chave = """ALTER TABLE pcp."SKU" ADD CONSTRAINT sku_pk PRIMARY KEY ("codSKU")"""
    conn2 = ConexaoPostgreMPL.conexaoPCP()  # Abrindo a conexao com o Postgre
    cursor = conn2.cursor()  # Abrindo o cursor com o Postgre
    cursor.execute(chave)
    conn2.commit()  # Atualizando a chave
    cursor.close()  # Fechando o cursor com o Postgre
    conn2.close()  # Fechando a Conexao com o POSTGRE
    etapa4 = controle.salvarStatus_Etapa4(rotina, 'automacao', etapa3, 'Criar Chave primaria na tabela codSKU')
    del etapa4
    del conn2
    del cursor, cursor_csw, conn
    gc.collect()
    print('\n OBJETOS NA MEMORIA')



def ExcluindoRelacoes():

    try:
        chave = """ALTER TABLE pcp."pedidosItemgrade" DROP CONSTRAINT pedidositemgrade_fk """

        conn2 = ConexaoPostgreMPL.conexaoPCP() # Abrindo a conexao com o Postgre

        cursor = conn2.cursor()# Abrindo o cursor com o Postgre
        cursor.execute(chave)
        conn2.commit() # Atualizando a chave
        cursor.close()# Fechando o cursor com o Postgre

        conn2.close() #Fechando a Conexao com o POSTGRE

    except:
        print('sem relacao de chave estrangeira')
