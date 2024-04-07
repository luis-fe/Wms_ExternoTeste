import datetime
import pandas as pd
import ConexaoPostgreMPL
from psycopg2 import errors
import pytz


def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    return hora_str

def PesquisarLinhaPadrao():
    conn = ConexaoPostgreMPL.conexao()

    linhas = pd.read_sql('select * from "Reposicao".off."linhapadrado" c order by "Linha" asc',conn)

    conn.close()
    return linhas
def RetornarNomeLinha(linha):
    conn = ConexaoPostgreMPL.conexao()

    linhas = pd.read_sql('select * from "Reposicao".off."linhapadrado" c where "Linha" = %s ', conn, params=(linha,))
    conn.close()

    if not linhas.empty:
        linhas.fillna('-', inplace=True)
        return pd.DataFrame([{'OperadoresLinha':linhas['operador1'][0]+'/'+linhas['operador2'][0]+'/'+linhas['operador3'][0],'status':'1',
                              'Mensagem':'Ja Possui Linha Cadastrada', 'operador1':linhas['operador1'][0],
                              'operador2':linhas['operador2'][0],'operador3':linhas['operador3'][0]}])
    else:
        return pd.DataFrame([{'Mensagem':'nao existe essa linha', 'status':'2'}])

def CadastrarLinha(nomeLinha, operador1, operador2, operador3):

    consularLinha = RetornarNomeLinha(nomeLinha)
    print(consularLinha)
    if consularLinha['status'][0] == '2':
        conn = ConexaoPostgreMPL.conexao()


        if operador3 != '-':
            insertInto = 'insert into "Reposicao".off."linhapadrado"' \
                     ' ("Linha",operador1, operador2, operador3) values (%s, %s, %s , %s)'
            cursor = conn.cursor()
            try:
                cursor.execute(insertInto, (nomeLinha, operador1, operador2, operador3))
                conn.commit()
                mensagem = pd.DataFrame([{'Mensagem': 'Linha cadastrado com sucesso'}])

            except errors.ForeignKeyViolation as e:
                # Se uma exceção de violação de chave estrangeira ocorrer, imprima a mensagem de erro ou faça o que for necessário
                cursor.close()
                conn.close()
                print(f"Erro inesperado nome operador1: {e}")

                mensagem = pd.DataFrame(
                    [{'Mensagem': f'{e}'}])


            except Exception as e:
                # Lide com outras exceções aqui, se necessário
                cursor.close()
                conn.close()
                print(f"Erro inesperado: {e}")
                mensagem = pd.DataFrame(
                    [{'Mensagem': f'NÃO EXISTE O NOME DO OPERADOR 1: {operador1} no cadastro de usuarios do portal !'}])

            cursor.close()
            conn.close()
            return mensagem


        elif operador3 == '-' and operador2 != '-':
            insertInto = 'insert into "Reposicao".off."linhapadrado" ' \
                         '("Linha", operador1, operador2) values (%s, %s, %s)'
            cursor = conn.cursor()
            try:
                cursor.execute(insertInto, (nomeLinha, operador1, operador2))
                conn.commit()
                mensagem = pd.DataFrame([{'Mensagem': 'Linha cadastrado com sucesso'}])

            except errors.ForeignKeyViolation as e:
                # Se uma exceção de violação de chave estrangeira ocorrer, imprima a mensagem de erro ou faça o que for necessário
                cursor.close()
                conn.close()
                print(f"Erro inesperado nome operador1: {e}")

                mensagem = pd.DataFrame(
                    [{'Mensagem': f'{e}'}])


            except Exception as e:
                # Lide com outras exceções aqui, se necessário
                cursor.close()
                conn.close()
                print(f"Erro inesperado: {e}")
                mensagem = pd.DataFrame(
                    [{'Mensagem': f'NÃO EXISTE O NOME DO OPERADOR 1: {operador1} no cadastro de usuarios do portal !'}])

            cursor.close()
            conn.close()
            return mensagem



        elif operador2 == '-':
            insertInto = 'insert into "Reposicao".off."linhapadrado" ' \
                         '("Linha", operador1) values (%s, %s)'
            cursor = conn.cursor()
            try:
                cursor.execute(insertInto, (nomeLinha, operador1))
                conn.commit()
                mensagem = pd.DataFrame([{'Mensagem': 'Linha cadastrado com sucesso'}])

            except errors.ForeignKeyViolation as e:
                # Se uma exceção de violação de chave estrangeira ocorrer, imprima a mensagem de erro ou faça o que for necessário
                cursor.close()
                conn.close()
                print(f"Erro inesperado nome operador1: {e}")

                mensagem = pd.DataFrame([{'Mensagem': f'NÃO EXISTE O NOME DO OPERADOR 1: {operador1} no cadastro de usuarios do portal !'}])


            except Exception as e:
                # Lide com outras exceções aqui, se necessário
                cursor.close()
                conn.close()
                print(f"Erro inesperado: {e}")
                mensagem = pd.DataFrame(
                    [{'Mensagem': f'NÃO EXISTE O NOME DO OPERADOR 1: {operador1} no cadastro de usuarios do portal !'}])

            cursor.close()
            conn.close()
            return mensagem
    else:
        return consularLinha

def AlterarLinha(nomeLinha, operador1, operador2, operador3):

    obterLinha = RetornarNomeLinha(nomeLinha)
    print(obterLinha)
    conn = ConexaoPostgreMPL.conexao()
    if operador3 == '-' and operador2 == '-' and obterLinha['status'][0] == '1':

        uptade = 'update "Reposicao".off."linhapadrado" ' \
             'set operador1 = %s ' \
             'where "Linha" = %s'
        cursor = conn.cursor()
        cursor.execute(uptade, (operador1, nomeLinha))
        conn.commit()
        cursor.close()
        conn.close()
        mensagem = 'Atualizado com sucesso !'

    elif operador3 == '-' and obterLinha['status'][0] == '1' and operador2 != '-':
        uptade = 'update "Reposicao".off."linhapadrado" ' \
             'set operador1 = %s, operador2 ' \
             'where "Linha" = %s'
        cursor = conn.cursor()
        cursor.execute(uptade, (operador1,operador2, nomeLinha))
        conn.commit()
        cursor.close()
        conn.close()
        mensagem = 'Atualizado com sucesso !'

    elif operador2 !='-' and obterLinha['status'][0] == '1':
        uptade = 'update "Reposicao".off."linhapadrado" ' \
             'set operador1 = %s, operador2 = %s , operador3 = %s ' \
             'where "Linha" = %s'
        cursor = conn.cursor()
        cursor.execute(uptade, (operador1, operador2, operador3, nomeLinha))
        conn.commit()
        cursor.close()
        conn.close()
        mensagem = 'Atualizado com sucesso !'



    else:
        mensagem = 'A Linha informada nao possui cadastro'


    return pd.DataFrame([{'Mensagem':mensagem}])

def ApontarProdutividadeLinha(OP, operador1, operador2 , operador3, linha,  qtd):

    dataHora = obterHoraAtual() # Passo 1 : Capturar a data e hora do apontamento

    conn = ConexaoPostgreMPL.conexao() # Abrir a Conexao com o Banco;

    if qtd == 0: #1.1 - Caso a quantidade nao for informada:

        # Consulta o total da OP em outra tabela do banco chamada: "off".ordemprod , que "captura as quantidades de OPs do CSW via automacao"
        consultar = pd.read_sql('select sum(total_pcs) as qtd from "off".ordemprod where numeroop = %s ',conn,params=(OP,))


        if consultar.empty: # Caso a consulta retorne vazia
            qtd = 0
        else:
            qtd = consultar['qtd'][0] # Caso consiga encontrar valor na consulta

    else:
        qtd=qtd #1.2 - Caso o usuario informar a quantidade, utiliza a quantidade informada ao inves de pesquisar


    # Etapa 2 - Avalia se ja existe a OP na tabela de Produtividade do Banco de dados
    consulta = pd.read_sql('select numeroop from "Reposicao".off.prodlinha where numeroop = %s ',conn,params=(OP,))

    # Etapa 2.1 - Caso nao existir, simplesmente fazemos o input
    if consulta.empty :
        insert = 'insert into "Reposicao".off.prodlinha (numeroop, operador1 , operador2, operador3 ,dataapontamento, qtd, linha) values (%s, %s , %s , %s, %s, %s, %s ) '

        cursor = conn.cursor()

        cursor.execute(insert,(OP, operador1, operador2 , operador3,dataHora,qtd, linha))
        conn.commit()

        cursor.close()

        conn.close()

        return pd.DataFrame([{'Mensagem':'Dados inseridos com sucesso'}])

    # Etapa 2.1 Caso existir a OP , avalia se nessa OP os operadores sao o mesmo que o usuario esta´tentando salvar , medida que evita duplicacao



    else:
        operadores = pd.read_sql('select operador1, operador2, operador3 from "Reposicao".off.prodlinha where numeroop = %s order by dataapontamento desc ', conn,params=(OP,))

        pesquisa1 = operadores['operador1'][0]
        pesquisa2 = operadores['operador2'][0]
        pesquisa3 = operadores['operador3'][0]

        # faz a comparacao:

        if operador1 in [pesquisa1, pesquisa2, pesquisa3] and operador2 in [pesquisa1, pesquisa2, pesquisa3]  :
            print('contem')

            update = 'update  "Reposicao".off.prodlinha set operador1 = %s , operador2 = %s , operador3 = %s ' \
                     'where numeroop = %s and linha = %s '

            cursor = conn.cursor()

            cursor.execute(update, (operador1, operador2, operador3, OP, linha,))
            conn.commit()

            cursor.close()

            conn.close()
            return pd.DataFrame([{'Mensagem': 'Dados inseridos com sucesso'}])



        else:
            print('não contem')

            insert = 'insert into "Reposicao".off.prodlinha (numeroop, operador1 , operador2, operador3 ,dataapontamento, qtd, linha) values (%s, %s , %s , %s, %s, %s, %s ) '

            cursor = conn.cursor()

            cursor.execute(insert, (OP, operador1, operador2, operador3, dataHora, qtd, linha))
            conn.commit()

            cursor.close()

            conn.close()

            return pd.DataFrame([{'Mensagem': 'Dados inseridos com sucesso'}])


# Produtividade Linha
def ProdutividadeOperadorLinha(dataInicio, dataFim):
    dataInicio = dataInicio[6:10]+'-'+ dataInicio[3:5]+ '-'+ dataInicio[0:2]
    dataFim = dataFim[6:10]+'-'+ dataFim[3:5]+ '-'+ dataFim[0:2]

    conn = ConexaoPostgreMPL.conexao()


    consulta = pd.read_sql('select numeroop, "DataReposicao":: date from "Reposicao". "Reposicao".tagsreposicao t where t.numeroop in' 
    ' (select p.numeroop from "Reposicao".off.prodlinha p) '
    'and "DataReposicao" :: date >= '
    "'"+dataInicio+"' and "
                   '"DataReposicao" :: date <='+"'"+dataFim+"'" ,conn)

    nomes1 = pd.read_sql('select p.numeroop, p.operador1 as operador from "Reposicao".off.prodlinha p', conn)
    nomes2 = pd.read_sql('select p.numeroop, p.operador2 as operador from "Reposicao".off.prodlinha p', conn)
    nomes3 = pd.read_sql('select p.numeroop, p.operador3 as operador from "Reposicao".off.prodlinha p', conn)


    consultaNome1 = pd.merge(consulta, nomes1,on='numeroop', how='left' )
    consultaNome2 = pd.merge(consulta, nomes2,on='numeroop', how='left' )
    consultaNome3 = pd.merge(consulta, nomes3,on='numeroop', how='left' )

    consulta = pd.concat([consultaNome1, consultaNome2, consultaNome3])

    conn.close()
    consulta['qtde'] = 1
    consulta = consulta.groupby('operador')['qtde'].sum().reset_index()
    consulta = consulta.sort_values(by='qtde', ascending=False,
                                ignore_index=True)


    return consulta

def OPsProducidasPeriodo(dataInico, dataFim, horaInicio,horaFim):
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql('select operador1, operador2, operador3 ,numeroop, qtd, linha, horario  '
                           'from "off"."ProdutividadeGarantiaEquipe1" pce '
                           'where dataapontamento >= %s and dataapontamento <= %s and horario >= %s and horario <= %s ',
                           conn,
                           params=(dataInico, dataFim, horaInicio, horaFim,))

    conn.close()

    consulta['horario'] = consulta['horario'].astype(str)
    consulta = consulta.sort_values(by=['horario'],
                                          ascending=True)  # escolher como deseja classificar

    return consulta


