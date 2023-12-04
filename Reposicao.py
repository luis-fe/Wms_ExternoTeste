from datetime import datetime
import pandas as pd
import ConexaoPostgreMPL
import datetime
import pytz
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    return hora_str



# CLASSE COM AS FUNÇOES PARA INTERAGIR COM AS APIS DE ACESSO DA "REPOSICAO"



def PesquisaEnderecosSKU(codreduzido):
    conn = ConexaoPostgreMPL.conexao()
    sku = pd.read_sql(
        ' select distinct  "Endereco" from "Reposicao"."tagsreposicao" '
        ' where "codreduzido"= '+"'"+ codreduzido+"'", conn)

    if sku.empty :
        return False
    else:

        return sku

def PesquisaEndereco(endereco):
    conn = ConexaoPostgreMPL.conexao()
    endercos = pd.read_sql(
        ' select * from "Reposicao"."cadendereco" ce  where "codendereco"= '+"'"+endereco+"'", conn)
    if endercos.empty:
        return pd.DataFrame({'Status': [False], 'Mensagem': [f'endereco {endereco} não cadastrado']})

    else:

        return pd.DataFrame({'Status': [True], 'Mensagem': [f'endereco {endereco} encontrado!']})
    






def Devolver_Inf_Tag(codbarras, padrao=0):
    conn = ConexaoPostgreMPL.conexao()
    cursor = conn.cursor()

    cursor.execute(
        'select "codreduzido", "engenharia", "Situacao", "usuario", "descricao", "cor", "epc", "numeroop" from "Reposicao"."filareposicaoportag" ce '
        'where "codbarrastag" = %s', (codbarras,))
    codreduzido = pd.DataFrame(cursor.fetchall(), columns=['codreduzido', 'engenharia', 'Situacao', 'usuario',  'descricao', 'cor', 'epc','numeroop'])

    cursor.execute(
        'select count("codbarrastag") as situacao, "codreduzido", "engenharia", "numeroop", "descricao", "cor", "epc", "tamanho", "totalop","usuario" from "Reposicao"."tagsreposicao" tr '
        'where "codbarrastag" = %s '
        'group by "usuario","codbarrastag", "codreduzido", "engenharia", "numeroop", "descricao", "cor", "epc", "tamanho", "totalop"', (codbarras,))
    TagApontadas = pd.DataFrame(cursor.fetchall(), columns=['situacao', 'codreduzido', 'Engenharia', 'numeroop', 'descricao', 'cor', 'epc', 'tamanho', 'totalop',"usuario"])

    if not TagApontadas.empty and TagApontadas["situacao"][0] >= 0 and padrao == 0:
        retorno = (
            'Reposto',
            TagApontadas['codreduzido'][0],
            TagApontadas['Engenharia'][0],
            TagApontadas['descricao'][0],
            TagApontadas['cor'][0],
            TagApontadas['epc'][0]
        )
    elif padrao == 1:
        cursor.execute('select "usuario" from "Reposicao"."filareposicaoportag" ce where "numeroop" = %s', (TagApontadas['numeroop'][0],))
        Usuario = pd.DataFrame(cursor.fetchall(), columns=['usuario'])

        if not Usuario.empty:
            retorno = (
                'Reposto',
                TagApontadas['codreduzido'][0],
                TagApontadas['Engenharia'][0],
                TagApontadas['numeroop'][0],
                TagApontadas['descricao'][0],
                TagApontadas['cor'][0],
                TagApontadas['epc'][0],
                TagApontadas['tamanho'][0],
                TagApontadas['totalop'][0],
                Usuario['usuario'][0]
            )
        else:
            retorno = (
                'Reposto',
                TagApontadas['codreduzido'][0],
                TagApontadas['Engenharia'][0],
                TagApontadas['numeroop'][0],
                TagApontadas['descricao'][0],
                TagApontadas['cor'][0],
                TagApontadas['epc'][0],
                TagApontadas['tamanho'][0],
                TagApontadas['totalop'][0],
                "-"
            )
    elif codreduzido.empty:
        retorno = (
            False,
            pd.DataFrame({'Status': [True], 'Mensagem': [f'codbarras {codbarras} encontrado!']}),
            False, False, False, False
        )
    else:
        retorno = (
            codreduzido['codreduzido'][0],
            codreduzido['engenharia'][0],
            codreduzido['usuario'][0],
            codreduzido['descricao'][0],
            codreduzido['cor'][0],
            codreduzido['epc'][0]
        )

    cursor.close()
    conn.close()

    return retorno



def Pesquisa_Estoque(reduzido, endereco):
    conn = ConexaoPostgreMPL.conexao()
    estoque = pd.read_sql(
        'select "Saldo"  from "Reposicao"."estoque" e '
        'where "codreduzido" = '+"'"+reduzido+"'"+' and "endereco"= ' +"'"+endereco+"'", conn)
    conn.close()
    if estoque.empty:
        return False
    else:
        return estoque['Saldo'][0]

def ApontarReposicao(codUsuario, codbarras, endereco, dataHora):
    conn = ConexaoPostgreMPL.conexao()
    #devolvendo o reduzido do codbarras
    reduzido, engenharia, usuario, descricao, cor, epc = Devolver_Inf_Tag(codbarras)
    if reduzido == False:
         return False
    if reduzido == 'Reposto':
        return 'Reposto'
    else:
        #insere os dados da reposicao
        Insert = ' INSERT INTO "Reposicao"."tagsreposicao" ("usuario","codbarrastag","Endereco","DataReposicao","codreduzido","Engenharia","descricao", ' \
                 '"cor", "epc" )' \
                 ' VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);'
        cursor = conn.cursor()
        cursor.execute(Insert
                       , (usuario, codbarras, endereco,dataHora,reduzido,engenharia,descricao,cor,epc))

        # Obter o número de linhas afetadas
        conn.commit()
        cursor.close()

       # print(f'Apontado a {numeroop} , endereco {endereco}, as {dataHora}')

        return  True
def EstornoApontamento(codbarrastag, empresa, natureza):
    conn = ConexaoPostgreMPL.conexao()
    situacao, reduzido, engenharia, numeroop, descricao, cor, epc, tam, totalop, usuario = Devolver_Inf_Tag(codbarrastag, 1)

    consulta = pd.read_sql('select codbarrastag from "Reposicao"."Reposicao".filareposicaoportag  t '
                           'where codbarrastag = %s ',conn,params=(codbarrastag,))
    if not consulta.empty:
        print('localizou')
        deletar = 'delete from "Reposicao"."Reposicao".filareposicaoportag  t' \
                  'where codbarrastag = %s '
        cursor = conn.cursor()
        cursor.execute(deletar,(codbarrastag,))
        conn.commit()
        cursor.close()

    else:
        print('estorno')

    Insert = 'INSERT INTO  "Reposicao"."filareposicaoportag" ("codreduzido", "engenharia","codbarrastag","numeroop", "descricao", "cor", "epc", "tamanho", "totalop", "Situacao",' \
             ' "usuario", codnaturezaatual) ' \
             'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'+"'Reposição não Iniciada'"+',%s, %s);'
    cursor = conn.cursor()
    cursor.execute(Insert
                   , (reduzido, engenharia, codbarrastag, numeroop, descricao, cor, epc, tam, totalop, usuario,natureza))
    # Obter o número de linhas afetadas
    numero_linhas_afetadas = cursor.rowcount
    conn.commit()
    cursor.close()
    # Apagando a Tag estorna
    cursor = conn.cursor()
    delete = 'Delete from "Reposicao"."tagsreposicao"  ' \
             'where "codbarrastag" = %s;'
    cursor.execute(delete
                   , (codbarrastag,))
    conn.commit()
    cursor.close()
    conn.close()

    return True


def RetornoLocalCodBarras(usuario, codbarras, endereco, dataHora, empresa, natureza):
    conn = ConexaoPostgreMPL.conexao()
    cursor = conn.cursor()

    # Verificando se está na Fila de Reposição
    cursor.execute(
        'SELECT "codbarrastag" FROM "Reposicao"."filareposicaoportag" ce '
        'WHERE "codbarrastag" = %s and codnaturezaatual = %s', (codbarras, natureza,)
    )
    fila_reposicao = pd.DataFrame(cursor.fetchall(), columns=['codbarrastag'])

    if not fila_reposicao.empty:
        update = 'UPDATE "Reposicao"."filareposicaoportag" ce ' \
                 'SET usuario = %s ' \
                 'WHERE "codbarrastag" = %s;'
        cursor.execute(update, (usuario, codbarras,))
        conn.commit()
        cursor.close()

        cursor = conn.cursor()

        insert = 'INSERT INTO "Reposicao"."tagsreposicao" ("usuario","codbarrastag", "DataReposicao", "Endereco", natureza, proveniencia) ' \
                 'VALUES (%s, %s, %s, %s, %s, %s)'
        cursor.execute(insert, (usuario,codbarras, dataHora, endereco,natureza, 'veio da fila: reposicaoOP'))
        conn.commit()
        cursor.close()
        retorno = 'A Repor'
    else:
        # Verificando se está na Prateleira
        cursor.execute(
            'SELECT "codbarrastag" FROM "Reposicao"."tagsreposicao" ce '
            'WHERE "codbarrastag" = %s and natureza = %s', (codbarras,natureza,)
        )
        prateleira = pd.DataFrame(cursor.fetchall(), columns=['codbarrastag'])
        if not prateleira.empty:
            retorno = 'Reposto'
        else:
            retorno = False

    cursor.close()
    conn.close()

    return retorno



