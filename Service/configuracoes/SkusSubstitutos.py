import ConexaoPostgreMPL
import pandas as pd


def SubstitutosPorOP(filtro = ''):
   if  filtro == '':
        conn = ConexaoPostgreMPL.conexao()

        consultar = pd.read_sql('Select categoria as "1-categoria", numeroop as "2-numeroOP", codproduto as "3-codProduto", cor as "4-cor", databaixa_req as "5-databaixa", '
                                '"coodigoPrincipal" as "6-codigoPrinc", '
                                'nomecompontente as "7-nomePrinc",'
                                '"coodigoSubs" as "8-codigoSub",'
                                'nomesub as "9-nomeSubst", aplicacao as "10-aplicacao", considera from "Reposicao"."SubstitutosSkuOP" ', conn)

        conn.close()

        consultar.fillna('-',inplace=True)

        # Fazer a ordenacao
        consultar = consultar.sort_values(by=['considera','5-databaixa'], ascending=False)  # escolher como deseja classificar
        consultar = consultar.drop_duplicates()

        return consultar
   else:
       conn = ConexaoPostgreMPL.conexao()

       consultar = pd.read_sql('Select categoria as "1-categoria", numeroop as "2-numeroOP", codproduto as "3-codProduto", cor as "4-cor", databaixa_req as "5-databaixa", '
                               '"coodigoPrincipal" as "6-codigoPrinc", '
                               'nomecompontente as "7-nomePrinc",'
                               '"coodigoSubs" as "8-codigoSub",'
                               'nomesub as "9-nomeSubst",aplicacao as "10-aplicacao",  considera from "Reposicao"."SubstitutosSkuOP" where categoria = %s ', conn, params=(filtro,))

       conn.close()

       # Fazer a ordenacao
       consultar = consultar.sort_values(by=['considera', '5-databaixa'],
                                         ascending=False)  # escolher como deseja classificar
       consultar.fillna('-', inplace=True)

       consultar = consultar.drop_duplicates()

       return consultar

def ObterCategorias():
    conn = ConexaoPostgreMPL.conexao()

    consultar = pd.read_sql('Select distinct categoria from "Reposicao"."SubstitutosSkuOP" ', conn)

    conn.close()


    return consultar

def UpdetaConsidera(arrayOP , arraycor, arraydesconsidera):
    conn = ConexaoPostgreMPL.conexao()

    indice = 0
    for i in range(len(arrayOP)):
        indice = 1 + indice
        op = arrayOP[i]
        cor = arraycor[i]
        considera = arraydesconsidera[i]

        update = 'update "Reposicao"."SubstitutosSkuOP" set considera = %s where numeroop = %s and "cor" = %s'

        cursor = conn.cursor()
        cursor.execute(update,(considera, op, cor,))
        conn.commit()
        cursor.close()


    conn.close()
    return pd.DataFrame([{'Mensagem':'Salvo com sucesso'}])


def PesquisaEnderecoSubstitutoVazio():
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql('select c.codendereco , saldo from "Reposicao"."Reposicao".enderecoporsku sle '
                        'right join "Reposicao"."Reposicao".cadendereco c on c.codendereco = sle.codendereco '
                            "where c.endereco_subst = 'sim' and saldo is null and pre_reserva is null", conn)

    conn.close()

    consulta['saldo'] = 0

    return consulta


def SugerirEnderecoRestrito(numeroop,SKU ):

    validador = PesquisarSKUOP(numeroop, SKU)

    if validador['status'][0] == 'False':

        return pd.DataFrame([{'mensagem':'sem restricao de Substituto segue fluxo !'}])

    else:


        sugestaoEndereco = PesquisaEnderecoSubstitutoVazio()

    if sugestaoEndereco.empty:

        return pd.DataFrame([{'mensagem':'Atencao! OP selecionada  como SUBSTUICAO. ',
                            'EnderecoRepor':'Solicitar para Supervisor endereco de SKU DE SUBSTITUICAO '}])
    else:
        endereco = sugestaoEndereco['codendereco'][0]


        #Atualizar endereco com a informacao
        PreReservarEndereco(endereco, validador['status'][0])

        return pd.DataFrame([{'mensagem':'Atencao! OP selecionada  como SKU DE SUBSTUICAO. ',
                            'EnderecoRepor':f'Repor no endereco {endereco}'}])


def PesquisarSKUOP(numeroop,SKU):
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql('select resticao from "off".filareposicaoof x '
                           'where numeroop = %s and codreduzido = %s ', conn ,params=(numeroop,SKU))

    conn.close()

    if consulta.empty:

        return pd.DataFrame([{'status':'False'}])
    else:

        resticao = consulta['resticao'][0]

        if resticao != '-':
            return pd.DataFrame([{'status': resticao}])

        else:
            return pd.DataFrame([{'status': 'False'}])


def PreReservarEndereco(endereco, restricao):
    conn = ConexaoPostgreMPL.conexao()

    update = 'update "Reposicao"."Reposicao".cadendereco ' \
             'set pre_reserva = %s ' \
             'where codendereco = %s '

    cursor = conn.cursor()
    cursor.execute(update,(restricao, endereco))
    conn.commit()

    cursor.close()

    conn.close()

def EnderecoPropostoSubtituicao(restricao):
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql('select codendereco from "Reposicao"."Reposicao".cadendereco '
                           'where pre_reserva = %s', conn,params=(restricao))
    conn.close()

    return consulta['codendereco'][0]

def LimprandoPréReserva(endereco):
    conn = ConexaoPostgreMPL.conexao()

    update = 'update "Reposicao"."Reposicao".cadendereco ' \
             'set reservado = pre_reserva , pre_reserva = null  ' \
             'where codendereco = %s '

    cursor = conn.cursor()
    cursor.execute(update,(endereco,))
    conn.commit()

    cursor.close()

    conn.close()


def AtualizarReservadoLiberados():
    conn = ConexaoPostgreMPL.conexao()


    update = 'update  "Reposicao"."Reposicao".cadendereco c'\
' set reservado = null '\
' where c.codendereco in ('\
' select c.codendereco from "Reposicao"."Reposicao".enderecoporsku sle' \
' right join "Reposicao"."Reposicao".cadendereco c on c.codendereco = sle.codendereco'\
" where c.endereco_subst = %s and saldo is null and reservado is not null)"

    cursor = conn.cursor()
    cursor.execute(update, ('sim'))
    conn.commit()

    cursor.close()

    conn.close()
