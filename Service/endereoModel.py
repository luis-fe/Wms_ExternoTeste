import ConexaoPostgreMPL
import pandas as pd

from Service import imprimirEtiquetaModel


def ObeterEnderecos():
    conn = ConexaoPostgreMPL.conexao()
    endercos = pd.read_sql(
        ' select * from "Reposicao"."cadendereco" ce   ', conn)
    return endercos
def CadEndereco (rua, modulo, posicao):
    inserir = 'insert into "Reposicao".cadendereco ("codendereco","rua","modulo","posicao")' \
          ' VALUES (%s,%s,%s,%s);'
    codenderco = rua+"-"+modulo+"-"+posicao

    conn = ConexaoPostgreMPL.conexao()
    cursor = conn.cursor()
    cursor.execute(inserir
                   , (codenderco, rua, modulo, posicao))

    # Obter o número de linhas afetadas
    numero_linhas_afetadas = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    return codenderco

def Deletar_Endereco(Endereco):
    conn = ConexaoPostgreMPL.conexao()
    # Validar se existe Restricao Para excluir o endereo
    Validar = pd.read_sql(
        'select "Endereco" from "Reposicao".tagsreposicao '
        'where "Endereco" = '+"'"+Endereco+"'", conn)
    if not Validar.empty:
         return pd.DataFrame({'Mensagem': [f'Endereco com saldo, nao pode ser excluido'], 'Status':False})
    else:
        delatar = 'delete from "Reposicao".cadendereco ' \
                  'where codendereco = %s '
        # Execute a consulta usando a conexão e o cursor apropriados
        cursor = conn.cursor()
        cursor.execute(delatar, (Endereco,))
        conn.commit()
        return pd.DataFrame({'Mensagem': [f'Endereco excluido!'], 'Status':True})

def EnderecosDisponiveis(natureza, empresa):
    conn = ConexaoPostgreMPL.conexao()

    # 1. Aqui carrego os enderecos do banco de dados, por uma view chamado enderecosReposicao,
    # essa view mostra o saldo de cada endereco cadastrado na plataforma, por empresa e por natureza

    # 1.1 Carregando somente os enderecos com saldo = 0
    relatorioEndereço = pd.read_sql(
        'select codendereco, contagem as saldo from "Reposicao"."enderecosReposicao" '
        'where contagem = 0 and natureza = %s ', conn, params=(natureza,))


    #1.2 Carregando todos os enderecos
    relatorioEndereço2 = pd.read_sql(
        'select codendereco, contagem as saldo from "Reposicao"."enderecosReposicao" where natureza = %s'
        ' ', conn, params=(natureza,))


    # Calculando a Taxa de Ocupacao
    TaxaOcupacao = 1-(relatorioEndereço["codendereco"].size/relatorioEndereço2["codendereco"].size)
    TaxaOcupacao = round(TaxaOcupacao, 2) * 100


    # Calculando o "tamanho"  de cada uma das consultas
    tamanho = relatorioEndereço["codendereco"].size
    tamanho2 = relatorioEndereço2["codendereco"].size
    tamanho2 = "{:,.0f}".format(tamanho2)
    tamanho2 = str(tamanho2)
    tamanho2 = tamanho2.replace(',', '.')
    tamanho = "{:,.0f}".format(tamanho)
    tamanho = str(tamanho)
    tamanho = tamanho.replace(',', '.')

    conn.close()

    # Exibindo as informacoes para o Json
    data = {

        '1- Total de Enderecos Natureza ': tamanho2,
        '2- Total de Enderecos Disponiveis': tamanho,
        '3- Taxa de Oculpaçao dos Enderecos': f'{TaxaOcupacao} %',
        '4- Enderecos disponiveis ': relatorioEndereço.to_dict(orient='records')
    }
    return [data]


# Codigo para incluir enderecos por atacado ou fazer um update por atacado
def ImportEndereco(rua, ruaLimite, modulo, moduloLimite, posicao, posicaoLimite, tipo, codempresa, natureza, imprimir, enderecoReservado = '-'):

    conn = ConexaoPostgreMPL.conexao()
    query = 'insert into "Reposicao".cadendereco (codendereco, rua, modulo, posicao, tipo, codempresa, natureza, endereco_subst) ' \
            'values (%s, %s, %s, %s, %s, %s, %s, %s )'

    update = 'update "Reposicao".cadendereco ' \
             ' set codendereco = %s , rua = %s , modulo = %s , posicao = %s , tipo = %s , codempresa = %s , natureza = %s , endereco_subst = %s ' \
            ' where  codendereco = %s '


    r = int(rua)
    ruaLimite = int(ruaLimite) + 1

    m = int(modulo)
    moduloLimite = int(moduloLimite) +1

    p = int(posicao)
    posicaoLimite = int(posicaoLimite)+1

    while r < ruaLimite:
        ruaAtual = Acres_0(r)
        while m < moduloLimite:
            moduloAtual = Acres_0(m)
            while p < posicaoLimite:
                posicaoAtual = Acres_0(p)
                codendereco = ruaAtual + '-' + moduloAtual +"-"+posicaoAtual
                cursor = conn.cursor()
                select = pd.read_sql('select codendereco from "Reposicao".cadendereco where codendereco = %s ', conn,
                                     params=(codendereco,))
                if imprimir == True:

                    imprimirEtiquetaModel.EtiquetaPrateleira('teste.pdf', codendereco, ruaAtual, moduloAtual, posicaoAtual, natureza)
                    imprimirEtiquetaModel.imprimir_pdf('teste.pdf')
                else:
                    print(f'sem imprimir')
                if select.empty:
                    cursor.execute(query, (codendereco, ruaAtual, moduloAtual, posicaoAtual, tipo, codempresa, natureza,enderecoReservado))
                    conn.commit()
                    cursor.close()
                else:

                    cursor.execute(update, (codendereco, ruaAtual, moduloAtual, posicaoAtual, tipo, codempresa, natureza,enderecoReservado, codendereco))
                    conn.commit()

                    cursor.close()
                    print(f'{codendereco} ja exite')
                p += 1
            p = int(posicao)
            m +=1
        m = int(modulo)
        r += 1



def Acres_0(valor):
    if valor < 10:
        valor = str(valor)
        valor = '0'+valor
        return valor
    else:
        valor = str(valor)
        return valor


def ImportEnderecoDeletar(rua, ruaLimite, modulo, moduloLimite, posicao, posicaoLimite, tipo, codempresa, natureza):

    conn = ConexaoPostgreMPL.conexao()
    query = 'delete from "Reposicao".cadendereco ' \
            'where rua = %s and modulo = %s and posicao = %s'



    r = int(rua)
    ruaLimite = int(ruaLimite) + 1

    m = int(modulo)
    moduloLimite = int(moduloLimite) +1

    p = int(posicao)
    posicaoLimite = int(posicaoLimite)+1

    while r < ruaLimite:
        ruaAtual = Acres_0(r)
        while m < moduloLimite:
            moduloAtual = Acres_0(m)
            while p < posicaoLimite:
                posicaoAtual = Acres_0(p)
                codendereco = ruaAtual + '-' + moduloAtual +"-"+posicaoAtual
                cursor = conn.cursor()
                select = pd.read_sql('select "Endereco" from "Reposicao".tagsreposicao where "Endereco" = %s ', conn,
                                     params=(codendereco,))
                if  select.empty:
                    cursor.execute(query, ( ruaAtual, moduloAtual, posicaoAtual,))
                    conn.commit()
                    cursor.close()
                else:
                    cursor.close()
                    print(f'{codendereco} nao pode ser excluido ')
                p += 1
            p = int(posicao)
            m +=1
        m = int(modulo)
        r += 1


def ObterTipoPrateleira():
    conn = ConexaoPostgreMPL.conexao()
    qurey = pd.read_sql('select tipo from "Reposicao"."configuracaoTipo" ',conn)

    return qurey


