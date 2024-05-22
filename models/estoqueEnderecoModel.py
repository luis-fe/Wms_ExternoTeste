import ConexaoPostgreMPL
import pandas as pd


def Estoque_endereco(endereco,empresa, natureza):
    conn = ConexaoPostgreMPL.conexao()
    consultaSql = 'select count(codbarrastag) as "Saldo" from "Reposicao"."tagsreposicao" e ' \
                  'where "Endereco" = %s and natureza = %s ' \
                  'group by "Endereco"'
    cursor = conn.cursor()
    cursor.execute(consultaSql, (endereco,natureza,))
    resultado = cursor.fetchall()
    cursor.close()
    if not resultado:
        return 0
    else:
        return resultado[0][0]

def SituacaoEndereco(endereco, empresa, natureza):
    conn = ConexaoPostgreMPL.conexao()
    select = 'select * from "Reposicao"."cadendereco" ce ' \
             'where codendereco = %s ' \
             'and natureza = %s ' #ocultado ata o Sergio arrumar no front essa opcao !! na reposicao
    resultado = pd.read_sql(select, conn, params=(endereco,natureza,))
    if  resultado.empty:
        conn.close()
        print(f'3 endereco {endereco} selecionado')
        return pd.DataFrame({'Status Endereco': [False], 'Mensagem': [f'endereco {endereco} nao existe na natureza {natureza}!']})
    else:
        saldo = Estoque_endereco(endereco, empresa, natureza)
        if saldo == 0:
            conn.close()
            print(f'2 endereco {endereco} selecionado')
            return pd.DataFrame({'Status Endereco': [True], 'Mensagem': [f'endereco {endereco} existe!'],
                                 'Status do Saldo': ['Vazio']})
        else:
            print(f'1 endereco {endereco} selecionado')
            skus = pd.read_sql('select  count(codbarrastag) as "Saldo Geral"  from "Reposicao".tagsreposicao e '
                                    'where "Endereco"= %s and natureza = %s ',conn,params=(endereco,natureza,))
            SaldoSku_Usuario = pd.read_sql('select  "Endereco", "codreduzido" as codreduzido , "usuario", count(codbarrastag) as "Saldo Sku"  from "Reposicao".tagsreposicao e '
                                    'where "Endereco"= %s and natureza = %s'
                                    'group by "Endereco", "codreduzido" , "usuario", natureza ', conn, params=(endereco,natureza,))
            usuarios = pd.read_sql(
                'select codigo as "usuario" , nome  from "Reposicao".cadusuarios c ',
                conn)
            usuarios['usuario'] = usuarios['usuario'].astype(str)
            SaldoSku_Usuario = pd.merge(SaldoSku_Usuario, usuarios, on='usuario', how='left')
            SaldoSku_Usuario['usuario'] = SaldoSku_Usuario["usuario"] + '-'+SaldoSku_Usuario["nome"]
            SaldoSku_Usuario.drop('nome', axis=1, inplace=True)
            SaldoSku_Usuario.drop('Endereco', axis=1, inplace=True)
            SaldoSku_Usuario.fillna('-', inplace=True)


            skus['enderco'] = endereco
            skus['Status Endereco'] = True
            skus['Mensagem'] = f'Endereço {endereco} existe!'
            skus['Status do Saldo']='Cheio'
            SaldoGeral = skus['Saldo Geral'][0]

            detalhatag = pd.read_sql(
                'select codbarrastag, "usuario", "codreduzido" as codreduzido, "DataReposicao"  from "Reposicao".tagsreposicao t '
                'where "Endereco"= %s and natureza = %s' ,conn, params=(endereco, natureza,))
            detalhatag = pd.merge(detalhatag, usuarios, on='usuario', how='left')
            conn.close()

            data = {
                '1- Endereço': f'{endereco} ',
                '2- Status Endereco':
                    True,
                '3- Mensagem': f'Endereço {endereco} existe!',
                '4- Status do Saldo': 'Cheio',
                '5- Saldo Geral': f'{SaldoGeral}',
                '6- Detalhamento Reduzidos': SaldoSku_Usuario.to_dict(orient='records'),
                '7- Detalhamento nivel tag':detalhatag.to_dict(orient='records')
            }

            return [data]

def EndereçoTag(codbarra, empresa, natureza):
    conn = ConexaoPostgreMPL.conexao()
    pesquisa = pd.read_sql(
        ' select t."Endereco"  from "Reposicao".tagsreposicao t  '
        'where codbarrastag = ' + "'" + codbarra + "' and natureza = '"+natureza+"'", conn)

    pesquisa['Situacao'] = 'Reposto'
    pesquisa2 = pd.read_sql(
        " select '-' as Endereco  from " + '"Reposicao".filareposicaoportag f   '
                                           'where codbarrastag = ' + "'" + codbarra + "' and codnaturezaatual = '"+natureza+"'", conn)

    pesquisa2['Situacao'] = 'na fila'
    pesquisa3 = pd.read_sql(
        ' select distinct f."Endereco"  from "Reposicao".tagsreposicao_inventario f   '
        'where codbarrastag = ' + "'" + codbarra + "'", conn)

    pesquisa3['Situacao'] = f'em inventario'
    conn.close()

    if not pesquisa2.empty:
        return 'Na fila ', pesquisa2

    if not pesquisa3.empty:
        return 'em inventario ', pesquisa3
    if not pesquisa.empty:

        return pesquisa['Endereco'][0], pesquisa
    else:
        return False, pd.DataFrame({'Mensagem': [f'tag nao encontrada na natureza {natureza}']})