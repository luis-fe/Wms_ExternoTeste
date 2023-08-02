import pytz

import ConexaoPostgreMPL
import pandas as pd
import numpy
import locale
import datetime
import numpy
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    return hora_str

def ProdutividadeRepositores(dataInicial = '0', dataFInal ='0'):
    conn = ConexaoPostgreMPL.conexao()
    if dataInicial == '0' and dataFInal == '0':
        TagReposicao = pd.read_sql('select  "usuario", sum(count), "DataReposicao", "min" , "max"   from '
                   '(select tr."usuario", '
                   'count(tr."codbarrastag"), '
                   'substring("DataReposicao",1,10) as "DataReposicao", '
                   'min("DataReposicao") as min, '
                   'max("DataReposicao") as max '
                   'from "Reposicao"."tagsreposicao" tr '
                   'group by "usuario" , substring("DataReposicao",1,10) '
                   'union '
                   'select tr."usuario_rep" as usuario, '
                   'count(tr."codbarrastag"), '
                   'substring("DataReposicao",1,10) as "DataReposicao", '
                   'min("DataReposicao") as min, '
                   'max("DataReposicao") as max '
                   'from "Reposicao".tags_separacao tr '
                   'group by "usuario_rep" , substring("DataReposicao",1,10)) as grupo '
                   'group by "DataReposicao", "min", "max", "usuario"  ',conn)
        Usuarios = pd.read_sql('Select codigo as usuario, nome from "Reposicao".cadusuarios ',conn)
        Usuarios['usuario'] = Usuarios['usuario'].astype(str)
        TagReposicao = pd.merge(TagReposicao, Usuarios,on='usuario',how='left')
        return TagReposicao
    else:

        TagReposicao = pd.read_sql(
            'SELECT usuario, count(datatempo) as Qtde from "Reposicao"."ProducaoRepositores" '
            'where datareposicao >= %s and datareposicao <= %s '
            'group by usuario ', conn, params=(dataInicial, dataFInal,))
        TagReposicao = TagReposicao.sort_values(by='qtde', ascending=False)
        total = TagReposicao['qtde'].sum()  # Formula do valor Total

        def format_with_separator(value):
            return locale.format('%0.0f', value, grouping=True)
        TagReposicao['qtde'] = TagReposicao['qtde'].apply(format_with_separator)
        total = "{:,.0f}".format(total)

        total = str(total)
        total = total.replace(',','.')






        Usuarios = pd.read_sql('Select codigo as usuario, nome from "Reposicao".cadusuarios ',conn)
        Usuarios['usuario'] = Usuarios['usuario'].astype(str)
        TagReposicao = pd.merge(TagReposicao, Usuarios,on='usuario',how='left')
        TagReposicao['qtde'] = TagReposicao['qtde'].astype(str)
        TagReposicao['qtde'] = TagReposicao['qtde'].str.replace(',', '.')
        TagReposicao.fillna('-', inplace=True)

        record = pd.read_sql('select usuario, datareposicao, count(datatempo) as qtde from "Reposicao"."ProducaoRepositores" '
                             ' group by usuario, datareposicao', conn)
        record = record.sort_values(by='qtde', ascending=False)
        record = pd.merge(record, Usuarios,on='usuario',how='left')
        record1 = record["qtde"][0]
        record1 = "{:,.0f}".format(record1)

        record1 = str(record1)
        record1 = record1.replace(',','.')



        Atualizado = obterHoraAtual()

        data = {
            '0- Atualizado:':f'{Atualizado}',
            '1- Record Repositor': f'{record["nome"][0]}',
            '1.1- Record qtd': f'{record1}',
            '1.2- Record data': f'{record["datareposicao"][0]}',
            '2 Total Periodo':f'{total}',
            '3- Ranking Repositores': TagReposicao.to_dict(orient='records')
        }
        return [data]






def ProdutividadeSeparadores(dataInicial = '0', dataFInal ='0'):
    conn = ConexaoPostgreMPL.conexao()

    if dataInicial == '0' and dataFInal == '0':

     TagReposicao = pd.read_sql('select tr."usuario", '
                   'count(tr."codbarrastag") as Qtde, '
                   'substring("dataseparacao",1,10) as "dataseparacao", '
                   'min("dataseparacao") as min, '
                   'max("dataseparacao") as max '
                   'from "Reposicao".tags_separacao tr '
                   ' where "dataseparacao" is not null '
                   'group by "usuario" , substring("dataseparacao",1,10) ',conn)
     Usuarios = pd.read_sql('Select codigo as usuario, nome from "Reposicao".cadusuarios ', conn)
     Usuarios['usuario'] = Usuarios['usuario'].astype(str)
     TagReposicao = pd.merge(TagReposicao, Usuarios, on='usuario', how='left')

     return TagReposicao
    else:

        TagReposicao = pd.read_sql('SELECT usuario, count(dataseparacao) as Qtde from "Reposicao"."ProducaoSeparadores" '
                                   'where dataseparacao >= %s and dataseparacao <= %s '
                                   'group by usuario ', conn, params=(dataInicial,dataFInal,))
        TagReposicao = TagReposicao.sort_values(by='qtde', ascending=False)
        total = TagReposicao['qtde'].sum()  # Formula do valor Total



        def format_with_separator(value):
            return locale.format('%0.0f', value, grouping=True)
        TagReposicao['qtde'] = TagReposicao['qtde'].apply(format_with_separator)
        total = "{:,.0f}".format(total)
        total = str(total)
        total = total.replace(',','.')

        # Aplicar a função na coluna do DataFrame

        Usuarios = pd.read_sql('Select codigo as usuario, nome from "Reposicao".cadusuarios ',conn)
        Usuarios['usuario'] = Usuarios['usuario'].astype(str)
        ritmo = pd.read_sql('SELECT usuario, ROUND(AVG(ritmo)::numeric, 0) as ritmo '
                            ' FROM "Reposicao"."ProducaoSeparadores"'
                            ' WHERE dataseparacao >= %s AND dataseparacao <= %s AND ritmo < 300 '
                            ' GROUP BY usuario ',conn,params=(dataInicial,dataFInal,))

        TagReposicao = pd.merge(TagReposicao, ritmo,on='usuario',how='left')
        TagReposicao = pd.merge(TagReposicao, Usuarios,on='usuario',how='left')
        TagReposicao.fillna('-', inplace=True)
        record = pd.read_sql('select usuario, dataseparacao, count(datatempo) as qtde from "Reposicao"."ProducaoSeparadores" '
                             ' group by usuario, dataseparacao', conn)
        record = record.sort_values(by='qtde', ascending=False)
        record = pd.merge(record, Usuarios,on='usuario',how='left')
        TagReposicao['qtde'] = TagReposicao['qtde'].astype(str)
        TagReposicao['qtde'] = TagReposicao['qtde'].str.replace(',', '.')
        record1 = record["qtde"][0]
        record1 = "{:,.0f}".format(record1)

        record1 = str(record1)
        record1 = record1.replace(',','.')

        Atualizado = obterHoraAtual()

        data = {
            '0- Atualizado:': f'{Atualizado}',
            '1- Record Repositor': f'{record["nome"][0]}',
            '1.1- Record qtd': f'{record1}',
            '1.2- Record data': f'{record["dataseparacao"][0]}',
            '2 Total Periodo': f'{total}',
            '3- Ranking Repositores': TagReposicao.to_dict(orient='records')
        }
        return [data]


def FilaPorOP(natureza, codempresa):
    conn = ConexaoPostgreMPL.conexao()
    df_OP1 = pd.read_sql(' select "numeroop", "totalop" as qtdpeçs_total, "usuario" as codusuario_atribuido, count("numeroop") as qtdpeçs_arepor  from "Reposicao"."filareposicaoportag" frt ' 
                        ' where "codnaturezaatual" = %s '
                         ' group by "numeroop", "usuario", "totalop"  ',conn,params=(natureza,))

    df_OP_Iniciada =pd.read_sql(' select "numeroop", count("numeroop") as qtdpeçs_reposto  from "Reposicao"."tagsreposicao" frt ' 
                        ' group by "numeroop" ',conn)
    df_OP1 = pd.merge(df_OP1,df_OP_Iniciada,on='numeroop',how='left')
    usuarios = pd.read_sql(
        'select codigo as codusuario_atribuido , nome as nomeusuario_atribuido  from "Reposicao".cadusuarios c ', conn)
    usuarios['codusuario_atribuido'] = usuarios['codusuario_atribuido'].astype(str)
    df_OP1 = pd.merge(df_OP1, usuarios, on='codusuario_atribuido', how='left')
    df_OP1['qtdpeçs_reposto'] = df_OP1['qtdpeçs_reposto'].replace('', numpy.nan).fillna('0')
    df_OP1['qtdpeçs_total'] = df_OP1['qtdpeçs_total'].replace('', numpy.nan).fillna('0')
    df_OP1['qtdpeçs_arepor'] = df_OP1['qtdpeçs_arepor'].replace('', numpy.nan).fillna('0')
    df_OP1['qtdpeçs_total'] = df_OP1['qtdpeçs_total'].astype(int)
    df_OP1 = df_OP1[df_OP1['qtdpeçs_total']>0]
    df_OP1['qtdpeçs_reposto'] = df_OP1['qtdpeçs_reposto'].astype(int)
    df_OP1['% Reposto'] = 1 - numpy.divide(df_OP1['qtdpeçs_arepor'],df_OP1['qtdpeçs_total'])
    df_OP1['% Reposto'] = df_OP1['% Reposto'] * 100
    df_OP1['% Reposto'] = df_OP1['% Reposto'].round(2)
    
    # Clasificando o Dataframe para analise
    df_OP1 = df_OP1.sort_values(by='qtdpeçs_total', ascending=False, ignore_index=True)  # escolher como deseja classificar
    df_OP1["Situacao"] = df_OP1.apply(lambda row: 'Iniciada'  if row['qtdpeçs_reposto'] >0 else 'Nao Iniciada', axis=1)
    df_OP1['codusuario_atribuido'] = df_OP1['codusuario_atribuido'].replace('', numpy.nan).fillna('-')
    df_OP1['nomeusuario_atribuido'] = df_OP1['nomeusuario_atribuido'].replace('', numpy.nan).fillna('-')
    conn.close()
    # Limitar o número de linhas usando head()
    df_OP1 = df_OP1.head(50)  # Retorna as 3 primeiras linhas
    return df_OP1

def detalhaOP(numeroop, empresa, natureza):
    conn = ConexaoPostgreMPL.conexao()
    df_op = pd.read_sql('select "numeroop" , "codbarrastag", "epc", "usuario" as codusuario_atribuido, "Situacao", "codreduzido", codnaturezaatual as natureza '
                   'from "Reposicao"."filareposicaoportag" frt where "numeroop" = ' +"'"+  numeroop +"' and "
                                                                                                     " codnaturezaatual = '"+natureza+"' ", conn)


    df_op['codusuario_atribuido'] = df_op['codusuario_atribuido'].replace('', numpy.nan).fillna('-')
    df_op2 = pd.read_sql(
        'select "numeroop" , "codbarrastag" AS codbarrastag, "epc" as epc, "usuario" as codusuario_atribuido,' +"'reposto'"+ 'as situacao, "codreduzido", natureza '
      'from "Reposicao"."tagsreposicao" frt where "numeroop" = ' + "'" + numeroop + "' and natureza = '"+natureza+"'", conn)
    df_op2.rename(columns={'codreduzido': 'codreduzido', "situacao":'Situacao'}, inplace=True)

    df_op = pd.concat([df_op, df_op2])
    usuarios = pd.read_sql('select codigo as codusuario_atribuido , nome as nomeusuario_atribuido  from "Reposicao".cadusuarios c ',conn)
    usuarios['codusuario_atribuido'] = usuarios['codusuario_atribuido'].astype(str)
    df_op = pd.merge(df_op,usuarios,on='codusuario_atribuido',how='left')
    df_op['codusuario_atribuido'] = df_op['codusuario_atribuido'].replace('', numpy.nan).fillna('-')
    df_op['nomeusuario_atribuido'] = df_op['nomeusuario_atribuido'].replace('', numpy.nan).fillna('-')
    conn.close()
    df_op.to_csv('verficiar.csv')
    if df_op.empty:
        return pd.DataFrame({'Status': [False],'Mensagem':['OP nao Encontrada']})
    else:
        return  df_op

def ConsultaSeExisteAtribuicao(numeroop):
    conn = ConexaoPostgreMPL.conexao()
    cursor = conn.cursor()
    cursor.execute('select "numeroop", "usuario"  from "Reposicao"."filareposicaoportag" frt  '
                   'WHERE "numeroop" = %s AND "usuario" IS NULL', (numeroop,))
    # Obter o número de linhas afetadas
    NumeroLInhas = cursor.rowcount

    cursor.close()
    conn.close()
    return NumeroLInhas

def AtribuiRepositorOP(codigo, numeroop):
    conn = ConexaoPostgreMPL.conexao()
    cursor = conn.cursor()
    cursor.execute('update "Reposicao"."filareposicaoportag" '
                   'set "usuario"  = %s where "numeroop" = %s'
                   , (codigo, numeroop))
    # Obter o número de linhas afetadas
    numero_linhas_afetadas = cursor.rowcount
    conn.commit()
    cursor.close()
def detalhaSku(codreduzido, empresa,natureza):
    conn = ConexaoPostgreMPL.conexao()
    df_op2 = pd.read_sql('select "Endereco", "codreduzido", "descricao", count("codreduzido") as saldo, natureza '
                   'from "Reposicao"."tagsreposicao" frt where "codreduzido" = ' +"'"+  codreduzido +"' and natureza = '"+natureza+"'"
                   ' group by "Endereco", "codreduzido", "descricao", natureza ', conn)
    if df_op2.empty:
        return pd.DataFrame({'Mensagem':[f'O reduzido {codreduzido} ainda nao foi reposto ou esta com as prateleiras vazias ']})
    else:
        return df_op2
    
def detalhaOPxSKU(numeroop, empresa, natureza):
    conn = ConexaoPostgreMPL.conexao()
    df_op = pd.read_sql('select "numeroop", "codreduzido", "engenharia", "cor", "tamanho", "descricao", "codnaturezaatual" as natureza '
                   'from "Reposicao"."filareposicaoportag" frt where "numeroop" = ' +"'"+  numeroop +"' and codnaturezaatual = '"+natureza+"' "
                   'group by "numeroop", "codreduzido","descricao" , "cor","tamanho","engenharia", codnaturezaatual', conn)
    df_op2 = pd.read_sql('select "numeroop", "codreduzido", "engenharia", "cor", "tamanho", "descricao", natureza '
                   'from "Reposicao"."tagsreposicao" frt where "numeroop" = ' +"'"+  numeroop +"' and natureza = '"+natureza+"' "
                   ' group by "numeroop", "codreduzido","descricao" , "cor","tamanho","engenharia" , natureza', conn)
    df_op2.rename(columns={'codreduzido': 'codreduzido', "engenharia": 'engenharia', "cor": "cor", "descricao": "descricao"}, inplace=True)

    df_op = pd.concat([df_op, df_op2])
    df_op.drop_duplicates(subset={'numeroop', 'codreduzido'}, inplace=True)


    conn.close()
    if df_op.empty:
        return pd.DataFrame({'Status': [False],'Mensagem':['OP nao Encontrada']})
    else:
        return  df_op



def ObterNaturezas():
    conn = ConexaoPostgreMPL.conexao()
    qurey = pd.read_sql('select * from "Reposicao".configuracoes ',conn)

    return qurey
