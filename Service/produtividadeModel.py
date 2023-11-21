import pytz
import ConexaoPostgreMPL
import pandas as pd
import locale
import datetime

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
        TagReposicao2 = pd.read_sql(
            'SELECT usuario, count(datatempo) as Qtde from "Reposicao"."ProducaoRepositores2" '
            'where datareposicao >= %s and datareposicao <= %s '
            'group by usuario ', conn, params=(dataInicial, dataFInal,))

        TagReposicao3 = pd.read_sql(
            'SELECT usuario, count(datatempo) as Qtde from "Reposicao"."ProducaoRepositores3" '
            'where datareposicao >= %s and datareposicao <= %s '
            'group by usuario ', conn, params=(dataInicial, dataFInal,))

        # Unir os DataFrames usando pd.concat()
        TagReposicao = pd.concat([TagReposicao, TagReposicao2, TagReposicao3])
        TagReposicao = TagReposicao.groupby('usuario')['qtde'].sum().reset_index()

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

        TagReposicao = pd.read_sql('SELECT usuario, count(dataseparacao) as Qtde, count(distinct codpedido) as "Qtd Pedido" from "Reposicao"."ProducaoSeparadores" '
                                   'where dataseparacao >= %s and dataseparacao <= %s '
                                   'group by usuario ', conn, params=(dataInicial,dataFInal,))
        TagReposicao = TagReposicao.sort_values(by='qtde', ascending=False)
        total = TagReposicao['qtde'].sum()  # Formula do valor Total
        TagReposicao['Méd pçs/ped.'] = TagReposicao['qtde']/TagReposicao['Qtd Pedido']
        TagReposicao['Méd pçs/ped.'] = TagReposicao['Méd pçs/ped.'].astype(int) + 1



        def format_with_separator(value):
            return locale.format('%0.0f', value, grouping=True)
        TagReposicao['qtde'] = TagReposicao['qtde'].apply(format_with_separator)
        total = "{:,.0f}".format(total)
        total = str(total)
        total = total.replace(',','.')

        # Aplicar a função na coluna do DataFrame

        Usuarios = pd.read_sql('Select codigo as usuario, nome from "Reposicao".cadusuarios ',conn)
        Usuarios['usuario'] = Usuarios['usuario'].astype(str)


        ritmo2 = pd.read_sql('select count_tempo as ritmo, dia, usuario, data_intervalo_min as intervalo from "Reposicao"."Reposicao".ritmosseparador r '
                             ' WHERE r.dia >= %s and r.dia <= %s ',conn,params=(dataInicial,dataFInal,))
        ritmo2['acum'] = ritmo2.groupby(['usuario','dia']).cumcount() + 1
        ritmo2['acum'] = ritmo2['acum'] * (15*60)
        ritmo2['ritmo'] = ritmo2.groupby(['usuario','dia'])['ritmo'].cumsum()
        ritmo2['ritmo'] = ritmo2['acum']/ritmo2['ritmo']
        ritmo2 = ritmo2.groupby(['usuario','dia']).tail(1)


        ritmo2 = ritmo2.groupby('usuario').agg({
            'ritmo': 'mean'})
        ritmo2['ritmo'] = ritmo2['ritmo'].round(2)


        TagReposicao = pd.merge(TagReposicao, ritmo2,on='usuario',how='left')
        TagReposicao = pd.merge(TagReposicao, Usuarios,on='usuario',how='left')
        TagReposicao.fillna('-', inplace=True)
        record = pd.read_sql('select usuario, dataseparacao, count(datatempo) as qtde, '
                             'COUNT(DISTINCT codpedido) as mediapedidos '
                             'from "Reposicao"."ProducaoSeparadores"'
                             " where tempo < '17:31:00'"
                             ' group by usuario, dataseparacao', conn)


        record = record.sort_values(by='qtde', ascending=False)
        record = pd.merge(record, Usuarios,on='usuario',how='left')
        record['mediaPedidos2'] = record["qtde"]/record["mediapedidos"]
        TagReposicao['qtde'] = TagReposicao['qtde'].astype(str)
        TagReposicao['qtde'] = TagReposicao['qtde'].str.replace(',', '.')
        record1 = record["qtde"][0]
        record1 = "{:,.0f}".format(record1)
        pecasPedido = int(record["mediaPedidos2"][0])
        pecasPedido = round(pecasPedido, 0)
        record1 = str(record1)
        record1 = record1.replace(',','.')

        Atualizado = obterHoraAtual()

        data = {
            '0- Atualizado:': f'{Atualizado}',
            '1- Record Repositor': f'{record["nome"][0]}',
            '1.1- Record qtd': f'{record1}',
            '1.2- Record data': f'{record["dataseparacao"][0]}',
            '1.3 Media Pçs Pedido': f'{pecasPedido}',
            '2 Total Periodo': f'{total}',
            '3- Ranking Repositores': TagReposicao.to_dict(orient='records')
        }
        return [data]
def RelatorioSeparacao(empresa, dataInicial, dataFInal, usuario = ''):
    if usuario == '':
        conn = ConexaoPostgreMPL.conexao()
        TagReposicao = pd.read_sql(
            'SELECT usuario, dataseparacao, count(dataseparacao) as Qtde, count(distinct codpedido) as "Qtd Pedido" from "Reposicao"."ProducaoSeparadores" '
            'where dataseparacao >= %s and dataseparacao <= %s '
            'group by usuario, dataseparacao ', conn, params=(dataInicial, dataFInal,))

        TagReposicao = TagReposicao.sort_values(by='qtde', ascending=False)

        TagReposicao['Méd pçs/ped.'] = TagReposicao['qtde'] / TagReposicao['Qtd Pedido']
        TagReposicao['Méd pçs/ped.'] = TagReposicao['Méd pçs/ped.'].astype(int) + 1


        def format_with_separator(value):
            return locale.format('%0.0f', value, grouping=True)

        TagReposicao['qtde'] = TagReposicao['qtde'].apply(format_with_separator)


        # Aplicar a função na coluna do DataFrame

        Usuarios = pd.read_sql('Select codigo as usuario, nome from "Reposicao".cadusuarios ', conn)
        Usuarios['usuario'] = Usuarios['usuario'].astype(str)
        TagReposicao = pd.merge(TagReposicao, Usuarios, on='usuario', how='left')

        ritmo2 = pd.read_sql(
            'select count_tempo as ritmo, dia, usuario, data_intervalo_min as intervalo from "Reposicao"."Reposicao".ritmosseparador r '
            ' WHERE r.dia >= %s and r.dia <= %s ', conn, params=(dataInicial, dataFInal,))
        ritmo2['acum'] = ritmo2.groupby(['usuario', 'dia']).cumcount() + 1
        ritmo2['acum'] = ritmo2['acum'] * (15 * 60)
        ritmo2['ritmo'] = ritmo2.groupby(['usuario', 'dia'])['ritmo'].cumsum()
        ritmo2['ritmo'] = ritmo2['acum'] / ritmo2['ritmo']
        ritmo2 = ritmo2.groupby(['usuario', 'dia']).tail(1)

        ritmo2 = ritmo2.groupby('usuario').agg({
            'ritmo': 'mean'})
        ritmo2['ritmo'] = ritmo2['ritmo'].round(2)
        ritmo2['dia'] = ritmo2['dia'].astype(str)
        ritmo2['dataseparacao'] = ritmo2['dia']


        TagReposicao = pd.merge(TagReposicao, ritmo2, on=('usuario','dataseparacao'), how='left')

        conn.close()

        #
        TagReposicao.to_csv('ProdSepa.csv')
        return TagReposicao
    else:
        TagReposicao = pd.read_csv('ProdSepa.csv')
        TagReposicao['usuario'] = TagReposicao['usuario'] .astype(str)
        TagReposicao = TagReposicao[TagReposicao['usuario']==str(usuario)]


        return TagReposicao