import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd
import psycopg2
from psycopg2 import Error
import datetime
import pytz

def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    return hora_str





def ApontarTag(codbarras, Ncaixa, empresa, usuario, natureza, estornar = False):
    conn = ConexaoCSW.Conexao()
    codbarras = "'"+codbarras+"'"

    pesquisa = pd.read_sql('select p.codBarrasTag as codbarrastag , p.codReduzido as codreduzido, p.codEngenharia as engenharia, '
                           ' (select i.nome from cgi.Item i WHERE i.codigo = p.codReduzido) as descricao, situacao, codNaturezaAtual as natureza, codEmpresa as codempresa, '
                           " (select s.corbase||'-'||s.nomecorbase  from tcp.SortimentosProduto s WHERE s.codempresa = 1 and s.codproduto = p.codEngenharia and s.codsortimento = p.codSortimento)" 
                           ' as cor, (select t.descricao from tcp.Tamanhos t WHERE t.codempresa = 1 and t.sequencia = p.seqTamanho ) as tamanho, p.numeroOP as numeroop '
                           ' FROM Tcr.TagBarrasProduto p'
                           ' WHERE p.codBarrasTag = '+codbarras+' and p.codempresa ='+empresa,conn)
    conn.close()
    pesquisa['usuario'] = usuario
    pesquisa['caixa'] = Ncaixa
    pesquisa['natureza'] = natureza
    pesquisa['DataReposicao'] = obterHoraAtual()

    if estornar == False:
        if pesquisa.empty:
            return pd.DataFrame([{'status': False, 'Mensagem': f'tag {codbarras} nao encontrada !'}])
        else:
            pesquisarSituacao = PesquisarTag(codbarras,Ncaixa)

            if pesquisarSituacao == 1:
                InculirDados(pesquisa)
                return pd.DataFrame([{'status':True , 'Mensagem':'tag inserido !'}])
            elif pesquisarSituacao ==2:
                return pd.DataFrame([{'status': False, 'Mensagem': f'tag {codbarras} ja bipado nessa caixa, deseja estornar ?'}])
            else:
                return pd.DataFrame(
                    [{'status': False, 'Mensagem': f'tag {codbarras} ja bipado em outra  caixa de n°{pesquisarSituacao}, deseja estornar ?'}])
    else:
        estorno = EstornarTag(codbarras)
        return estorno




def InculirDados(dataframe):
        conn = ConexaoPostgreMPL.conexao()

        cursor = conn.cursor()  # Crie um cursor para executar a consulta SQL
        insert =  'insert into off.reposicao_qualidade (codbarrastag, codreduzido, engenharia, descricao, natureza, codempresa, cor, tamanho, numeroop, caixa, usuario, "DataReposicao")' \
                  ' values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )'

        values = [(row['codbarrastag'], row['codreduzido'], row['engenharia'],row['descricao']
                   ,row['natureza'],row['codempresa'],row['cor'],row['tamanho'],row['numeroop'], row['caixa'],row['usuario'], row['DataReposicao'] ) for index, row in dataframe.iterrows()]

        cursor.executemany(insert, values)
        conn.commit()  # Faça o commit da transação
        cursor.close()  # Feche o cursor



        conn.close()
def EncontrarEPC(caixa,endereco,empresa):
    # Passo1: Pesquisar em outra funcao um dataframe que retorna a coluna numeroop
    conn = ConexaoPostgreMPL.conexao()
    consulta = pd.read_sql('select rq.codbarrastag , rq.codreduzido, rq.engenharia, rq.descricao, rq.natureza'
                            ', rq.codempresa, rq.cor, rq.tamanho, rq.numeroop, rq.usuario, rq."DataReposicao"  from "off".reposicao_qualidade rq  '
                            "where rq.caixa = %s ", conn, params=(caixa,))
    conn.close()
    caixaNova = consulta.drop_duplicates(subset=['codbarrastag'])
    # Passo2: Retirar do dataframe somente a coluna numeroop
    ops1 = caixaNova[['numeroop']]
    ops1 = ops1.drop_duplicates(subset=['numeroop'])

    # Passo 3: Transformar o dataFrame em lista
    resultado = '({})'.format(', '.join(["'{}'".format(valor) for valor in ops1['numeroop']]))

    conn = ConexaoCSW.Conexao()

    if consulta.empty:

        return pd.DataFrame({'mensagem':['caixa vazia'],'status':False})
    else:
        # Use parâmetro de substituição na consulta SQL
        epc = pd.read_sql('SELECT t.codBarrasTag AS codbarrastag, numeroOP as numeroop, (SELECT epc.id FROM Tcr_Rfid.NumeroSerieTagEPC epc WHERE epc.codTag = t.codBarrasTag) AS epc '
                "FROM tcr.SeqLeituraFase t WHERE t.codempresa = 1 and t.numeroOP IN "+resultado,conn)

        epc = epc.drop_duplicates(subset=['codbarrastag'])

        result = pd.merge(caixaNova,epc,on=('codbarrastag','numeroop'), how='left')
        result.fillna('-', inplace=True)

        #Avaliar se a op da tag foi baixada
        result['mensagem'] = result.apply(lambda row: 'OP em estoque' if row['epc']!='-' else 'OP nao entrou em estoque',axis=1)
        #Filtrar somente as OPs que entraram no estoque, verificar se a prateleira ta livre, inserir na tagsreposicao e excluir da reposicaoqualidade
        inserir = result[result['mensagem']=='OP em estoque']
        inserir = IncrementarCaixa(endereco,inserir)
        ExcluirCaixa(caixa)

        QtdtotalCaixa = result['codbarrastag'].count()
        Qtde_noEstoque = inserir['codbarrastag'].count()


        if QtdtotalCaixa == Qtde_noEstoque:
            return pd.DataFrame([{'status':True,'Mensagem':'Endereco carregado com sucesso!'}])
        else:
            NaoEntrou = result[result['mensagem'] == 'OP nao entrou em estoque']

            NaoEntrou = NaoEntrou[['codbarrastag', 'numeroop']]


            data = {
                'status': False,
                'Mensagem': 'Algumas OPs da caixa nao entraram em estoque',
                'tags Pendentes':NaoEntrou.to_dict(orient='records')
            }
            return pd.DataFrame([data])



def ConsultaCaixa(NCaixa, empresa):
    conn = ConexaoPostgreMPL.conexao()
    consultar = pd.read_sql('select rq.codbarrastag , rq.codreduzido, rq.engenharia, rq.descricao, rq.natureza'
                            ', rq.codempresa, rq.cor, rq.tamanho, rq.numeroop, rq.usuario, rq."DataReposicao"  from "off".reposicao_qualidade rq  '
                            "where rq.caixa = %s ",conn,params=(NCaixa,))
    conn.close()
    print(NCaixa)
    if consultar.empty :
        return pd.DataFrame({'mensagem':['caixa vazia'],'codbarrastag':'','numeroop':'','status':True})
    else:

        consultar, totalOP = Get_quantidadeOP_Sku(consultar,empresa,'1')
        numeroOP = consultar['numeroop'][0]
        codempresa = consultar['codempresa'][0]
        codreduzido = consultar['codreduzido'][0]
        descricao = consultar['descricao'][0]
        cor = consultar['cor'][0]
        eng = consultar['engenharia'][0]
        tam = consultar['tamanho'][0]
        totalPcSku = consultar['total_pcs'][0]
        consultar.drop(['numeroop','codempresa','codreduzido','descricao','cor','engenharia','tamanho',
                        'total_pcs']
                       , axis=1, inplace=True)
        totalbipagemOP, totalbipagemSku = TotalBipado(empresa, numeroOP, codreduzido,True)

        data = {

            '0- mensagem ': 'Caixa Cheia',
            '01- status': False,
            '02- Empresa':codempresa,
            '03- numeroOP': numeroOP,
            '04- totalOP': totalOP,
            '05- totalOPBipado':totalbipagemOP,
            '06- engenharia':eng,
            '07- codreduzido':codreduzido,
            '08- descricao':descricao,
            '09- cor':cor,
            '10- tamanho':tam,
            '11- totalpçsSKU':totalPcSku,
            '12- totalpcsSkuBipado':totalbipagemSku,
            '13- Tags da Caixa ': consultar.to_dict(orient='records')
        }
        return [data]

def OPsAliberar(empresa):
    conn = ConexaoCSW.Conexao()
    consulta = pd.read_sql("SELECT op.numeroOP as numeroop , op.codProduto,"
                           " (select l.descricao FROM  tcl.Lote l WHERE op.codempresa = l.codempresa and op.codLote = l.codlote) as lote, "
                           "(SELECT t.codtipo||'-'||t.nome FROM tcp.TipoOP t WHERE t.empresa = op.codempresa and t.codtipo=op.codTipoOP ) as tipoOP, "
                           "(SELECT e.descricao  FROM tcp.Engenharia e where e.codempresa =1 and e.codEngenharia = op.codProduto) as nome,"
                           " op.codFaseAtual ||'-'||op.nomeFaseAtual as faseAtual,"
                           " (SELECT r.situacao from tco.ControleReceb r WHERE r.codempresa = op.codEmpresa and r.numeroop = op.numeroop) as status_Recebimento  "
                           " FROM tco.OrdemProd op WHERE op.codEmpresa = 1 "
                           'and op.situacao = 3 and op.codFaseAtual in (210, 320, 56, 432, 441 ) '
                           'order by numeroOP desc', conn)
    consulta.fillna('Nao Iniciado', inplace=True)
    consulta['status_Recebimento'] = consulta.apply(lambda row: 'Iniciado' if row['status_Recebimento'] != 'Nao Iniciado' else row['status_Recebimento'] ,
                                       axis=1)

    totalOPs = consulta['status_Recebimento'].count()
    totalOPs_iniciado = consulta[consulta['status_Recebimento'] == 'Iniciado']
    totalOPs_iniciado = totalOPs_iniciado['status_Recebimento'].count()

    conn2 = ConexaoPostgreMPL.conexao()

    consulta2 = pd.read_sql("select numeroop, 'Iniciado' status_Reposicao "
                            'from "off".reposicao_qualidade rq group by numeroop',conn2)
    conn2.close()

    Op_ReposicaoIniciada = consulta2['numeroop'].count()
    consulta = pd.merge(consulta, consulta2 ,on='numeroop', how= 'left')
    consulta.fillna('Nao Iniciado', inplace=True)
    faccionista = InformacoesOPsGarantia(empresa, consulta)
    faccionista_Costura = faccionista.loc[(faccionista['codFase'] == 55) | (faccionista['codFase'] == 429)]
    faccionista_CosturaAcab = faccionista.loc[(faccionista['codFase'] == 195) | (faccionista['codFase'] == 437)]


    faccionista_Costura.drop(['codFase','codFaccio','quantidade'], axis=1, inplace=True)
    faccionista_CosturaAcab.drop(['codFase', 'codFaccio','quantidade'], axis=1, inplace=True)


    faccionista_Costura.rename(columns={'dataEmissao': 'dataCostura'}, inplace=True)
    faccionista_CosturaAcab.rename(columns={'dataEmissao': 'dataAcab','nomeFaccionista':'FacAcabamento'}, inplace=True)

    faccionista_Costura_mei = FaccionistaMei(empresa, consulta)


    consulta = pd.merge(consulta, faccionista_Costura, on='numeroop',how='left' )
    consulta = pd.merge(consulta, faccionista_CosturaAcab, on='numeroop',how='left' )
    consulta = pd.merge(consulta, faccionista_Costura_mei, on='numeroop',how='left' )


    consulta['nomeFaccionista']= consulta.apply(lambda row: row['nomeFaccionistaMei'] if row['nomeFaccionista'] == '-' else row['nomeFaccionista'],
                                      axis=1)

    consulta.fillna('-', inplace=True)
    consulta.drop('nomeFaccionistaMei', axis=1, inplace=True)

    consulta['categoria'] = '-'
    consulta['categoria'] = consulta.apply(lambda row: Categoria('CAMISA', row['nome'], 'CAMISA', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(lambda row: Categoria('TSHIRT', row['nome'], 'CAMISETA', row['categoria']),
                                           axis=1)
    consulta['categoria'] = consulta.apply(lambda row: Categoria('TSHORT', row['nome'], 'CAMISETA', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(lambda row: Categoria('POLO', row['nome'], 'POLO', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(lambda row: Categoria('BABY', row['nome'], 'CAMISETA', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(lambda row: Categoria('REGATA', row['nome'], 'CAMISETA', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(lambda row: Categoria('JUST', row['nome'], 'CAMISETA', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(lambda row: Categoria('BATA', row['nome'], 'CAMISA', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(lambda row: Categoria('JAQUETA', row['nome'], 'JAQUETA', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(lambda row: Categoria('SHORT', row['nome'], 'BOARDSHORT', row['categoria']),
                                     axis=1)
    consulta['categoria'] = consulta.apply(lambda row: Categoria('CARTEIRA', row['nome'], 'CARTEIRA', row['categoria']),
                                     axis=1)
    consulta['categoria'] = consulta.apply(lambda row: Categoria('MEIA', row['nome'], 'MEIA', row['categoria']), axis=1)
    consulta['categoria'] = consulta.apply(lambda row: Categoria('BLAZER', row['nome'], 'JAQUETA', row['categoria']), axis=1)

    quantidade = QuantidadeOP(empresa,consulta,True)
    consulta = pd.merge(consulta, quantidade, on='numeroop', how='left')
    consulta.fillna(0, inplace=True)
    totalPcs = consulta['quantidade'].sum()
    consulta['lote'] = consulta['lote'].str.replace('LOTE INTERNO ', '', regex=True)

    data = {

        '0 - Total de OPs ': totalOPs,
        '01 - Ops RecebimentoIniciado ': totalOPs_iniciado,
        '02 - Ops ReposicaoIniciada ': Op_ReposicaoIniciada,
        '03 - Total Pçs ': totalPcs,
        'Detalhamento das OPs ': consulta.to_dict(orient='records')
    }
    return [data]

def Categoria(contem, valorReferencia, valorNovo, categoria):
    if contem in valorReferencia:
        return valorNovo
    else:
        return categoria




def IncrementarCaixa(endereco, dataframe):

    try:
        conn = ConexaoPostgreMPL.conexao()
        insert = 'insert into "Reposicao".tagsreposicao ("Endereco","codbarrastag","codreduzido",' \
                 '"engenharia","descricao","natureza","codempresa","cor","tamanho","numeroop","usuario", "proveniencia","DataReposicao") values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )'
        dataframe['proveniencia'] = 'Veio da Caixa'

        cursor = conn.cursor()  # Crie um cursor para executar a consulta SQL

        values = [(endereco,row['codbarrastag'], row['codreduzido'], row['engenharia'], row['descricao']
                   , row['natureza'], row['codempresa'], row['cor'], row['tamanho'], row['numeroop'],
                   row['usuario'],row['proveniencia'],row['DataReposicao']) for index, row in dataframe.iterrows()]

        cursor.executemany(insert, values)
        conn.commit()  # Faça o commit da transação
        cursor.close()  # Feche o cursor

        return dataframe


    except psycopg2.Error as e:
        if 'duplicate key value violates unique constraint' in str(e):

            dataframe['mensagem'] = "codbarras ja existe em outra prateleira"

            return dataframe

        else:
            # Lidar com outras exceções que não são relacionadas à chave única
            print("Erro inesperado:", e)
            dataframe['mensagem'] = "Erro inesperado:", e

            return dataframe

    finally:
        if conn:
            conn.close()
            return dataframe


def PesquisarTagCsw(codbarras, empresa):
    conn = ConexaoCSW.Conexao()
    codbarras = "'" + codbarras + "'"

    pesquisa = pd.read_sql(
        'select p.codBarrasTag as codbarrastag , p.codReduzido as codreduzido, p.codEngenharia as engenharia, '
        ' (select i.nome from cgi.Item i WHERE i.codigo = p.codReduzido) as descricao, situacao, codNaturezaAtual as natureza, codEmpresa as codempresa, '
        " (select s.corbase||'-'||s.nomecorbase  from tcp.SortimentosProduto s WHERE s.codempresa = 1 and s.codproduto = p.codEngenharia and s.codsortimento = p.codSortimento)"
        ' as cor, (select t.descricao from tcp.Tamanhos t WHERE t.codempresa = 1 and t.sequencia = p.seqTamanho ) as tamanho, p.numeroOP as numeroop '
        ' FROM Tcr.TagBarrasProduto p'
        ' WHERE p.codBarrasTag = ' + codbarras + ' and p.codempresa =' + empresa, conn)
    conn.close()

    return pesquisa


def PesquisarTag(codbarrastag, caixa):
    conn = ConexaoPostgreMPL.conexao()
    consulta = pd.read_sql('select caixa  from "off".reposicao_qualidade rq'
                           ' where rq.codbarrastag = '+codbarrastag, conn )
    conn.close()

    if consulta.empty:
        return 1
    else:
        caixaAntes = consulta['caixa'][0]

        if  caixaAntes == str(caixa):
            return 2
        else:
         return consulta['caixa'][0]

def EstornarTag(codbarrastag):
    conn = ConexaoPostgreMPL.conexao()
    delete = 'delete from "off".reposicao_qualidade ' \
             'where codbarrastag  = '+codbarrastag
    cursor = conn.cursor()
    cursor.execute(delete,)
    conn.commit()
    cursor.close()
    conn.close()

    return pd.DataFrame([{'status':True,'Mensagem':'tag estornada! '}])

def ExcluirCaixa(Ncaixa):
    delete = 'delete from "off".reposicao_qualidade ' \
             'where caixa  = %s '

    conn = ConexaoPostgreMPL.conexao()
    cursor = conn.cursor()
    cursor.execute(delete,(Ncaixa,))
    conn.commit()
    cursor.close()
    conn.close()

    return pd.DataFrame([{'status':True,'Mensagem':'Caixa Excluida com sucesso! '}])


def PesquisaOPSKU_tag(codbarras):
    conn = ConexaoCSW.Conexao()
    codbarras = "'" + codbarras + "'"

    consulta = pd.read_sql('select p.numeroOP, p.codReduzido, '
                           '(select i.nome from cgi.Item i WHERE i.codigo = p.codReduzido) as descricao, '
                           "(select s.corbase||'-'||s.nomecorbase  from tcp.SortimentosProduto s WHERE s.codempresa = 1 and s.codproduto = p.codEngenharia and s.codsortimento = p.codSortimento) as cor,"
                           " (select t.descricao from tcp.Tamanhos t WHERE t.codempresa = 1 and t.sequencia = p.seqTamanho ) as tamanho"
                           ' FROM tcr.TagBarrasProduto p '
                           'where codBarrasTag = '+codbarras,conn)

    reduzido = consulta['codReduzido'][0]
    numeroOP = consulta['numeroOP'][0]
    descricao = consulta['descricao'][0]
    cor = consulta['cor'][0]
    tamanho = consulta['tamanho'][0]



    codbarras_ = pd.read_sql('select codBarrasTag from tcr.TagBarrasProduto p '
                             'where codReduzido = '+"'"+reduzido+"' and numeroOP = "+"'"+numeroOP+"'",conn)
    totalTags = codbarras_['codBarrasTag'].count()

    conn.close()

    lista_de_dicionarios = codbarras_['codBarrasTag'].tolist()

    data = {
        '1 - Reduzido': f'{reduzido}',
        '2 - numeroOP': f'{numeroOP}',
        '2.1 - totalTags': f'{totalTags}',
        '3 - Descricao': f'{descricao}',
        '4 - cor': f'{cor}',
        '5 - tamanho': f'{tamanho}',
        '6- Detalhamento dos Tags:': lista_de_dicionarios
    }
    return [data]

def CaixasAbertas(empresa):
    conn = ConexaoPostgreMPL.conexao()
    consulta =  pd.read_sql('select distinct rq.caixa, rq.usuario from "Reposicao"."off".reposicao_qualidade rq '
                            'where rq.codempresa  = %s order by caixa ', conn, params=(empresa,))
    Usuarios = pd.read_sql('Select codigo as usuario, nome from "Reposicao".cadusuarios ', conn)
    Usuarios['usuario'] = Usuarios['usuario'].astype(str)
    consulta = pd.merge(consulta, Usuarios, on='usuario', how='left')
    BipadoSKU = pd.read_sql('select numeroop, codreduzido, count(rq.codreduzido) as bipado_sku_OP from "Reposicao"."off".reposicao_qualidade rq  group by codreduzido, numeroop ',conn)
    consulta = pd.merge(consulta, BipadoSKU, on='codreduzido', how='left')


    conn.close()
    return consulta

def CaixasAbertasUsuario(empresa, codusuario):
    conn = ConexaoPostgreMPL.conexao()
    consulta =  pd.read_sql('select  rq.caixa, rq.usuario, numeroop, rq.codreduzido , descricao, count(caixa) N_bipado from "Reposicao"."off".reposicao_qualidade rq '
                            'where rq.codempresa  = %s and rq.usuario = %s  group by rq.caixa, rq.usuario, rq.numeroop, rq.codreduzido, descricao  ',
                            conn, params=(empresa,codusuario,))
    BipadoSKU = pd.read_sql('select numeroop, codreduzido, count(rq.codreduzido) as bipado_sku_OP from "Reposicao"."off".reposicao_qualidade rq  '
                            'group by codreduzido, numeroop ',conn)

    Usuarios = pd.read_sql('Select codigo as usuario, nome from "Reposicao".cadusuarios ', conn)
    Usuarios['usuario'] = Usuarios['usuario'].astype(str)
    consulta = pd.merge(consulta, Usuarios, on='usuario', how='left')
    consulta = pd.merge(consulta, BipadoSKU, on=('codreduzido','numeroop'), how='left')
    consulta, boleano = Get_quantidadeOP_Sku(consulta, empresa, '0')
    consulta['1 - status']= consulta["bipado_sku_op"].astype(str)
    consulta['1 - status']=  consulta['1 - status'] + '/' + consulta["total_pcs"].astype(str)


    conn.close()
    return consulta



def Get_quantidadeOP_Sku(ops1, empresa, numeroop_ ='0'):
    conn = ConexaoCSW.Conexao()

    if not ops1.empty :

        novo = ops1[['numeroop']]
        novo = novo.drop_duplicates(subset=['numeroop'])


        # Passo 3: Transformar o dataFrame em lista
        resultado = '({})'.format(', '.join(["'{}'".format(valor) for valor in novo['numeroop']]))

        get = pd.read_sql('SELECT  '
                          '(SELECT i.codItem  from cgi.Item2 i WHERE i.Empresa = 1 and i.codseqtamanho = op.seqTamanho '
                          "and i.codsortimento = op.codSortimento and '0'||i.coditempai||'-0' = op.codproduto) as codreduzido, "
                          "case WHEN op.qtdePecas1Qualidade is null then op.qtdePecasProgramadas else qtdePecas1Qualidade end total_pcs "
                          "FROM tco.OrdemProdTamanhos op "
                          "WHERE op.codEmpresa = "+ empresa + "and op.numeroOP IN "+resultado,conn)

        if numeroop_ != '0':
            totalGeral = get["total_pcs"].sum()
            totalGeral = int(totalGeral)
            get = pd.merge(ops1, get, on='codreduzido', how='left')

            return get, totalGeral

        else:
            numeroop_ ='0'
            get = pd.merge(ops1, get , on='codreduzido', how='left')


       # get['total_pcs'] = get['total_pcs'].astype(str)
       # get['bipado_sku_OP'] = get['bipado_sku_OP']  + '/' + get['total_pcs']

            return get, False
    else:
        return ops1

def TotalBipado(empresa, numeroop, reduzido, agrupado = True):
    conn = ConexaoPostgreMPL.conexao()
    consulta = pd.read_sql('select numeroop, rq.codreduzido, rq.cor as  "codSortimento", tamanho, count(codreduzido) as "Qtbipado"  from "Reposicao"."off".reposicao_qualidade rq '
                           'where rq.codempresa  = %s and numeroop = %s group by numeroop, codreduzido, cor, tamanho',
                           conn, params=(empresa,numeroop,))
    conn.close()
    totalBipadoOP = consulta['numeroop'].count()


    if agrupado == True:
        totalSku = consulta[consulta['codreduzido'] == reduzido]
        totalSku = totalSku['Qtbipado'].sum()

        return  totalBipadoOP, totalSku
    else:

        consulta['codSortimento'] = consulta['codSortimento'].str.split('-').str[0]
        consulta['sortimentosCores'] = consulta['codSortimento']
        consulta.drop('codSortimento'
                  , axis=1, inplace=True)


        print(consulta)
        return consulta, totalBipadoOP
def InformacoesOPsGarantia(empresa, dataframe):
    novo = dataframe[['numeroop']]
    novo = novo.drop_duplicates(subset=['numeroop'])

    # Passo 3: Transformar o dataFrame em lista
    resultado = '({})'.format(', '.join(["'{}'".format(valor) for valor in novo['numeroop']]))

    conn = ConexaoCSW.Conexao()
    consulta = pd.read_sql("SELECT r.codOP as numeroop , r.dataEmissao , r.codFaccio,  "
                           "(SELECT f.nome from Tcg.Faccionista f WHERE f.Empresa = 1 and f.codFaccionista = r.codFaccio) as nomeFaccionista, "
                           "r.quantidade, "
                           "r.codFase  FROM tct.RetSimbolicoNFERetorno r "
                           "WHERE r.Empresa = 1 and  r.codOP IN "+resultado, conn)
    conn.close()

    return consulta
def FaccionistaMei(empresa, dataframe):
    novo = dataframe[['numeroop']]
    novo = novo.drop_duplicates(subset=['numeroop'])

    # Passo 3: Transformar o dataFrame em lista
    resultado = '({})'.format(', '.join(["'{}'".format(valor) for valor in novo['numeroop']]))
    conn = ConexaoCSW.Conexao()
    consulta = pd.read_sql("SELECT f.numeroOP as numeroop, "
                           "(SELECT f2.nome from Tcg.Faccionista f2 WHERE f2.Empresa = 1 and f2.codFaccionista = f.codFaccionista) as nomeFaccionistaMei "
                           "  FROM tco.MovimentacaoOPFase f "
                           "WHERE f.codEmpresa = 1 and f.codFase in (54, 50, 430, 431) and codFaccionista > 0 "
                           "and f.numeroOP in "+resultado,conn)

    conn.close()

    return consulta


def QuantidadeOP(empresa, dataframe, agrupado = True):
    novo = dataframe[['numeroop']]
    novo = novo.drop_duplicates(subset=['numeroop'])

    # Passo 3: Transformar o dataFrame em lista
    resultado = '({})'.format(', '.join(["'{}'".format(valor) for valor in novo['numeroop']]))
    conn = ConexaoCSW.Conexao()

    consulta = pd.read_sql('SELECT CONVERT(varchar(11), ot.numeroop) as numeroop, ot.codSortimento as codSortimento,  '
                           '(select t.descricao from tcp.Tamanhos  t WHERE t.codempresa = 1 and t.sequencia = ot.seqTamanho) as tamanho , '
                           'ot.qtdePecas1Qualidade as quantidade , qtdePecasProgramadas  from tco.OrdemProdTamanhos ot '
                           'WHERE ot.codEmpresa = 1 and numeroOP in '+resultado,conn)
    conn.close()
    consulta.fillna('-', inplace=True)
    consulta['quantidade'] = consulta.apply(lambda row: row['qtdePecasProgramadas'] if row['quantidade'] == '-' else row['quantidade'] ,
                                      axis=1)


    if agrupado == True:
        consulta = consulta.groupby('numeroop').agg({
            'quantidade': 'sum'
        })

    else:
        consulta['codSortimento'] = consulta['codSortimento'].astype(str)

    return consulta

def DetalhaQuantidadeOP(empresa, numeroop):
    conn = ConexaoCSW.Conexao()
    df = pd.read_sql('SELECT op.numeroOP as numeroop , op.codProduto, op.sortimentosCores, op.codSortimento  FROM tco.OrdemProd op '
                           'where numeroOP ='+" '"+numeroop+"' "+ 'and codempresa ='+" '"+empresa+"'",conn)
    conn.close()
    engenharia = df['codProduto'][0]
    # Dividir as strings e transformar em listas
    novo = pd.DataFrame({'codSortimento': []})
    novo['codSortimento'] = df['codSortimento'].str.split(',')
    novo = novo.explode('codSortimento', ignore_index=True)  # Use ignore_index para redefinir o índice
    # Redefinir o índice e exibir o DataFrame
    novo = novo.reset_index(drop=True)  # drop=True para remover o índice anterior
    novo['indice'] = novo.index

    # Dividir as strings e transformar em listas
    novo2 = pd.DataFrame({'sortimentosCores': []})
    novo2['sortimentosCores'] = df['sortimentosCores'].str.split(',')
    novo2 = novo2.explode('sortimentosCores', ignore_index=True)  # Use ignore_index para redefinir o índice
    # Redefinir o índice e exibir o DataFrame
    novo2 = novo2.reset_index(drop=True)  # drop=True para remover o índice anterior
    novo2['indice'] = novo2.index

    novo = pd.merge(novo,novo2,on='indice')
    novo['numeroop']=numeroop

    quantidade = QuantidadeOP(empresa,novo,False)
    novo = pd.merge(novo,quantidade,on=['numeroop','codSortimento'], how='left')
    novo.drop(['indice','qtdePecasProgramadas','numeroop']
                   , axis=1, inplace=True)

    bipadoSku, totalbipado = TotalBipado(empresa, numeroop, '', False)
    print(novo)
    novo = pd.merge(novo, bipadoSku, on=['sortimentosCores', 'tamanho'], how='left')
    novo['quantidade'] = novo['quantidade'].astype(int)
    novo['quantidade'] = novo['quantidade'].astype(str)
    novo.fillna('0', inplace=True)
    novo['Qtbipado'] = novo['Qtbipado'].astype(str)

    novo['quantidade'] = novo['Qtbipado']+"/"+novo['quantidade']

    novo = novo.groupby(['codSortimento',"sortimentosCores"]).agg({'tamanho': list, 'quantidade': list}).reset_index()


    novo.rename(columns={'codSortimento': '1- codSortimento','sortimentosCores':'2-sortimentosCores'
                         ,'Tamanho':'3-Tam'}, inplace=True)
    novo = novo
    data = {
        '1 -numeroOP': numeroop,
        '2 -CodProduto':engenharia,
        '3- Detalhamento da Grade': novo.to_dict(orient='records')
    }
    return pd.DataFrame([data])





