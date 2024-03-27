import BuscasAvancadas
import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd


def TagSegundaQualidade(iniVenda, finalVenda):
    dataIni = iniVenda[6:] + "-" + iniVenda[3:5] + "-" + iniVenda[:2]
    finalVenda = finalVenda[6:] + "-" + finalVenda[3:5] + "-" + finalVenda[:2]

    iniFacMes = iniVenda[3:5]
    IniFacAno = iniVenda[6:]

    if iniFacMes in ['01','02']:
        IniFacAno = int(IniFacAno) -1
        IniFacAno = str(IniFacAno)
        iniFacMes = '11'

    elif iniFacMes in ['03','04','05','06','07','08','09']:
        iniFacMes = int(iniFacMes[1:2])
        iniFacMes = iniFacMes - 1
        iniFacMes = '0' + str(iniFacMes)



    iniProd = IniFacAno + "-" + iniFacMes + "-" + '01'

    conn = ConexaoCSW.Conexao()

    tags = pd.read_sql(BuscasAvancadas.TagsSegundaQualidadePeriodo(dataIni,finalVenda), conn)
    motivos = pd.read_sql(BuscasAvancadas.Motivos(),conn)
    tags['motivo2Qualidade'] = tags['motivo2Qualidade'].astype(str)
    motivos['motivo2Qualidade'] = motivos['motivo2Qualidade'].astype(str)


    tags = pd.merge(tags,motivos,on='motivo2Qualidade', how='left')

    tags['motivo2Qualidade'] =tags['motivo2Qualidade']+tags['nome']+"("+tags['nomeOrigem']+")"
    tags['qtde'] = 1

    PecasBaixadas = pd.read_sql(BuscasAvancadas.OpsBaixadas(dataIni,finalVenda), conn)

    OpsFaccinista = pd.read_sql(BuscasAvancadas.OpsBaixadasFaccionista(iniProd,finalVenda), conn)

    OpsFaccinista1 = OpsFaccinista[OpsFaccinista['codFase'].isin([55, 429,455])]
    OpsFaccinista1.drop(['codFase','numeroOP2','codFac','nomeFase'], axis=1, inplace=True)
    OpsFaccinista1['nomeOrigem']= 'COSTURA'
    OpsFaccinista1.rename(columns={'nomeFaccicionista': 'nomeFaccicionistaCostura'}, inplace=True)


    tags['OPpai'] = tags['numeroOP'].str.split('-').str.get(0)
    tags = pd.merge(tags,OpsFaccinista1,on=['OPpai','nomeOrigem'], how='left')

    OpsFaccinista2 = OpsFaccinista[OpsFaccinista['codFase'].isin([439, 200])]
    OpsFaccinista2.drop(['codFase','numeroOP2','codFac','nomeFase'], axis=1, inplace=True)
    OpsFaccinista2['nomeOrigem']= 'LAVANDERIA'


    ObterSilkPartes = OpsEstampariaFaccionista()
    ObterSilkPartes['nomeOrigem']= 'SILK'



    tags = pd.merge(tags,OpsFaccinista2,on=['OPpai','nomeOrigem'], how='left')
    tags = pd.merge(tags,ObterSilkPartes,on=['OPpai','nomeOrigem'], how='left')


    fasesInternas = pd.read_sql(BuscasAvancadas.MovFase('427, 62',iniProd,finalVenda),conn)
    fasesInternas['OPpai'] = fasesInternas['numeroOP'].str.split('-').str.get(0)
    fasesInternas.drop(['numeroOP','dataMov'], axis=1, inplace=True)
    fasesInternas['nomeInterno'] = 'COSTURA INTERNA MPL'
    fasesInternas['nomeOrigem']= 'COSTURA'

    tags = pd.merge(tags,fasesInternas,on=['OPpai','nomeOrigem'], how='left')

    tags.fillna('-',inplace=True)


    tags['nomeFaccicionistaCostura'] = tags.apply(lambda row: row['nomeInterno'] if row['nomeFaccicionistaCostura'] == '-' else row['nomeFaccicionistaCostura'], axis=1)
    tags['nomeFaccicionista'] = tags.apply(lambda row: row['nomeFaccicionista'] if row['nomeFaccicionistaCostura'] == '-' else row['nomeFaccicionistaCostura'], axis=1)
    tags['nomeFaccicionista'] = tags.apply(lambda row: row['nomeOrigem'] if row['nomeOrigem'] == 'BORDADO' else row['nomeFaccicionistaCostura'], axis=1)
    tags['nomeFaccicionista'] = tags.apply(lambda row: row['nomeOrigem'] if row['nomeOrigem'] == 'CORTE' else   row['nomeFaccicionista'], axis=1)
    tags['nomeFaccicionista'] = tags.apply(lambda row: row['nomeOrigem'] if row['nomeOrigem'] == 'SILK' and row['estamparia'] == '-'  else   row['nomeFaccicionista'], axis=1)
    tags['nomeFaccicionista'] = tags.apply(lambda row: row['estamparia'] if row['nomeOrigem'] == 'SILK' and row['estamparia'] != '-'  else   row['nomeFaccicionista'], axis=1)

    tags.drop('nomeInterno', axis=1, inplace=True)



    TotalPCsBaixadas = PecasBaixadas['qtdMovto'].sum()


    conn.close()

    TotalPecas = tags['qtde'].sum()
    data = {
        '1- Peças com Motivo de 2Qual.': TotalPecas ,
        '2- Total Peças Baixadas periodo': TotalPCsBaixadas,
        '4- Detalhamento ': tags.to_dict(orient='records')
    }
    return pd.DataFrame([data])

# Essa Funcao é utilizada para capturar as tags de motivo de 2 qualidade e agrupalas por motivo + origem
def MotivosAgrupado(iniVenda, finalVenda):
    dataini = iniVenda[6:] + "-" + iniVenda[3:5] + "-" + iniVenda[:2]
    datafim = finalVenda[6:] + "-" + finalVenda[3:5] + "-" + finalVenda[:2]
    conn = ConexaoCSW.Conexao()

    tags = pd.read_sql(BuscasAvancadas.TagsSegundaQualidadePeriodo(dataini, datafim), conn)
    motivos = pd.read_sql(BuscasAvancadas.Motivos(), conn)
    tags['motivo2Qualidade'] = tags['motivo2Qualidade'].astype(str)
    motivos['motivo2Qualidade'] = motivos['motivo2Qualidade'].astype(str)

    tags = pd.merge(tags, motivos, on='motivo2Qualidade', how='left')

    tags['motivo2Qualidade'] =tags['motivo2Qualidade']+"-"+tags['nome']+"("+tags['nomeOrigem']+")"
    tags['qtde'] = 1
    conn.close()

    Agrupamento = tags.groupby('motivo2Qualidade')['qtde'].sum().reset_index()
    Agrupamento = Agrupamento.sort_values(by='qtde', ascending=False,
                        ignore_index=True)  # escolher como deseja classificar



    return Agrupamento

def PorOrigem(iniVenda, finalVenda):

    x = TagSegundaQualidade(iniVenda, finalVenda)
    x = x['4- Detalhamento '][0]
    x = pd.DataFrame(x)
    Agrupamento = x.groupby('nomeFaccicionista')['qtde'].sum().reset_index()
    Agrupamento = Agrupamento.sort_values(by='qtde', ascending=False,
                                          ignore_index=True)  # escolher como deseja classificar
    Agrupamento.rename(columns={'nomeFaccicionista': 'Origem'}, inplace=True)

    return Agrupamento


def OpsEstampariaFaccionista():

    conn = ConexaoPostgreMPL.conexao()
    consulta = pd.read_sql('select "OPpai", "nomeFaccionista" as estamparia from "Reposicao"."Reposicao"."OpsEstamparia" oe ',conn)

    conn.close()
    consulta['OPpai'] = consulta['OPpai'].str.split('-').str.get(0)

    return consulta
