import BuscasAvancadas
import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd


def TagSegundaQualidade(iniVenda, finalVenda):
    iniVenda = iniVenda[6:] + "-" + iniVenda[3:5] + "-" + iniVenda[:2]
    finalVenda = finalVenda[6:] + "-" + finalVenda[3:5] + "-" + finalVenda[:2]
    conn = ConexaoCSW.Conexao()

    tags = pd.read_sql(BuscasAvancadas.TagsSegundaQualidadePeriodo(iniVenda,finalVenda), conn)
    motivos = pd.read_sql(BuscasAvancadas.Motivos(),conn)
    tags['motivo2Qualidade'] = tags['motivo2Qualidade'].astype(str)
    motivos['motivo2Qualidade'] = motivos['motivo2Qualidade'].astype(str)


    tags = pd.merge(tags,motivos,on='motivo2Qualidade', how='left')

    tags['motivo2Qualidade'] =tags['motivo2Qualidade']+tags['nome']+"("+tags['nomeOrigem']+")"
    tags['qtde'] = 1

    PecasBaixadas = pd.read_sql(BuscasAvancadas.OpsBaixadas(iniVenda,finalVenda), conn)

    OpsFaccinista = pd.read_sql(BuscasAvancadas.OpsBaixadasFaccionista(iniVenda,finalVenda), conn)

    OpsFaccinista1 = OpsFaccinista[OpsFaccinista['codFase'].isin([55, 429])]
    OpsFaccinista1.drop(['codFase','numeroOP2'], axis=1, inplace=True)
    OpsFaccinista1['nomeOrigem']= 'COSTURA'

    tags['OPpai'] = tags['numeroOP'].str.split('-').str.get(0)
    tags = pd.merge(tags,OpsFaccinista1,on=['OPpai','nomeOrigem'], how='left')

    OpsFaccinista2 = OpsFaccinista[OpsFaccinista['codFase'].isin([439, 200])]
    OpsFaccinista2.drop(['codFase','numeroOP2'], axis=1, inplace=True)
    OpsFaccinista2['nomeOrigem']= 'LAVANDERIA'

    tags = pd.merge(tags,OpsFaccinista2,on=['OPpai','nomeOrigem'], how='left')



    tags.fillna('-',inplace=True)

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
    iniVenda = iniVenda[6:] + "-" + iniVenda[3:5] + "-" + iniVenda[:2]
    finalVenda = finalVenda[6:] + "-" + finalVenda[3:5] + "-" + finalVenda[:2]
    conn = ConexaoCSW.Conexao()

    tags = pd.read_sql(BuscasAvancadas.TagsSegundaQualidadePeriodo(iniVenda, finalVenda), conn)
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




