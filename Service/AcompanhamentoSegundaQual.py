import BuscasAvancadas
import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd


def TagSegundaQualidade(iniVenda, finalVenda):
    iniVenda = iniVenda[6:] + "-" + iniVenda[3:5] + "-" + iniVenda[:2]
    finalVenda = finalVenda[6:] + "-" + finalVenda[3:5] + "-" + finalVenda[:2]
    conn = ConexaoCSW.Conexao()

    tags = pd.read_sql(BuscasAvancadas.TagsSegundaQualidadePeriodo(iniVenda,finalVenda), conn)

    conn.close()



    return tags


def MotivosAgrupado(iniVenda, finalVenda):

    tags = TagSegundaQualidade(iniVenda,finalVenda)
    #Agrupamento do quantitativo
    tags['qtde'] = 1

    Agrupamento = tags.groupby('motivo2Qualidade')['qtde'].sum().reset_index()

    return Agrupamento




