import BuscasAvancadas
import ConexaoCSW
import ConexaoPostgreMPL



def TagSegundaQualidade(iniVenda, finalVenda):
    iniVenda = iniVenda[6:] + "-" + iniVenda[3:5] + "-" + iniVenda[:2]
    finalVenda = finalVenda[6:] + "-" + finalVenda[3:5] + "-" + finalVenda[:2]
    conn = ConexaoCSW.Conexao()

    tags = BuscasAvancadas.TagsSegundaQualidadePeriodo(iniVenda,finalVenda)

    conn.close()

    return tags
