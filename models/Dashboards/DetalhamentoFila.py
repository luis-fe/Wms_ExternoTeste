import pandas as pd

import ConexaoPostgreMPL


def detalhaFila():
    detalalhaTags = """
    select f.numeroop, codreduzido , count(codbarrastag) as pcs  from "Reposicao"."Reposicao".filareposicaoportag f 
    group by numeroop, codreduzido  
    order by count(codbarrastag) desc
    """

    with ConexaoPostgreMPL.conexao() as conn:
        detalalhaTags = pd.read_sql(detalalhaTags, conn)

    return detalalhaTags



#print(detalhaFila())