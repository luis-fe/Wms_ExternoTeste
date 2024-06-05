import pandas as pd

import ConexaoPostgreMPL


def detalhaFila():
    detalalhaTags = """
    select f.numeroop, codreduzido , descricao, count(codbarrastag) as pcs  from "Reposicao"."Reposicao".filareposicaoportag f 
    group by numeroop, codreduzido, descricao  
    order by count(codbarrastag) desc
    """

    caixa = """
    select rq.caixa , rq.numeroop , rq.codreduzido , count(rq.codbarrastag) pc  from "Reposicao"."off".reposicao_qualidade rq 
inner join "Reposicao"."Reposicao".filareposicaoportag f on f.codbarrastag = rq.codbarrastag
group by rq.caixa , rq.numeroop , rq.codreduzido
    """

    with ConexaoPostgreMPL.conexao() as conn:
        detalalhaTags = pd.read_sql(detalalhaTags, conn)
        caixa = pd.read_sql(caixa, conn)

    return detalalhaTags



#print(detalhaFila())