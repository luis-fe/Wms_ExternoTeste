import pandas as pd

import ConexaoPostgreMPL


def detalhaFila(empresa, natureza):
    detalalhaTags = """
select f.numeroop, codreduzido , descricao, count(codbarrastag) as pcs  from "Reposicao"."Reposicao".filareposicaoportag f 
where f.codempresa = %s and f.codnaturezaatual = %s
group by numeroop, codreduzido, descricao  
order by count(codbarrastag) desc
    """

    caixa = """
    select rq.caixa , rq.numeroop , rq.codreduzido , count(rq.codbarrastag) pc  from "Reposicao"."off".reposicao_qualidade rq 
inner join "Reposicao"."Reposicao".filareposicaoportag f on f.codbarrastag = rq.codbarrastag
group by rq.caixa , rq.numeroop , rq.codreduzido
    """

    with ConexaoPostgreMPL.conexao() as conn:
        detalalhaTags = pd.read_sql(detalalhaTags, conn, params=(empresa, natureza))
        caixapd = pd.read_sql(caixa, conn)

    # Agrupando por 'op' e 'sku' e agregando as colunas 'caixa' e 'qt'
    caixa = caixapd.groupby(['numeroop', 'codreduzido']).apply(
        lambda x: ', '.join(x['caixa'].astype(str) + ':' + x['pc'].astype(str))).reset_index(name='caixas')

    detalalhaTags = pd.merge(detalalhaTags, caixa ,on=['numeroop', 'codreduzido'],how='left')
    detalalhaTags.fillna('-',inplace=True)

    data = { '1.0- Total Peças Fila': f'{detalalhaTags["pcs"].sum()} pcs',
             '1.1- Total Caixas na Fila': f'{caixapd["caixa"].count()}',
        '2.0- Detalhamento': detalalhaTags.to_dict(orient='records')}

    return pd.DataFrame([data])




