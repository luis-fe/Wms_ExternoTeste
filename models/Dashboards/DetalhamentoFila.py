import pandas as pd

import ConexaoCSW
import ConexaoPostgreMPL


def detalhaFila(empresa, natureza):
    detalalhaTags = """
select f.numeroop, codreduzido , descricao, count(codbarrastag) as pcs  from "Reposicao"."Reposicao".filareposicaoportag f 
where f.codempresa = %s and f.codnaturezaatual = %s and status_fila is null
group by numeroop, codreduzido, descricao  
order by count(codbarrastag) desc
    """

    caixa = """
    select rq.caixa , rq.numeroop , rq.codreduzido , count(rq.codbarrastag) pc  from "Reposicao"."off".reposicao_qualidade rq 
inner join "Reposicao"."Reposicao".filareposicaoportag f on f.codbarrastag = rq.codbarrastag
group by rq.caixa , rq.numeroop , rq.codreduzido
    """

    sqlCsw = """
    SELECT top 100000 numeroOP as numeroop ,dataFim, L.descricao as descOP FROM tco.OrdemProd o 
inner join tcl.Lote L on L.codempresa = o.codempresa and L.codlote = o.codlote
WHERE o.codEmpresa = 1 
and o.situacao = 2 
order by numeroOP desc
    """

    with ConexaoCSW.Conexao() as conn:
        with conn.cursor() as cursor_csw:
            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute(sqlCsw)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            dadosOP = pd.DataFrame(rows, columns=colunas)
            del rows

    with ConexaoPostgreMPL.conexao() as conn:
        detalalhaTags = pd.read_sql(detalalhaTags, conn, params=(empresa, natureza))
        caixapd = pd.read_sql(caixa, conn)

    # Agrupando por 'op' e 'sku' e agregando as colunas 'caixa' e 'qt'
    caixa = caixapd.groupby(['numeroop', 'codreduzido']).apply(
        lambda x: ', '.join(x['caixa'].astype(str) + ':' + x['pc'].astype(str))).reset_index(name='caixas')

    detalalhaTags = pd.merge(detalalhaTags, caixa ,on=['numeroop', 'codreduzido'],how='left')

    detalalhaTags = pd.merge(detalalhaTags,dadosOP,on='numeroop', how='left')
    detalalhaTags.fillna('-',inplace=True)

    data = { '1.0- Total Peças Fila': f'{detalalhaTags["pcs"].sum()} pcs',
             '1.1- Total Caixas na Fila': f'{caixapd["caixa"].count()}',
        '2.0- Detalhamento': detalalhaTags.to_dict(orient='records')}

    return pd.DataFrame([data])

def DetalharCaixa(numeroCaixa):

    DetalharCaixa = """
    select rq.usuario, c.nome  , rq."DataReposicao",  rq.codbarrastag , f.epc ,rq.numeroop , rq.codreduzido from "Reposicao"."off".reposicao_qualidade rq 
left join "Reposicao"."Reposicao".filareposicaoportag f on rq.codbarrastag = f.codbarrastag 
inner join "Reposicao"."Reposicao".cadusuarios c on c.codigo = rq.usuario ::int
where rq.caixa = %s 
    """

    with ConexaoPostgreMPL.conexao() as conn:
        DetalharCaixa = pd.read_sql(DetalharCaixa, conn, params=(numeroCaixa,))

    return DetalharCaixa

def TagsFilaConferencia():
    sql = """
    select codbarrastag, engenharia , codreduzido , epc , dataseparacao, codpedido, "Endereco" as "EnderecoOrigem" , c.nome as usuario_Separou
from "Reposicao"."Reposicao".tags_separacao ts 
inner join "Reposicao"."Reposicao".cadusuarios c on c.codigo = ts.usuario :: int
where ts.codbarrastag in (select codbarrastag  from "Reposicao"."Reposicao".filareposicaoportag f)

    """
    with ConexaoPostgreMPL.conexao() as conn:
        detalalhaTags = pd.read_sql(sql, conn)


    pedidos = detalalhaTags.groupby('codpedido')
    pedidos = pedidos['codpedido'].size

    data = { '1.0- Total Peças': f'{detalalhaTags["codbarrastag"].sum()} pcs',
             '1.1- Total Pedidos na Fila': f'{pedidos}',
        '2.0- Detalhamento': detalalhaTags.to_dict(orient='records')}

    return pd.DataFrame([data])


