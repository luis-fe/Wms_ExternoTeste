import gc
import pandas as pd
import ConexaoPostgreMPL

def BuscarServicos():

        conn = ConexaoPostgreMPL.conexaoEngine()
        consulta = pd.read_sql("""
        Select * from "Reposicao"."configuracoes"."Automacao" order by id
        """,conn)
        ultima = """
                select rotina as "Servico", max(atualizado) as ultima_atualizacao from 
        (select rotina , (substring(crc.fim, 7 ,4)||'-'||substring(crc.fim, 4 ,2)||'-'||substring(crc.fim, 0 ,3)||' '||crc.fim :: time)as atualizado from "Reposicao".configuracoes.controle_requisicao_csw crc 
        where crc.ip_origem = 'automacao')as dp 
        group by dp.rotina 
                """
        ultima = pd.read_sql(ultima,conn)
        consulta = pd.merge(consulta,ultima,on='Servico',how='left')


        gc.collect()
        consulta.fillna('-', inplace=True)
        return consulta