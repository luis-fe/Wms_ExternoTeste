'''''
        Nesse arquivo .py é realizado a operacao de conectar ao csw para atualizar a fila de tags , 
             disponibilizando -as no WMS. 
'''''
import ConexaoCSW
import pandas as pd
import ConexaoPostgreMPL
from Service.configuracoes import empresaConfigurada
import BuscasAvancadas

# 1 - Funcao para atualizar toda a fila da garantia
def AtualizaFilaGarantia():
    emp = empresaConfigurada.EmpresaEscolhida()

    conn = ConexaoCSW.Conexao() # Abrir conexao com o csw

    consulta = pd.read_sql(BuscasAvancadas.TagDisponiveis(emp),conn)
    conn.close() # encerrar conexao com o csw

    restringe = BuscaResticaoSubstitutos()
    consulta = pd.merge(consulta,restringe,on=['numeroop','cor'],how='left')

    ConexaoPostgreMPL.Funcao_InserirOFF(consulta, consulta.size, 'filareposicaoof', 'replace')

    return pd.DataFrame([{'Status': True, 'mensagem':'tags atualizadas na Garantia com sucesso !'}])


def AtualizacaoFilaOFF_op(op):
    emp = empresaConfigurada.EmpresaEscolhida()
    numeroop = op
    op = "'"+op+"'"

    conn = ConexaoCSW.Conexao() # Abrir conexao com o csw

    consulta = pd.read_sql(
        'SELECT p.codBarrasTag as codbarrastag , p.codReduzido as codreduzido, p.codEngenharia as engenharia,'
        '(select i.nome from cgi.Item i WHERE i.codigo = p.codReduzido) as descricao, situacao, codNaturezaAtual as natureza, codEmpresa as codempresa,'
        " (select s.corbase||'-'||s.nomecorbase  from tcp.SortimentosProduto s WHERE s.codempresa = 1 and s.codproduto = p.codEngenharia and s.codsortimento = p.codSortimento)"
        ' as cor, (select t.descricao from tcp.Tamanhos t WHERE t.codempresa = 1 and t.sequencia = p.seqTamanho ) as tamanho, p.numeroOP as numeroop'
        ' from Tcr.TagBarrasProduto p WHERE p.codEmpresa = ' + emp + ' and '
                                                                     ' p.numeroOP = '+op,
        conn)

    conn.close() # encerrar conexao com o csw

    conn2 = ConexaoPostgreMPL.conexao()

    delete = 'Delete from "Reposicao".off.filareposicaoof where numeroop = %s '

    cursor = conn2.cursor()
    cursor.execute(delete
                   , (
                       numeroop,))
    conn2.commit()
    cursor.close()

    conn2.close()



    restringe = BuscaResticaoSubstitutos()
    consulta = pd.merge(consulta,restringe,on=['numeroop','cor'],how='left')

    ConexaoPostgreMPL.Funcao_InserirOFF(consulta, consulta.size, 'filareposicaoof', 'append')

    return pd.DataFrame([{'Status': True, 'mensagem':f'tags da op {numeroop} atualizadas na Garantia com sucesso !'}])


def BuscaResticaoSubstitutos():
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql('select numeroop , cor, considera  from "Reposicao"."Reposicao"."SubstitutosSkuOP" '
                           "sso where sso.considera = 'sim'",conn)

    conn.close()

    return consulta
