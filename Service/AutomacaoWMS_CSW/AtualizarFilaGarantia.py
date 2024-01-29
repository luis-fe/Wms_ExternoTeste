import ConexaoCSW
import pandas as pd

import ConexaoPostgreMPL
from Service.configuracoes import empresaConfigurada



def AtualizaFilaGarantia():
    emp = empresaConfigurada.EmpresaEscolhida()

    conn = ConexaoCSW.Conexao()

    consulta = pd.read_sql(
        'SELECT p.codBarrasTag as codbarrastag , p.codReduzido as codreduzido, p.codEngenharia as engenharia,'
        '(select i.nome from cgi.Item i WHERE i.codigo = p.codReduzido) as descricao, situacao, codNaturezaAtual as natureza, codEmpresa as codempresa,'
        " (select s.corbase||'-'||s.nomecorbase  from tcp.SortimentosProduto s WHERE s.codempresa = 1 and s.codproduto = p.codEngenharia and s.codsortimento = p.codSortimento)"
        ' as cor, (select t.descricao from tcp.Tamanhos t WHERE t.codempresa = 1 and t.sequencia = p.seqTamanho ) as tamanho, p.numeroOP as numeroop'
        ' from Tcr.TagBarrasProduto p WHERE p.codEmpresa = ' + emp + ' and '
                                                                     ' p.numeroOP in ( SELECT numeroOP  FROM tco.OrdemProd o WHERE codEmpresa = ' + emp + ' and codFaseAtual in (210, 320, 56, 432, 441, 452, 423, 433, 452 ) and situacao = 3) ',
        conn)
    conn.close()

    ConexaoPostgreMPL.Funcao_InserirOFF(consulta, consulta.size, 'filareposicaoof', 'replace')

    return pd.DataFrame([{'Status': True, 'mensagem':'tags atualizadas na Garantia com sucesso !'}])


def AtualizacaoFilaOFF_op(op):
    emp = empresaConfigurada.EmpresaEscolhida()
    numeroop = op
    op = "'"+op+"'"

    conn = ConexaoCSW.Conexao()

    consulta = pd.read_sql(
        'SELECT p.codBarrasTag as codbarrastag , p.codReduzido as codreduzido, p.codEngenharia as engenharia,'
        '(select i.nome from cgi.Item i WHERE i.codigo = p.codReduzido) as descricao, situacao, codNaturezaAtual as natureza, codEmpresa as codempresa,'
        " (select s.corbase||'-'||s.nomecorbase  from tcp.SortimentosProduto s WHERE s.codempresa = 1 and s.codproduto = p.codEngenharia and s.codsortimento = p.codSortimento)"
        ' as cor, (select t.descricao from tcp.Tamanhos t WHERE t.codempresa = 1 and t.sequencia = p.seqTamanho ) as tamanho, p.numeroOP as numeroop'
        ' from Tcr.TagBarrasProduto p WHERE p.codEmpresa = ' + emp + ' and '
                                                                     ' p.numeroOP = '+op,
        conn)

    conn.close()

    conn2 = ConexaoPostgreMPL.conexao()

    delete = 'Delete from "Reposicao".off.filareposicaoof where numeroop = %s '

    cursor = conn2.cursor()
    cursor.execute(delete
                   , (
                       numeroop,))
    conn2.commit()
    cursor.close()

    conn2.close()

    ConexaoPostgreMPL.Funcao_InserirOFF(consulta, consulta.size, 'filareposicaoof', 'append')

    return pd.DataFrame([{'Status': True, 'mensagem':f'tags da op {numeroop} atualizadas na Garantia com sucesso !'}])
