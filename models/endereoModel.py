import ConexaoPostgreMPL
import pandas as pd

from models import imprimirEtiquetaModel


def ObeterEnderecos():
    conn = ConexaoPostgreMPL.conexao()
    endercos = pd.read_sql(
        ' select * from "Reposicao"."cadendereco" ce   ', conn)
    return endercos







def Acres_0(valor):
    if valor < 10:
        valor = str(valor)
        valor = '0'+valor
        return valor
    else:
        valor = str(valor)
        return valor


def ImportEnderecoDeletar(rua, ruaLimite, modulo, moduloLimite, posicao, posicaoLimite, tipo, codempresa, natureza):

    conn = ConexaoPostgreMPL.conexao()
    query = 'delete from "Reposicao".cadendereco ' \
            'where rua = %s and modulo = %s and posicao = %s'



    r = int(rua)
    ruaLimite = int(ruaLimite) + 1

    m = int(modulo)
    moduloLimite = int(moduloLimite) +1

    p = int(posicao)
    posicaoLimite = int(posicaoLimite)+1

    while r < ruaLimite:
        ruaAtual = Acres_0(r)
        while m < moduloLimite:
            moduloAtual = Acres_0(m)
            while p < posicaoLimite:
                posicaoAtual = Acres_0(p)
                codendereco = ruaAtual + '-' + moduloAtual +"-"+posicaoAtual
                cursor = conn.cursor()
                select = pd.read_sql('select "Endereco" from "Reposicao".tagsreposicao where "Endereco" = %s ', conn,
                                     params=(codendereco,))
                if  select.empty:
                    cursor.execute(query, ( ruaAtual, moduloAtual, posicaoAtual,))
                    conn.commit()
                    cursor.close()
                else:
                    cursor.close()
                    print(f'{codendereco} nao pode ser excluido ')
                p += 1
            p = int(posicao)
            m +=1
        m = int(modulo)
        r += 1


def ObterTipoPrateleira():
    conn = ConexaoPostgreMPL.conexao()
    qurey = pd.read_sql('select tipo from "Reposicao"."configuracaoTipo" ',conn)

    return qurey


def ObterEnderecosEspeciais():
    conn = ConexaoPostgreMPL.conexao()

    consulta = """
    select c.codendereco, ce.saldo , ce."SaldoLiquid"  from "Reposicao"."Reposicao".cadendereco c 
left join "Reposicao"."Reposicao"."calculoEndereco" ce on ce.endereco = c.codendereco 
where c.endereco_subst = 'sim'
    """
    consulta = pd.read_sql(consulta,conn)

    conn.close()

    consulta.fillna(0,inplace = True)

    return consulta