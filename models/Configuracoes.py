import pandas as pd
import ConexaoPostgreMPL


class ConfiguracoesGerais():
    '''Classe criada para as configuracoes gerais do WMS'''

    def __init__(self, empresa = None, implantaSubstituto = None, natureza = None, regraReserva = None, descricaoRegraca = None,
                 situacaoRegra = None):

        self.empresa = empresa
        self.implantaSubstituto = implantaSubstituto
        self.natureza = natureza
        self.regraReserva = regraReserva
        self.descricaoRegraca = descricaoRegraca
        self.situacaoRegra = situacaoRegra

    def consultaRegraDeEnderecoParaSubstituto(self):
        '''Metodo criado para consultar a regra de Endereci Substituto'''
        conn = ConexaoPostgreMPL.conexaoEngine()
        empresa = pd.read_sql('Select implenta_endereco_subs from "Reposicao".configuracoes.empresa '
                              'where empresa = %s', conn, params=(self.empresa,))

        return empresa['implenta_endereco_subs'][0]

    def put_RegraDeEnderecoParaSubstituto(self):
        '''Metodo que modifica a rega do substituto no WMS'''

        update = """
        update 
            "Reposicao".configuracoes.empresa
        set 
            implenta_endereco_subs = %s
        where
            empresa = %s
        """

        with ConexaoPostgreMPL.conexao() as conn:
            with conn.cursor() as curr:
                curr.execute(update,(self.implantaSubstituto, self.empresa))
                conn.commit()

