import PediosApontamento
import Reposicao
from Service import reposicaoOPModel
from flask import Blueprint, jsonify, request
from functools import wraps
from Service.configuracoes import empresaConfigurada, SkusSubstitutos
import pandas as pd

reposicaoOP_routes = Blueprint('reposicaoOP', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@reposicaoOP_routes.route('/api/FilaReposicaoOP', methods=['GET'])
@token_required
def get_FilaReposicaoOP():
    emp = empresaConfigurada.EmpresaEscolhida()
    empresa = request.args.get('empresa',emp)
    natureza = request.args.get('natureza','5')

    FilaReposicaoOP = reposicaoOPModel.FilaPorOP(natureza, empresa)
    # Obtém os nomes das colunas
    column_names = FilaReposicaoOP.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    FilaReposicaoOP_data = []
    for index, row in FilaReposicaoOP.iterrows():
        FilaReposicaoOP_dict = {}
        for column_name in column_names:
            FilaReposicaoOP_dict[column_name] = row[column_name]
        FilaReposicaoOP_data.append(FilaReposicaoOP_dict)
    return jsonify(FilaReposicaoOP_data)

@reposicaoOP_routes.route('/api/AtribuirOPRepositor', methods=['POST'])
@token_required
def get_AtribuirOPRepositor():
    # Obtenha os dados do corpo da requisição
    data = request.get_json()
    OP = data['numeroOP']
    Usuario = data['codigo']
    Reatribuir = data.get('reatribuir', False)  # Valor padrão: False, se 'estornar' não estiver presente no corpo

    # Verifica Se existe atribuicao
    existe = reposicaoOPModel.ConsultaSeExisteAtribuicao(OP)
    if existe == 0:
        if Reatribuir is True:
            reposicaoOPModel.AtribuiRepositorOP(Usuario, OP)
            return jsonify({'message': f'OP {OP} reatribuida para o Usuario {Usuario}'})
        else:
            # Retorna uma resposta de existencia
            return jsonify({'message': f'OP já foi Atribuida'})
    else:

        reposicaoOPModel.AtribuiRepositorOP(Usuario, OP)
        # Retorna uma resposta de sucesso
        return jsonify({'message': True})

@reposicaoOP_routes.route('/api/DetalhaOP', methods=['GET'])
@token_required
def get_DetalhaOP():
    emp = empresaConfigurada.EmpresaEscolhida()
    empresa = request.args.get('empresa',emp)
    natureza = request.args.get('natureza','5')

    # Obtém o código do usuário e a senha dos parâmetros da URL
    NumeroOP = request.args.get('numeroOP')
    op = reposicaoOPModel.detalhaOP(NumeroOP,empresa, natureza)
    # Obtém os nomes das colunas
    column_names = op.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in op.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)


@reposicaoOP_routes.route('/api/DetalhaOPxSKU', methods=['GET'])
@token_required
def get_DetalhaOPxSKU():
    emp = empresaConfigurada.EmpresaEscolhida()
    # Obtém o código do usuário e a senha dos parâmetros da URL
    NumeroOP = request.args.get('numeroOP')
    empresa = request.args.get('empresa',emp)
    natureza = request.args.get('natureza','5')


    op = reposicaoOPModel.detalhaOPxSKU(NumeroOP,empresa,natureza)
    # Obtém os nomes das colunas
    column_names = op.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in op.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)


### API UTILIZADA PARA Apontar a Reposicao das Tags nos enderecos, utilizados nos modulos REPOSICAO OP e REPOSICAO SKU da aplicacao mobile:
@reposicaoOP_routes.route('/api/ApontamentoReposicao', methods=['POST'])
@token_required
def get_ApontaReposicao():
        emp = empresaConfigurada.EmpresaEscolhida() # Verificar qual a empresa configuracada (1 - matriz , 4 - Filial)
    #try:

        data = request.get_json() # Obtenha os dados do corpo da requisição
        codUsuario = data['codUsuario']
        codbarra = data['codbarra']
        endereco = data['endereco']
        dataHora = data['dataHora']
        estornar = data.get('estornar', False)  # Valor padrão: False, se 'estornar' não estiver presente no corpo
        natureza = data.get('natureza', '5')  # Valor padrão: False, se 'estornar' não estiver presente no corpo
        empresa = data.get('empresa', emp)  # Valor padrão: False, se 'estornar' não estiver presente no corpo
        print(data)


        # ETAPA 1 - Funacao utilizada para fazer a atualizacao do codigo de barra como REPOSTO.
        Apontamento, restricao = Reposicao.RetornoLocalCodBarras(codUsuario, codbarra, endereco, dataHora, empresa, natureza, estornar)

        if Apontamento == 'Reposto':
            if estornar == True:
                Reposicao.EstornoApontamento(codbarra, empresa, natureza)
                return jsonify({'message': f'codigoBarras {codbarra} estornado!'})

            ender, ender2 = PediosApontamento.EndereçoTag(codbarra, empresa, natureza)
            return jsonify({'message': f'codigoBarras {codbarra} ja reposto no endereço {ender}'})

        if Apontamento is False:
            return jsonify({'message': False, 'Status': f'codigoBarras {codbarra} nao existe no Estoque'})

        else:
            ## ETAPA 2 - Verifica se a tag tem restricao de Substituto para ser tratato especial:

            configuracaoRestricao = empresaConfigurada.RegraDeEnderecoParaSubstituto()  # Retorno implenta_endereco_subs: sim ou nao

            # 2.1 - Verifica se o WMS esta configurado para restringir os enderecos dos substitutos
            if configuracaoRestricao == 'simxx':

                if restricao != '-': # Caso a tag tenha restricao de substituto

                    # Verifica o endereco proposto para a reposicao
                    enderecoPreReservado = SkusSubstitutos.EnderecoPropostoSubtituicao(restricao)  # Retorna o endereco Pré reservado

                    # 2.1.1 Caso o endereco reposto nao corresponda ao pré reservado:
                    if enderecoPreReservado != endereco:

                        Retorno = pd.DataFrame([{'status': False,
                                                 'message': f'Erro! o Endereco: {endereco}  nao corresponde ao Sugerido {enderecoPreReservado}, reponha no endereco sugerido!'}])  # 'Mesagem'
                        column_names = Retorno.columns  # Obtém os nomes das colunas
                        enderecos_data = []  # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes

                        for index, row in Retorno.iterrows():
                            enderecos_dict = {}
                            for column_name in column_names:
                                enderecos_dict[column_name] = row[column_name]
                            enderecos_data.append(enderecos_dict)
                        return jsonify(
                            enderecos_data)  # Devolve a resposta no Json com a mensagem de erro
                    else:

                        Reposicao.InserirReposicao(codUsuario, codbarra, endereco, dataHora, empresa, natureza,
                                                   estornar)
                        # Limpar a Pré Reserva do Endereco
                        SkusSubstitutos.LimprandoPréReserva(endereco)

                        return jsonify({'message': True, 'status': f'Salvo com Sucesso'})

            else:
                Reposicao.InserirReposicao(codUsuario, codbarra, endereco, dataHora, empresa, natureza, estornar)
                return jsonify({'message': True, 'status': f'eu seu alvo com Sucesso'})

   # except KeyError as e:
    #    return jsonify({'message': 'Erro nos dados enviados.', 'error': str(e)}), 400
