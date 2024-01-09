from Service import reposicaoOPModel
from flask import Blueprint, jsonify, request
from functools import wraps
from Service.configuracoes import empresaConfigurada

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
    # Obtém o código do usuário e a senha dos parâmetros da URL
    NumeroOP = request.args.get('numeroOP')
    empresa = request.args.get('empresa','1')
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