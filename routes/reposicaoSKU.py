from Service import reposicaoSKUModel
from flask import Blueprint, jsonify, request
from functools import wraps

reposicaoSKU_routes = Blueprint('reposicaoSKU', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@reposicaoSKU_routes.route('/api/DetalhaSKU', methods=['GET'])
@token_required
def get_DetalhaSKU():
    # Obtém o código do usuário e a senha dos parâmetros da URL
    codreduzido = request.args.get('codreduzido')
    empresa = request.args.get('empresa','1')
    natureza = request.args.get('natureza','5')
    op = reposicaoSKUModel.detalhaSku(codreduzido, empresa, natureza)
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

@reposicaoSKU_routes.route('/api/DetalhaCodBarras', methods=['GET'])
@token_required
def get_DetalhacodBarras():
    # Obtém o código do usuário e a senha dos parâmetros da URL
    codbarras = request.args.get('codbarras')

    op = reposicaoSKUModel.DetalhaTag(codbarras)
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



