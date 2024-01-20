
from flask import Blueprint, jsonify, request
from functools import wraps
from Service import LinhasPortal
from Service.configuracoes import empresaConfigurada

linhas_routes = Blueprint('linhasPortal', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@linhas_routes.route('/api/linhasPadrao', methods=['GET'])
@token_required
def linhasPadrao():
    usuarios = LinhasPortal.PesquisarLinhaPadrao()
    # Obtém os nomes das colunas
    column_names = ['Linha', 'operador1', 'operador2', 'operador3']
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    usuarios_data = []
    for row in usuarios:
        usuario_dict = dict(zip(column_names, row))
        usuarios_data.append(usuario_dict)
    return jsonify(usuarios_data)

