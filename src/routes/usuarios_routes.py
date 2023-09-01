from flask import jsonify, request,Blueprint
from functools import wraps
from src.Service import usuariosCad

usuarios_routes = Blueprint('usuarios_routes', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@usuarios_routes.route('/api/Usuarios', methods=['GET'])
@token_required
def get_usuarios():
    usuarios = usuariosCad.PesquisarUsuarios()
    # Obtém os nomes das colunas
    column_names = ['codigo', 'nome', 'funcao', 'situacao']
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    usuarios_data = []
    for row in usuarios:
        usuario_dict = dict(zip(column_names, row))
        usuarios_data.append(usuario_dict)
    return jsonify(usuarios_data)