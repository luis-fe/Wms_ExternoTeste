######
# Nesse arquivo é fornecido a Api das operacoes envolvendo o login e cadastro de usuarios do WMS
from flask import Blueprint, jsonify, request
from functools import wraps
from  models import PerfilTelaAcesso

PerfilTelaAcesso_routes = Blueprint('PerfilTelaAcesso_routes', __name__) # Esse é o nome atribuido para o conjunto de rotas envolvendo usuario


# TOKEN FIXO PARA ACESSO AO CONTEUDO
def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@PerfilTelaAcesso_routes.route('/api/TelasAcesso', methods=['GET'])
@token_required
def get_usuarios():
    consulta = PerfilTelaAcesso.TelaAcesso().consultaTelasAcesso()
    # Obtém os nomes das colunas
    column_names = consulta.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    consulta_data = []
    for index, row in consulta.iterrows():
        consulta_dict = {}
        for column_name in column_names:
            consulta_dict[column_name] = row[column_name]
        consulta_data.append(consulta_dict)
    return jsonify(consulta_data)
