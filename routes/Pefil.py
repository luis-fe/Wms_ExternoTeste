######
# Nesse arquivo é fornecido a Api das operacoes envolvendo o login e cadastro de usuarios do WMS
from flask import Blueprint, jsonify, request
from functools import wraps
from  models import Perfil

Perfil_routes = Blueprint('Perfil_routes', __name__) # Esse é o nome atribuido para o conjunto de rotas envolvendo usuario


# TOKEN FIXO PARA ACESSO AO CONTEUDO
def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@Perfil_routes.route('/api/consultaPerfil', methods=['GET'])
@token_required
def get_consultaPerfil():
    consulta = Perfil.Perfil().consultaPerfil()
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

@Perfil_routes.route('/api/cadastrarOuAtualizarPerfil', methods=['POST'])
@token_required
def post_cadastrarOuAtualizarPerfil():

    # Obtenha os dados do corpo da requisição
    novo_Tela = request.get_json()
    # Extraia os valores dos campos do novo usuário
    codPerfil = novo_Tela.get('codPerfil')
    nomePerfil = novo_Tela.get('nomePerfil')
    telasAcesso = novo_Tela.get('telasAcesso')

    consulta = Perfil.Perfil(codPerfil, nomePerfil, telasAcesso).cadastrarOuAtualizarPerfil()
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
