from models.configuracoes import escalaTrabalhoModel
from flask import Blueprint, jsonify, request
from functools import wraps
import pandas as pd

escalaTrabalho_routes = Blueprint('escalaTrabalho', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@escalaTrabalho_routes.route('/api/escalaTrabalho', methods=['GET'])
@token_required
def escalaTrabalho():
    # Obtém os dados do corpo da requisição (JSON)


    Endereco_det = escalaTrabalhoModel.Get_escalaTrabalho()

    # Obtém os nomes das colunas
    column_names = Endereco_det.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in Endereco_det.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)
