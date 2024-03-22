from Service.configuracoes import SkusSubstitutos
from flask import Blueprint, jsonify, request, Flask, send_from_directory
from functools import wraps
from flask_cors import CORS
import pandas as pd
import os
from werkzeug.utils import secure_filename


app = Flask(__name__)
app.debug = True  # Ativa o modo de depuração


SkusSubstitutos_routes = Blueprint('SkusSubstitutos', __name__)

# Defina o diretório onde as imagens serão armazenadas
UPLOAD_FOLDER = 'imagens_chamado'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
CORS(app)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@SkusSubstitutos_routes.route('/api/SubstitutosPorOP', methods=['GET'])
@token_required
def SubstitutosPorOP():
    # Obtém os dados do corpo da requisição (JSON)
    categoria = request.args.get('categoria','')


    Endereco_det = SkusSubstitutos.SubstitutosPorOP(categoria)
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

@SkusSubstitutos_routes.route('/api/CategoriasSubstitutos', methods=['GET'])
@token_required
def CategoriasSubstitutos():
    # Obtém os dados do corpo da requisição (JSON)


    Endereco_det = SkusSubstitutos.ObterCategorias()
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

@SkusSubstitutos_routes.route('/api/SalvarSubstitutos', methods=['PUT'])
@token_required
def SalvarSubstitutos():
    # Obtém os dados do corpo da requisição (JSON)


    arrayOP = request.args.get('arrayOP','')
    arraycor = request.args.get('arraycor','')
    arraydesconsidera = request.args.get('arraydesconsidera','')


    Endereco_det = SkusSubstitutos.UpdetaConsidera(arrayOP, arraycor, arraydesconsidera)
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

@SkusSubstitutos_routes.route('/api/AvaliarRestricao', methods=['GET'])
@token_required
def AvaliarRestricao():
    # Obtém os dados do corpo da requisição (JSON)
    numeroop = request.args.get('numeroop')
    sku = request.args.get('sku')

    Endereco_det = SkusSubstitutos.SugerirEnderecoRestrito(numeroop, sku)
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