from Service import AcompanhamentoSegundaQual
from flask import Blueprint, jsonify, request, Flask, send_from_directory
from functools import wraps
from flask_cors import CORS
import pandas as pd
import os
from werkzeug.utils import secure_filename


app = Flask(__name__)
app.debug = True  # Ativa o modo de depuração


AcompanhamentoQual_routes = Blueprint('AcompanhamentoQual', __name__)

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

@AcompanhamentoQual_routes.route('/api/AcompanhamentoQualidade', methods=['GET'])
@token_required
def AcompanhamentoQualidade():
    # Obtém os dados do corpo da requisição (JSON)
    DataIncial = request.args.get('DataIncial','')
    DataFinal = request.args.get('DataFinal', '')


    Endereco_det = AcompanhamentoSegundaQual.TagSegundaQualidade(DataIncial,DataFinal)
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

@AcompanhamentoQual_routes.route('/api/MotivosAgrupado', methods=['GET'])
@token_required
def MotivosAgrupado():
    # Obtém os dados do corpo da requisição (JSON)
    DataIncial = request.args.get('DataIncial','')
    DataFinal = request.args.get('DataFinal', '')

    Endereco_det = AcompanhamentoSegundaQual.MotivosAgrupado(DataIncial,DataFinal)
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

@AcompanhamentoQual_routes.route('/api/OrigemAgrupado', methods=['GET'])
@token_required
def OrigemAgrupado():
    # Obtém os dados do corpo da requisição (JSON)
    DataIncial = request.args.get('DataIncial','')
    DataFinal = request.args.get('DataFinal', '')

    Endereco_det = AcompanhamentoSegundaQual.PorOrigem(DataIncial,DataFinal)
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


