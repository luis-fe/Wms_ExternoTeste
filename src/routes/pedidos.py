from src.Service import pedidosModel
from flask import Blueprint, jsonify, request
from functools import wraps
import pandas as pd

pedidos_routes = Blueprint('pedidos', __name__)


def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function
@pedidos_routes.route('/api/FilaPedidos', methods=['GET'])
@token_required
def get_FilaPedidos():

    Pedidos = pedidosModel.FilaPedidos()
    # Obtém os nomes das colunas
    column_names = Pedidos.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in Pedidos.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)

@pedidos_routes.route('/api/FilaPedidosUsuario', methods=['GET'])
@token_required
def get_FilaPedidosUsuario():
    codUsuario = request.args.get('codUsuario')
    Pedidos = pedidosModel.FilaAtribuidaUsuario(codUsuario)
    # Obtém os nomes das colunas
    column_names = Pedidos.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in Pedidos.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)
@pedidos_routes.route('/api/DetalharPedido', methods=['GET'])
@token_required
def get_DetalharPedido():
    # Obtém os dados do corpo da requisição (JSON)
    codPedido = request.args.get('codPedido')

    Endereco_det = pedidosModel.DetalhaPedido(codPedido)
    Endereco_det = pd.DataFrame(Endereco_det)

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