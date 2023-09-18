from src.Service import finalizacaoPedidoModel
from flask import Blueprint, jsonify, request
from functools import wraps
import pandas as pd

finalizacaoPedido_route = Blueprint('finalizacaoPedido', __name__)

@finalizacaoPedido_route.route('/api/caixas', methods=['GET'])
def get_Caixas():
    # Obtém os dados do corpo da requisição (JSON)

    Endereco_det = finalizacaoPedidoModel.Buscar_Caixas()

    # Converte o DataFrame em um dicionário
    end_data = Endereco_det.to_dict(orient='records')

    # Retorna o dicionário como JSON
    return jsonify(end_data)