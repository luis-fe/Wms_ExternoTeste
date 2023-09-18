from src.Service import finalizacaoPedidoModel
from flask import Blueprint, jsonify, request
from functools import wraps
import pandas as pd

finalizacaoPedido_route = Blueprint('finalizacaoPedido', __name__)

@finalizacaoPedido_route.route('/api/caixas', methods=['GET'])
def get_Caixas():
    # Obtém os dados do corpo da requisição (JSON)

    Endereco_det = finalizacaoPedidoModel.Buscar_Caixas()
    # Crie um dicionário com a chave desejada (por exemplo, "nome_do_vetor")
    response_data = {
        "Opçoes de Caixa": Endereco_det
    }


    # Retorna o dicionário como JSON
    return jsonify(response_data)