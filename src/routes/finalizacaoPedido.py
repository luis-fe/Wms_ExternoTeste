from src.Service import finalizacaoPedidoModel
from flask import Blueprint, jsonify, request
from functools import wraps
import pandas as pd

finalizacaoPedido_route = Blueprint('finalizacaoPedido', __name__)

@finalizacaoPedido_route.route('/api/caixas', methods=['GET'])
def get_Caixas():
    # Obtém os dados do corpo da requisição (JSON)

    Endereco_det = finalizacaoPedidoModel.Buscar_Caixas()

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