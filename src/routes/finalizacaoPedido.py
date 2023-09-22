from src.Service import finalizacaoPedidoModel
from flask import Blueprint, jsonify, request
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

@finalizacaoPedido_route.route('/api/FinalizarPedido', methods=['POST'])
def FinalizarPedido():
    # Obtenha os dados do corpo da requisição
    novo_endereco = request.get_json()
    # Extraia os valores dos campos do novo usuário

    pedido = novo_endereco.get('pedido')
    dataFinalizacao = novo_endereco.get('dataFinalizacao')
    caixas = novo_endereco.get('Modelo de caixa')
    consumos = novo_endereco.get('consumos')


    Endereco_det = finalizacaoPedidoModel.finalizarPedido(pedido, caixas, consumos)
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