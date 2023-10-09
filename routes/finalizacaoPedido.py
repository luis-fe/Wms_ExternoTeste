from Service import finalizacaoPedidoModel
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

@finalizacaoPedido_route.route('/api/CaixasCadastradas', methods=['GET'])
def CaixasCadastradas():
    # Obtém os dados do corpo da requisição (JSON)

    Endereco_det = finalizacaoPedidoModel.GetCaixas()

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

@finalizacaoPedido_route.route('/api/CadastrarCaixa', methods=['POST'])
def CadastrarCaixa():
    # Obtém os dados do corpo da requisição (JSON)
    # Obtenha os dados do corpo da requisição
    novo_endereco = request.get_json()
    codcaixa = novo_endereco.get('codcaixa')
    nomecaixa = novo_endereco.get('nomecaixa')
    tamanhocaixa = novo_endereco.get('tamanhocaixa')

    Endereco_det = finalizacaoPedidoModel.InserirNovaCaixa(codcaixa,nomecaixa,tamanhocaixa)


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

@finalizacaoPedido_route.route('/api/AtualizarCaixa/<string:codcaixa>', methods=['PUT'])
def AtualizarCaixa(codcaixa):
    # Obtém os dados do corpo da requisição (JSON)
    # Obtenha os dados do corpo da requisição
    novo_endereco = request.get_json()
    codcaixa2 = novo_endereco.get('codcaixa','0')
    nomecaixa2 = novo_endereco.get('nomecaixa','0')
    tamanhocaixa2 = novo_endereco.get('tamanhocaixa','0')

    Endereco_det = finalizacaoPedidoModel.AtualizarCaixa(codcaixa,codcaixa2, nomecaixa2,tamanhocaixa2)


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

@finalizacaoPedido_route.route('/api/relatorioCaixas', methods=['GET'])
def relatorioCaixas():
    # Obtém os valores dos parâmetros DataInicial e DataFinal, se estiverem presentes na requisição
    empresa = request.args.get('empresa','1')
    dataInicio = request.args.get('dataInicio')
    dataFim = request.args.get('dataFim')
    #Relatorios.RelatorioSeparadoresLimite(10)
    TagReposicao = finalizacaoPedidoModel.RelatorioConsumoCaixa(dataInicio, dataFim)


    # Obtém os nomes das colunas
    column_names = TagReposicao.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in TagReposicao.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)
