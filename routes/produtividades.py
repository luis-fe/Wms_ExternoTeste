from Service import produtividadeModel
from flask import Blueprint, jsonify, request
from functools import wraps
import pandas as pd

produtividade_routes = Blueprint('produtividade', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@produtividade_routes.route('/api/TagsReposicao/Resumo', methods=['GET'])
@token_required
def get_TagsReposicao():
    # Obtém os valores dos parâmetros DataInicial e DataFinal, se estiverem presentes na requisição
    data_inicial = request.args.get('DataInicial','0')
    data_final = request.args.get('DataFinal','0')
    horarioInicial = request.args.get('horarioInicial', '01:00:00')
    horarioFinal = request.args.get('horarioFinal', '23:59:00')
    #Relatorios.RelatorioSeparadoresLimite(10)
    TagReposicao = produtividadeModel.ProdutividadeRepositores(data_inicial,data_final, horarioInicial , horarioFinal)
    TagReposicao = pd.DataFrame(TagReposicao)


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

@produtividade_routes.route('/api/TagsSeparacao/Resumo', methods=['GET'])
@token_required
def get_TagsSeparacao():
    # Obtém os valores dos parâmetros DataInicial e DataFinal, se estiverem presentes na requisição
    data_inicial = request.args.get('DataInicial','0')
    data_final = request.args.get('DataFinal','0'),
    horarioInicial = request.args.get('horarioInicial', '01:00:00')
    horarioFinal = request.args.get('horarioFinal', '23:59:00')

    #Relatorios.RelatorioSeparadoresLimite(10)
    TagReposicao = produtividadeModel.ProdutividadeSeparadores(data_inicial,data_final, horarioInicial, horarioFinal)
    TagReposicao = pd.DataFrame(TagReposicao)

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
@produtividade_routes.route('/api/DetalhaRitmoRepositor', methods=['GET'])
@token_required
def DetalhaRitmoRepositor():
    # Obtém os valores dos parâmetros DataInicial e DataFinal, se estiverem presentes na requisição
    data_inicial = request.args.get('DataInicial','0')
    data_final = request.args.get('DataFinal','0'),
    horarioInicial = request.args.get('horarioInicial', '01:00:00')
    horarioFinal = request.args.get('horarioFinal', '23:59:00')
    usuario = request.args.get('usuario','-')

    #Relatorios.RelatorioSeparadoresLimite(10)
    TagReposicao = produtividadeModel.DetalhaRitmoRepositor(usuario,data_inicial,data_final)

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

@produtividade_routes.route('/api/RelatorioSeparacao', methods=['GET'])
@token_required
def RelatorioSeparacao():
    # Obtém os valores dos parâmetros DataInicial e DataFinal, se estiverem presentes na requisição
    data_inicial = request.args.get('DataInicial','0')
    data_final = request.args.get('DataFinal','0')
    usuario = request.args.get('usuario','')

    #Relatorios.RelatorioSeparadoresLimite(10)
    TagReposicao = produtividadeModel.RelatorioSeparacao('1',data_inicial,data_final,usuario)
    TagReposicao = pd.DataFrame(TagReposicao)

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