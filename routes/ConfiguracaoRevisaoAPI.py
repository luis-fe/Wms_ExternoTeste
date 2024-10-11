'''
rotas para acessar a API no modulo configuracao Revisao
'''

from models import ConfiguracaoRevisao as Conf
from flask import Blueprint, jsonify, request
from functools import wraps

ConfiguracaoRevisao_routes = Blueprint('ConfiguracaoRevisao_routes', __name__)


def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function
'''
@ConfiguracaoRevisao_routes('/api/obterConceitosCsw', methods=['GET'])
@token_required
def get_obterConceitos():
    # Obtém os valores dos parâmetros DataInicial e DataFinal, se estiverem presentes na requisição
    #empresa = request.args.get('empresa','1')
    dados = Conf.ConfiguracaoRevisao().obterConceitosCsw()


    # Obtém os nomes das colunas
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in dados.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)

'''