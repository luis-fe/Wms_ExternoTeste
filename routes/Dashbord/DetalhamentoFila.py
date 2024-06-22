from flask import Blueprint, jsonify, request
from functools import wraps
from models.Dashboards import DetalhamentoFila


dashboardFila_routes = Blueprint('dashboardFila', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@dashboardFila_routes.route('/api/DetalhamentoFila', methods=['GET'])
@token_required
def getDetalhamentoFila():
    # Obtém os dados do corpo da requisição (JSON)
    empresa = request.args.get('empresa','1')
    natureza = request.args.get('natureza','5')
    Endereco_det = DetalhamentoFila.detalhaFila(empresa, natureza)
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

@dashboardFila_routes.route('/api/DetalharCaixa', methods=['GET'])
@token_required
def getDetalharCaixa():
    # Obtém os dados do corpo da requisição (JSON)
    numeroCaixa = request.args.get('numeroCaixa','1')
    Endereco_det = DetalhamentoFila.DetalharCaixa(numeroCaixa)
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

@dashboardFila_routes.route('/api/DetalhaTagsNumeroOPReduzido', methods=['GET'])
@token_required
def getDetalhaTagsNumeroOPReduzido():
    # Obtém os dados do corpo da requisição (JSON)
    numeroop = request.args.get('numeroop','1')
    codreduzido = request.args.get('codreduzido','1')
    codEmpresa = request.args.get('codEmpresa','1')
    natureza = request.args.get('natureza','5')


    Endereco_det = DetalhamentoFila.DetalhaTagsNumeroOPReduzido(numeroop, codreduzido, codEmpresa, natureza)
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

@dashboardFila_routes.route('/api/TagsFilaConferencia', methods=['GET'])
@token_required
def getTagsFilaConferencia():

    Endereco_det = DetalhamentoFila.TagsFilaConferencia()
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

