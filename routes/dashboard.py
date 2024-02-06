import Service.ConfrontoTag_Posicao
from Service import dashboardModel
from flask import Blueprint, jsonify, request
from functools import wraps
import pandas as pd
from Service.Dashboards import ConsultaEstoque


dashboard_routes = Blueprint('dashboard', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@dashboard_routes.route('/api/RelatorioTotalFila', methods=['GET'])
@token_required
def get_RelatorioTotalFila():
    # Obtém os dados do corpo da requisição (JSON)
    empresa = request.args.get('empresa','1')
    natureza = request.args.get('natureza','5')
    Endereco_det = dashboardModel.relatorioTotalFila(empresa, natureza)
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

@dashboard_routes.route('/api/statuspedidos', methods=['GET'])
@token_required
def get_statuspedidos():
    # Obtém os dados do corpo da requisição (JSON)

    Endereco_det = dashboardModel.Pedidos_fecha100()
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

@dashboard_routes.route('/api/confrontoTags', methods=['GET'])
@token_required
def confrontoTags():
    # Obtém os dados do corpo da requisição (JSON)

    Endereco_det = Service.ConfrontoTag_Posicao.Confronto()
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
@dashboard_routes.route('/api/ConsultaEstoqueWMS', methods=['GET'])
@token_required
def ConsultaEstoqueWMS():
    # Obtém os dados do corpo da requisição (JSON)
    natureza = request.args.get('natureza','-')
    codengenharia = request.args.get('codengenharia','-')
    endereco = request.args.get('endereco','-')
    codreduzido = request.args.get('codreduzido','-')
    numeroOP = request.args.get('numeroOP','-')
    rua = request.args.get('rua','-')
    modulo = request.args.get('modulo','-')
    posicao = request.args.get('posicao','-')

    limit  = request.args.get('limit',10000)

    Endereco_det = ConsultaEstoque.ConsultaEnderecoReposto(natureza,codreduzido,codengenharia,numeroOP,rua, modulo , posicao, limit)
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

