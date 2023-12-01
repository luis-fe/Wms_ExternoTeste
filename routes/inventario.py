from Service import inventarioModel
from flask import Blueprint, jsonify, request
from functools import wraps
import pandas as pd

inventario_routes = Blueprint('inventario', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function
# Api para o processo de inventario
@inventario_routes.route('/api/RegistrarInventario', methods=['POST'])
@token_required
def get_ProtocolarInventario():
    # Obtém os dados do corpo da requisição (JSON)
    datas = request.get_json()
    codUsuario = datas['codUsuario']
    data = datas['data']
    endereco = datas['endereço']

    Endereco_det = inventarioModel.SituacaoEndereco(endereco, codUsuario, data)
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


@inventario_routes.route('/api/ApontarTagInventario', methods=['POST'])
@token_required
def get_ApontarTagInventario():
    # Obtém os dados do corpo da requisição (JSON)
    datas = request.get_json()
    codbarras = datas['codbarras']
    codusuario = datas['codUsuario']
    endereco = datas.get('endereço','-')
    Prosseguir = datas.get('Prosseguir', False)  # Valor padrão: False, se 'estornar' não estiver presente no corpo

    Endereco_det = inventarioModel.ApontarTagInventario(codbarras, endereco, codusuario, Prosseguir)

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

@inventario_routes.route('/api/FinalizarInventario', methods=['POST'])
@token_required
def get_FinalizarInventario():
    # Obtém os dados do corpo da requisição (JSON)
    datas = request.get_json()
    endereco = datas['endereço']
    inventarioModel.ExcluirTagsDuplicadas(endereco)

    Endereco_det = inventarioModel.SalvarInventario(endereco)
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

@inventario_routes.route('/api/RelatorioInventario', methods=['GET'])
@token_required
def get_RelatorioInventario():
    # Obtém os dados do corpo da requisição (JSON)
    natureza = request.args.get('natureza', '')
    empresa = request.args.get('empresa', '1')
    datainicio =request.args.get('datainicio')
    datafinal = request.args.get('datafinal')
    emitirRelatorio = request.args.get('emitirRelatorio','')

    if emitirRelatorio == 'True' or emitirRelatorio == True:
        emitir = True
    else:
        emitir = False

    Endereco_det = inventarioModel.RelatorioInventario(datainicio,datafinal,natureza,empresa,emitir)

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