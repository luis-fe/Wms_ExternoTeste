from Service import chamadosModel
from Service.chamados import areaModel
from flask import Blueprint, jsonify, request
from functools import wraps
import pandas as pd

chamados_routes = Blueprint('chamados', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@chamados_routes.route('/api/chamados', methods=['GET'])
@token_required
def get_chamados():
    # Obtém os dados do corpo da requisição (JSON)
    status = request.args.get('status','')
    solicitante = request.args.get('solicitante', '')
    atribuido_para = request.args.get('atribuido_para', '')
    tipo_chamado = request.args.get('tipo_chamado', '')

    Endereco_det = chamadosModel.Obter_chamados(status,solicitante, atribuido_para,tipo_chamado)
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

@chamados_routes.route('/api/NovoChamado', methods=['POST'])
@token_required
def post_novochamado():
    # Obtenha os dados do corpo da requisição
    data = request.get_json()
    solicitante = data['solicitante']
    data_chamado = data['data_chamado']
    tipo_chamado = data['tipo_chamado']
    descricao_chamado = data['descricao_chamado']
    data_finalizacao_chamado = data['data_finalizacao_chamado','-']
    empresa = data['empresa','1']
    area = data['area']

    # Busca o responsavel pela area
    responsavel = areaModel.Atribuir_por_Area(empresa,area)
    # Verifica Se existe atribuicao
    existe = chamadosModel.novo_chamados(solicitante, data_chamado, tipo_chamado, responsavel, descricao_chamado, 'nao iniciado', data_finalizacao_chamado )
    if existe ==True:
        # Retorna uma resposta de sucesso
        return jsonify({'status': True, 'mensagem':'novo chamado criado !'})
    else:
        return jsonify({'status': False, 'mensagem':'erro ao criar chamado'})

@chamados_routes.route('/api/EncerrarChamado', methods=['PUT'])
@token_required
def EncerrarChamado():
    # Obtenha os dados do corpo da requisição
    data = request.get_json()
    id_chamado = data['id_chamado']
    data_finalizacao_chamado = data['data_finalizacao_chamado']


    # Verifica Se existe atribuicao
    existe = chamadosModel.encerrarchamado(id_chamado, data_finalizacao_chamado )
    if existe ==True:
        # Retorna uma resposta de sucesso
        return jsonify({'status': True, 'mensagem':'Chamado Finalizado'})
    else:
        return jsonify({'status': False, 'mensagem':'Chamado nao encontrado'})

@chamados_routes.route('/api/area', methods=['GET'])
@token_required
def get_areas():
    # Obtém os dados do corpo da requisição (JSON)
    empresa = request.args.get('empresa','1')


    Endereco_det = areaModel.get_Areas(empresa)

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