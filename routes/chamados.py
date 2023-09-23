from Service import chamadosModel
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

    Endereco_det = chamadosModel.Obter_chamados()
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
    atribuido_para = data['atribuido_para']
    descricao_chamado = data['descricao_chamado']
    status_chamado = data['status_chamado']
    data_finalizacao_chamado = data['data_finalizacao_chamado']


    # Verifica Se existe atribuicao
    existe = chamadosModel.novo_chamados(solicitante, data_chamado, tipo_chamado, atribuido_para, descricao_chamado, status_chamado, data_finalizacao_chamado )
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