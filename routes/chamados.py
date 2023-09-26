from Service.chamados import areaModel, chamadosModel
from flask import Blueprint, jsonify, request, send_from_directory
from functools import wraps
import pandas as pd
import os
from werkzeug.utils import secure_filename

chamados_routes = Blueprint('chamados', __name__)

# Defina o diretório onde as imagens serão armazenadas
#UPLOAD_FOLDER = 'imagens_chamado'
#chamados_routes.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

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
    solicitante = data.get('solicitante')
    data_chamado = data.get('data_chamado')
    tipo_chamado = data.get('tipo_chamado')
    descricao_chamado = data.get('descricao_chamado')
    empresa = data.get('empresa', '1')
    area = data.get('area')

    # Busca o responsavel pela area
    responsavel = areaModel.Atribuir_por_Area(empresa,area)
    # Verifica Se existe atribuicao
    existe = chamadosModel.novo_chamados(solicitante, data_chamado, tipo_chamado, responsavel, descricao_chamado, 'nao iniciado', '-' )
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



# Rota para enviar a imagem
@chamados_routes.route('/api/upload_chamado/<string:idchamado>', methods=['POST'])
def upload_image(idchamado):
    # Verifique se a solicitação possui um arquivo anexado
    if 'file' not in request.files:
        return jsonify({'message': 'chamado sem anexo'}), 200

    file = request.files['file']

    # Verifique se o nome do arquivo é vazio
    if file.filename == '':
        return jsonify({'message': 'chamado sem anexo'}), 200

    # Verifique se a extensão do arquivo é permitida (opcional)
    allowed_extensions = {'png', 'jpg', 'jpeg', 'gif'}
    if not file.filename.rsplit('.', 1)[1].lower() in allowed_extensions:
        return jsonify({'message': 'Extensão de arquivo não permitida'}), 400

    # Renomeie o arquivo com o ID do chamado e a extensão original
    filename = secure_filename(f'{idchamado}.{file.filename.rsplit(".", 1)[1]}')

    # Salve o arquivo na pasta de uploads usando idchamado como diretório
    upload_directory = os.path.join(chamados_routes.config['UPLOAD_FOLDER'], idchamado)

    # Verifique se o diretório existe e crie-o se não existir
    os.makedirs(upload_directory, exist_ok=True)

    # Salve o arquivo com o novo nome
    file.save(os.path.join(upload_directory, filename))

    return jsonify({'message': 'Arquivo enviado com sucesso'}), 201

