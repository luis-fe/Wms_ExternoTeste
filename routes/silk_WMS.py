from Service import silkWMSModel
from flask import Blueprint, jsonify, request
from functools import wraps

silkWMS_routes = Blueprint('silkWMS', __name__)


def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function



# ETAPA 2:  Api para acesso do Quadro de Estamparia - Projeto WMS das Telas de  Silk:

@silkWMS_routes.route('/api/Silk/PesquisaEndereco', methods=['GET'])
@token_required
def get_PesquisaEndereco():
    Coluna = request.args.get('Coluna')
    Operador = request.args.get('Operador')
    Nome = request.args.get('Nome')

    resultados = silkWMSModel.PesquisaEnderecos(Coluna, Operador, Nome)
    # Obtém os nomes das colunas
    column_names = resultados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    enderecos_data = []
    for index, row in resultados.iterrows():
        enderecos_dict = {}
        for column_name in column_names:
            enderecos_dict[column_name] = row[column_name]
        enderecos_data.append(enderecos_dict)
    return jsonify(enderecos_data)


@silkWMS_routes.route('/api/Silk/deleteTelas', methods=['DELETE'])
@token_required
def delete_endpoint():
    # Obtenha os dados do corpo da requisição
    novo_usuario = request.get_json()
    # Extraia os valores dos campos do novo usuário
    endereco = novo_usuario.get('endereco')
    produto = novo_usuario.get('produto')


    # Chama a função Funcao_Deletar para realizar a exclusão
    resultado = silkWMSModel.Funcao_Deletar(endereco, produto)

    if resultado == True:
        return f'endereco: {endereco}, produto {produto}  EXCLUIDOS NO CADASTRO DE SILK', 200
    else:
        return 'Falha ao deletar', 500


@silkWMS_routes.route('/api/Silk/IserirTelas', methods=['PUT'])
@token_required
def insert_endpoint():
    # Obtenha os dados do corpo da requisição
    novo_usuario = request.get_json()
    # Extraia os valores dos campos do novo usuário
    endereco = novo_usuario.get('endereco')
    produto = novo_usuario.get('produto')

    # Chama a função Funcao_Inserir para realizar a inserção
    resultado = silkWMSModel.Funcao_Inserir(produto, endereco)

    if resultado == True:
        return jsonify({'Mesagem':f'produto {produto} endereço {endereco}, Inserção realizada com sucesso'}), 200
    else:
        return jsonify({'Mesagem':'Falha ao inserir'}), 500

@silkWMS_routes.route('/api/Silk/PesquisaReferencia', methods=['GET'])
@token_required
def PesquisaReferencia():
    numeroOP = request.args.get('numeroOP')

    enderecos = silkWMSModel.PesquisarReferencia(numeroOP)
    # Obtém os nomes das colunas
    column_names = enderecos.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    enderecos_data = []
    for index, row in enderecos.iterrows():
        enderecos_dict = {}
        for column_name in column_names:
            enderecos_dict[column_name] = row[column_name]
        enderecos_data.append(enderecos_dict)
    return jsonify(enderecos_data)

