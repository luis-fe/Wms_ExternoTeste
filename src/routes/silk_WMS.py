from src.Service import silkWMSModel
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

    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    filaeposicao_data = []
    for row in resultados:
        filaeposicao_dict = {}
        for i, value in enumerate(row):
            filaeposicao_dict[
                f'col{i + 1}'] = value  # Substitua 'col{i+1}' pelo nome da coluna correspondente, se disponível
        filaeposicao_data.append(filaeposicao_dict)

    return jsonify(filaeposicao_data)


@silkWMS_routes.route('/api/Silk/deleteTelas', methods=['DELETE'])
@token_required
def delete_endpoint():
    endereco = request.args.get('endereco')
    produto = request.args.get('produto')

    # Chama a função Funcao_Deletar para realizar a exclusão
    resultado = silkWMSModel.Funcao_Deletar(endereco, produto)

    if resultado == True:
        return f'endereco: {endereco}, produto {produto}  EXCLUIDOS NO CADASTRO DE SILK', 200
    else:
        return 'Falha ao deletar', 500


@silkWMS_routes.route('/api/Silk/IserirTelas', methods=['PUT'])
@token_required
def insert_endpoint():
    produto = request.args.get('produto')
    endereco = request.args.get('endereco')

    # Chama a função Funcao_Inserir para realizar a inserção
    resultado = silkWMSModel.Funcao_Inserir(produto, endereco)

    if resultado == True:
        return f'produto{produto} endereço{endereco}, Inserção realizada com sucesso', 200
    else:
        return 'Falha ao inserir', 500

