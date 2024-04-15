from Service import endereoModel,imprimirEtiquetaModel
from flask import Blueprint, jsonify, request
from functools import wraps
import pandas as pd


endereco_routes = Blueprint('endereco', __name__)


def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@endereco_routes.route('/api/Enderecos', methods=['GET'])
@token_required
def get_enderecos():
    enderecos = endereoModel.ObeterEnderecos()
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

@endereco_routes.route('/api/NovoEndereco', methods=['PUT'])
@token_required
def criar_enderco():
    # Obtenha os dados do corpo da requisição
    novo_endereco = request.get_json()
    # Extraia os valores dos campos do novo usuário

    rua = novo_endereco.get('rua')
    modulo = novo_endereco.get('modulo')
    posicao = novo_endereco.get('posicao')

    codendereco = endereoModel.CadEndereco(rua, modulo, posicao)

    # inserir o novo usuário no banco de dados
    return jsonify({'message': f'Novo endereco:{codendereco} criado com sucesso'}), 201

@endereco_routes.route('/api/DisponibilidadeEnderecos', methods=['GET'])
@token_required
def get_DisponibilidadeEnderecos():
    # Obtém os dados do corpo da requisição (JSON)
    empresa = request.args.get('empresa','1')
    natureza = request.args.get('natureza','5')
    Endereco_det = endereoModel.EnderecosDisponiveis(natureza, empresa)
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

@endereco_routes.route('/api/endereco/<string:codigoEndereco>', methods=['DELETE'])
@token_required
def delet_Endereco(codigoEndereco):
    # Obtém os dados do corpo da requisição (JSON)
    data = request.get_json()
    # Verifica se a coluna "funcao" está presente nos dados recebidos
    dados = endereoModel.Deletar_Endereco(codigoEndereco)
    # Obtém os nomes das colunas
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in dados.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)

@endereco_routes.route('/api/EnderecoAtacado', methods=['PUT'])
@token_required
def EnderecoAtacado():
    # Obtenha os dados do corpo da requisição
    novo_endereco = request.get_json()
    # Extraia os valores dos campos do novo usuário

    ruaInicial = novo_endereco.get('ruaInicial')
    ruaFinal = novo_endereco.get('ruaFinal')
    modulo = novo_endereco.get('modulo')
    moduloFinal = novo_endereco.get('moduloFinal')
    posicao = novo_endereco.get('posicao')
    posicaoFinal = novo_endereco.get('posicaoFinal')
    tipo = novo_endereco.get('tipo','COLECAO')
    natureza = novo_endereco.get('natureza','5')
    empresa = novo_endereco.get('empresa','1')
    imprimir = novo_endereco.get('imprimir', False)
    enderecoReservado = novo_endereco.get('enderecoReservado', None)

    if enderecoReservado in ['','-']:
        enderecoReservado = ''


    endereoModel.ImportEndereco(ruaInicial, ruaFinal, modulo,moduloFinal, posicao, posicaoFinal, tipo, empresa, natureza, bool(imprimir),enderecoReservado)




    # inserir o novo usuário no banco de dados
    return jsonify({'message': f'Novos enderecos criado com sucesso'}), 200
@endereco_routes.route('/api/EnderecoAtacado', methods=['DELETE'])
@token_required
def EnderecoAtacadoDelatar():
    # Obtenha os dados do corpo da requisição
    novo_endereco = request.get_json()
    # Extraia os valores dos campos do novo usuário

    rua = novo_endereco.get('ruaInicial')
    ruaFinal = novo_endereco.get('ruaFinal')
    modulo = novo_endereco.get('modulo')
    moduloFinal = novo_endereco.get('moduloFinal')
    posicao = novo_endereco.get('posicao')
    posicaoFinal = novo_endereco.get('posicaoFinal')
    tipo = novo_endereco.get('tipo','COLECAO')
    natureza = novo_endereco.get('natureza','5')
    empresa = novo_endereco.get('empresa','1')


    endereoModel.ImportEnderecoDeletar(rua, ruaFinal, modulo,moduloFinal, posicao, posicaoFinal, tipo, empresa, natureza)

    # inserir o novo usuário no banco de dados
    return jsonify({'message': f' enderecos excluidos com sucesso, exceto o que tem saldo !'}), 200

@endereco_routes.route('/api/ObterTipoPrateleira', methods=['GET'])
@token_required
def ObterTipoPrateleira():


    FilaReposicaoOP = endereoModel.ObterTipoPrateleira()
    # Obtém os nomes das colunas
    column_names = FilaReposicaoOP.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    FilaReposicaoOP_data = []
    for index, row in FilaReposicaoOP.iterrows():
        FilaReposicaoOP_dict = {}
        for column_name in column_names:
            FilaReposicaoOP_dict[column_name] = row[column_name]
        FilaReposicaoOP_data.append(FilaReposicaoOP_dict)
    return jsonify(FilaReposicaoOP_data)


@endereco_routes.route('/api/GerarCaixa', methods=['PUT'])
@token_required
def GerarCaixa():
    # Obtenha os dados do corpo da requisição
    novo_endereco = request.get_json()
    # Extraia os valores dos campos do novo usuário

    QuantidadeImprimir = novo_endereco.get('QuantidadeImprimir')
    usuario = novo_endereco.get('usuario','')
    salvaEtiqueta = novo_endereco.get('salvaEtiqueta', False)


    imprimirEtiquetaModel.QuantidadeImprimir(QuantidadeImprimir,usuario,bool(salvaEtiqueta))

    # inserir o novo usuário no banco de dados
    return jsonify({'message': f' ok!'}), 200