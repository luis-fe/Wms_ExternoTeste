import pandas as pd

from models import ReposicaoViaOFF
from flask import Blueprint, jsonify, request
from functools import wraps

ReposicaoViaOFF_routes = Blueprint('ReposicaoViaOFF_routes', __name__)


def token_required(f):
    # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@ReposicaoViaOFF_routes.route('/api/consultaTagOFFWMS', methods=['GET'])
@token_required
def get_consultaTagOFFWMS():
    # Obtém os dados do corpo da requisição (JSON)

    codbarrastag = request.args.get('codbarrastag')
    empresa = request.args.get('empresa','1')

    consulta = ReposicaoViaOFF.ReposicaoViaOFF(codbarrastag,'',empresa).consultaTagOFFWMS()
    # Obtém os nomes das colunas
    column_names = consulta.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in consulta.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)


@ReposicaoViaOFF_routes.route('/api/buscarTagCsw', methods=['GET'])
@token_required
def get_buscarTagCsw():
    # Obtém os dados do corpo da requisição (JSON)

    codbarrastag = request.args.get('codbarrastag')
    empresa = request.args.get('empresa','1')

    consulta = ReposicaoViaOFF.ReposicaoViaOFF(codbarrastag,'',empresa).buscarTagCsw()
    # Obtém os nomes das colunas
    column_names = consulta.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in consulta.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)


@ReposicaoViaOFF_routes.route('/api/ReporCaixaLivre', methods=['POST'])
@token_required
def ReporCaixaLivre():
    #try:
        # Obtenha os dados do corpo da requisição
        novo_usuario = request.get_json()
        # Extraia os valores dos campos do novo usuário
        empresa = novo_usuario.get('empresa','1')
        natureza = novo_usuario.get('natureza','5')
        codbarras = novo_usuario.get('codbarras', '5')
        NCaixa = novo_usuario.get('NCaixa', '')
        NCarrinho = novo_usuario.get('NCarrinho', '')

        usuario = novo_usuario.get('usuario', '')
        estornar = novo_usuario.get('estornar', False)



        FilaReposicaoOP = ReposicaoViaOFF.ReposicaoViaOFF(codbarras,NCaixa,empresa,usuario,natureza, estornar,NCarrinho).apontarTagCaixa()
        # Obtém os nomes das colunas
        column_names = FilaReposicaoOP.columns
        # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
        enderecos_data = []
        for index, row in FilaReposicaoOP.iterrows():
            enderecos_dict = {}
            for column_name in column_names:
                enderecos_dict[column_name] = row[column_name]
            enderecos_data.append(enderecos_dict)
        return jsonify(enderecos_data)

@ReposicaoViaOFF_routes.route('/api/ConsultaCaixa', methods=['GET'])
@token_required
def ConsultaCaixa():
    empresa = request.args.get('empresa','1')
    Ncaixa = request.args.get('Ncaixa')

    FilaReposicaoOP = ReposicaoViaOFF.ReposicaoViaOFF('',Ncaixa,empresa).consultaCaixa()
    FilaReposicaoOP = pd.DataFrame(FilaReposicaoOP)
    # Obtém os nomes das colunas
    column_names = FilaReposicaoOP.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    enderecos_data = []
    for index, row in FilaReposicaoOP.iterrows():
        enderecos_dict = {}
        for column_name in column_names:
            enderecos_dict[column_name] = row[column_name]
        enderecos_data.append(enderecos_dict)
    return jsonify(enderecos_data)

@ReposicaoViaOFF_routes.route('/api/qtdCaixaPorCarrinho', methods=['GET'])
@token_required
def get_qtdCaixaPorCarrinho():
    empresa = request.args.get('empresa','1')

    FilaReposicaoOP = ReposicaoViaOFF.ReposicaoViaOFF('','',empresa).qtdCaixaPorCarrinho()
    # Obtém os nomes das colunas
    column_names = FilaReposicaoOP.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    enderecos_data = []
    for index, row in FilaReposicaoOP.iterrows():
        enderecos_dict = {}
        for column_name in column_names:
            enderecos_dict[column_name] = row[column_name]
        enderecos_data.append(enderecos_dict)
    return jsonify(enderecos_data)


@ReposicaoViaOFF_routes.route('/api/consulaDetalharCarrinho', methods=['GET'])
@token_required
def get_consulaDetalharCarrinhoo():
    empresa = request.args.get('empresa','1')
    NCarrinho = request.args.get('NCarrinho','-')

    FilaReposicaoOP = ReposicaoViaOFF.ReposicaoViaOFF('','',empresa,'','','',NCarrinho,'').consulaDetalharCarrinho()
    # Obtém os nomes das colunas
    column_names = FilaReposicaoOP.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    enderecos_data = []
    for index, row in FilaReposicaoOP.iterrows():
        enderecos_dict = {}
        for column_name in column_names:
            enderecos_dict[column_name] = row[column_name]
        enderecos_data.append(enderecos_dict)
    return jsonify(enderecos_data)

@ReposicaoViaOFF_routes.route('/api/liberarCarrinho', methods=['PUT'])
@token_required
def put_liberarCarrinhoo():
    # Obtém os dados do corpo da requisição (JSON)
    datas = request.get_json()
    NCarrinho = datas['NCarrinho']
    empresa = datas['empresa']

    consulta = ReposicaoViaOFF.ReposicaoViaOFF('','',empresa,'','','',NCarrinho,'').liberarCarrinho()

    # Obtém os nomes das colunas
    column_names = consulta.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in consulta.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)

@ReposicaoViaOFF_routes.route('/api/obterOPReduzido', methods=['GET'])
@token_required
def GET_obterOPReduzido():
    empresa = request.args.get('empresa','1')
    codbarrastag = request.args.get('codbarrastag','-')

    FilaReposicaoOP = ReposicaoViaOFF.ReposicaoViaOFF(codbarrastag,'',empresa,'','','','','').obterOPReduzido()
    # Obtém os nomes das colunas
    column_names = FilaReposicaoOP.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    enderecos_data = []
    for index, row in FilaReposicaoOP.iterrows():
        enderecos_dict = {}
        for column_name in column_names:
            enderecos_dict[column_name] = row[column_name]
        enderecos_data.append(enderecos_dict)
    return jsonify(enderecos_data)

@ReposicaoViaOFF_routes.route('/api/consultarTags_OP_rdz', methods=['GET'])
@token_required
def GET_consultarTags_OP_rdz():
    empresa = request.args.get('empresa','1')
    numeroOP = request.args.get('numeroOP','-')
    reduzido = request.args.get('reduzido','-')


    FilaReposicaoOP = ReposicaoViaOFF.ReposicaoViaOFF('','',empresa,'','','','',numeroOP,reduzido).consultarTags_OP_rdz()
    # Obtém os nomes das colunas
    column_names = FilaReposicaoOP.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    enderecos_data = []
    for index, row in FilaReposicaoOP.iterrows():
        enderecos_dict = {}
        for column_name in column_names:
            enderecos_dict[column_name] = row[column_name]
        enderecos_data.append(enderecos_dict)
    return jsonify(enderecos_data)


@ReposicaoViaOFF_routes.route('/api/registrarTagsOFArray', methods=['PUT'])
@token_required
def put_registrarTagsOFArray():
    # Obtém os dados do corpo da requisição (JSON)
    datas = request.get_json()
    arrayTags = datas['arrayTags']
    empresa = datas['empresa']
    Ncarrinho = datas['Ncarrinho']
    usuario = datas['usuario']
    Ncaixa = datas['Ncaixa']


    consulta = ReposicaoViaOFF.ReposicaoViaOFF("", Ncaixa, empresa, usuario, '', '',
                 Ncarrinho,'' ,'').registrarTagsOFArray(arrayTags)

    # Obtém os nomes das colunas
    column_names = consulta.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in consulta.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)