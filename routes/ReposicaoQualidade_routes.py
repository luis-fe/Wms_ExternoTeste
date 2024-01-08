from Service import ReposicaoQualidade
from flask import Blueprint, jsonify, request
from functools import wraps
import pandas as pd
import subprocess
reposicao_qualidadeRoute = Blueprint('reposicao_qualidadeRoute', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function
def restart_server():
    print("Reiniciando o aplicativo...")
    subprocess.call(["python", "main.py"])
@reposicao_qualidadeRoute.route('/api/ReporCaixaLivre', methods=['POST'])
@token_required
def ReporCaixaLivre():
    try:
        # Obtenha os dados do corpo da requisição
        novo_usuario = request.get_json()
        # Extraia os valores dos campos do novo usuário
        empresa = novo_usuario.get('empresa','1')
        natureza = novo_usuario.get('natureza','5')
        codbarras = novo_usuario.get('codbarras', '5')
        NCaixa = novo_usuario.get('NCaixa', '')
        usuario = novo_usuario.get('usuario', '')
        estornar = novo_usuario.get('estornar', False)


        FilaReposicaoOP = ReposicaoQualidade.ApontarTag(codbarras,NCaixa ,empresa,usuario, natureza, estornar)
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

    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})


@reposicao_qualidadeRoute.route('/api/RecarrearEndereco', methods=['POST'])
@token_required
def RecarrearEndereco():
    # Obtenha os dados do corpo da requisição
    novo_usuario = request.get_json()
    # Extraia os valores dos campos do novo usuário
    Ncaixa = novo_usuario.get('Ncaixa')
    endereco = novo_usuario.get('endereco')
    empresa = novo_usuario.get('empresa','1')

    FilaReposicaoOP = ReposicaoQualidade.EncontrarEPC(Ncaixa, endereco, empresa)
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

@reposicao_qualidadeRoute.route('/api/PesquisarCodbarrastag', methods=['GET'])
@token_required
def PesquisarCodbarrastag():
    empresa = request.args.get('empresa','1')
    codbarras = request.args.get('codbarras','5')

    FilaReposicaoOP = ReposicaoQualidade.PesquisarTagCsw(codbarras, empresa)
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

@reposicao_qualidadeRoute.route('/api/PesquisaOPSKU_tag', methods=['GET'])
@token_required
def PesquisaOPSKU_tag():
    codbarras = request.args.get('codbarras','1')


    FilaReposicaoOP = ReposicaoQualidade.PesquisaOPSKU_tag(codbarras)
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

@reposicao_qualidadeRoute.route('/api/ExcluirCaixa', methods=['DELETE'])
@token_required
def ExcluirCaixa():
    # Obtenha os dados do corpo da requisição
    novo_usuario = request.get_json()
    # Extraia os valores dos campos do novo usuário
    Ncaixa = novo_usuario.get('Ncaixa')

    FilaReposicaoOP = ReposicaoQualidade.ExcluirCaixa(Ncaixa)
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

@reposicao_qualidadeRoute.route('/api/ConsultaCaixa', methods=['GET'])
@token_required
def ConsultaCaixa():
    empresa = request.args.get('empresa','1')
    Ncaixa = request.args.get('Ncaixa')

    FilaReposicaoOP = ReposicaoQualidade.ConsultaCaixa(Ncaixa, empresa)
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

@reposicao_qualidadeRoute.route('/api/CaixasAbertasGeral', methods=['GET'])
@token_required
def CaixasAbertasGeral():
    empresa = request.args.get('empresa','1')


    FilaReposicaoOP = ReposicaoQualidade.CaixasAbertas(empresa)
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

@reposicao_qualidadeRoute.route('/api/CaixasAbertasUsuario', methods=['GET'])
@token_required
def CaixasAbertasUsuario():
    empresa = request.args.get('empresa','1')
    codUsuario = request.args.get('codUsuario')

    FilaReposicaoOP = ReposicaoQualidade.CaixasAbertasUsuario(empresa, codUsuario)
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

@reposicao_qualidadeRoute.route('/api/RelacaoDeOPs', methods=['GET'])
@token_required
def RelacaoDeOPs():
    empresa = request.args.get('empresa','1')


    FilaReposicaoOP = ReposicaoQualidade.OPsAliberar(empresa)
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

@reposicao_qualidadeRoute.route('/api/DetalhaOPQuantidade', methods=['GET'])
@token_required
def DetalhaOPQuantidade():
    try:
        empresa = request.args.get('empresa','1')
        numeroop = request.args.get('numeroop')


        FilaReposicaoOP = ReposicaoQualidade.DetalhaQuantidadeOP(empresa, numeroop)

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


    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})
