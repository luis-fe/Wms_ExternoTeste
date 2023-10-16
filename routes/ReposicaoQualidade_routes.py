from Service import ReposicaoQualidade
from flask import Blueprint, jsonify, request
from functools import wraps

reposicao_qualidadeRoute = Blueprint('reposicao_qualidadeRoute', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@reposicao_qualidadeRoute.route('/api/ReporCaixaLivre', methods=['GET'])
@token_required
def ReporCaixaLivre():
    empresa = request.args.get('empresa','1')
    natureza = request.args.get('natureza','5')
    codbarras = request.args.get('codbarras', '5')

    FilaReposicaoOP = ReposicaoQualidade.ApontarTag(codbarras,'1' ,empresa)
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