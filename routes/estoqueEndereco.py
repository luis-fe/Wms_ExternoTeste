from models import estoqueEnderecoModel
from flask import Blueprint, jsonify, request
from functools import wraps
import pandas as pd

estoqueEndereco_routes = Blueprint('estoqueEndereco', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@estoqueEndereco_routes.route('/api/DetalhaEndereco', methods=['GET'])
@token_required
def get_DetalhaEndereco():
    # Obtém o código do endereco e a senha dos parâmetros da URL
    Endereco = request.args.get('Endereco')
    empresa = request.args.get('empresa','1')
    natureza = request.args.get('natureza','5')

    Endereco_det = estoqueEnderecoModel.SituacaoEndereco(Endereco, empresa, natureza)
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

@estoqueEndereco_routes.route('/api/DetalhaTag', methods=['GET'])
@token_required
def get_DetalhaTag():
    # Obtém o código do usuário e a senha dos parâmetros da URL
    codbarra = request.args.get('codbarra')
    empresa = request.args.get('empresa','1')
    natureza = request.args.get('natureza','5')
    codbarra, codbarra1 = estoqueEnderecoModel.EndereçoTag(codbarra,empresa,natureza)
    # Obtém os nomes das colunas
    column_names = codbarra1.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in codbarra1.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)