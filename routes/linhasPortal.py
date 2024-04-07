
from flask import Blueprint, jsonify, request
from functools import wraps
from Service import LinhasPortal
from Service.configuracoes import empresaConfigurada
import pandas as pd

linhas_routes = Blueprint('linhasPortal', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@linhas_routes.route('/api/linhasPadrao', methods=['GET'])
@token_required
def linhasPadrao():
    linhas = LinhasPortal.PesquisarLinhaPadrao()
    # Obtém os nomes das colunas

    # Obtém os nomes das colunas
    column_names = linhas.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in linhas.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data), 200

@linhas_routes.route('/api/NomesLinha', methods=['GET'])
@token_required
def NomesLinha():
    linha = request.args.get('linha')
    linhas = LinhasPortal.RetornarNomeLinha(linha)
    # Obtém os nomes das colunas

    # Obtém os nomes das colunas
    column_names = linhas.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in linhas.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data),200

@linhas_routes.route('/api/NovaLinha', methods=['POST'])
@token_required
def NovaLinha():
    data = request.get_json()
    linha = data.get('linha')
    oper1 = data.get('operador1')
    oper2 = data.get('operador2','-')
    oper3 = data.get('operador3','-')
    print('usou a api nova linha ')
    linhas = LinhasPortal.CadastrarLinha(linha,oper1, oper2, oper3)
    # Obtém os nomes das colunas

    # Obtém os nomes das colunas
    column_names = linhas.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in linhas.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data),200

@linhas_routes.route('/api/AtualizarLinha', methods=['PUT'])
@token_required
def AtualizarLinha():
    data = request.get_json()
    linha = data.get('linha')
    oper1 = data.get('operador1')
    oper2 = data.get('operador2','-')
    oper3 = data.get('operador3','-')

    linhas = LinhasPortal.AlterarLinha(linha,oper1, oper2, oper3)
    # Obtém os nomes das colunas

    # Obtém os nomes das colunas
    column_names = linhas.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in linhas.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data),200


@linhas_routes.route('/api/SalvarProdutividadeLinha', methods=['POST'])
@token_required
def SalvarProdutividadeLinha():
    data = request.get_json()
    numeroop = data.get('numeroop')
    oper1 = data.get('operador1')
    oper2 = data.get('operador2','-')
    oper3 = data.get('operador3','-')
    linha = data.get('linha','-')

    qtd = data.get('qtd', 0)

    print(data)
    if qtd == '':
        qtd =0

    linhas = LinhasPortal.ApontarProdutividadeLinha(numeroop,oper1, oper2, oper3, linha, qtd)
    # Obtém os nomes das colunas

    # Obtém os nomes das colunas
    column_names = linhas.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in linhas.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data),200


@linhas_routes.route('/api/ProdutividadeOperadorLinha', methods=['GET'])
@token_required
def ProdutividadeOperadorLinha():
    dataInicio = request.args.get('dataInicio')
    dataFinal = request.args.get('dataFinal','-')
    linhas = LinhasPortal.ProdutividadeOperadorLinha(dataInicio, dataFinal)


    # Obtém os nomes das colunas

    # Obtém os nomes das colunas
    column_names = linhas.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in linhas.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data),200


@linhas_routes.route('/api/OpsProduzidasGarantia', methods=['GET'])
@token_required
def OpsProduzidasGarantia():
    dataInicio = request.args.get('dataInicio')
    dataFinal = request.args.get('dataFinal','-')
    horaInico = request.args.get('horaInico','02:00:00')
    horaFinal = request.args.get('horaFinal','23:00:00')

    linhas = LinhasPortal.OPsProducidasPeriodo(dataInicio, dataFinal, horaInico, horaFinal)


    # Obtém os nomes das colunas

    # Obtém os nomes das colunas
    column_names = linhas.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in linhas.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data),200

@linhas_routes.route('/api/AlterarOPsProduzidasGarantia', methods=['PUT'])
@token_required
def AlterarOPsProduzidasGarantia():

    data = request.get_json()
    linha = data.get('linha','')
    numeroop = data.get('numeroop','')
    oper1 = data.get('operador1','')
    oper2 = data.get('operador2','')
    oper3 = data.get('operador3','')
    linhaNova = data.get('linhaNova','')
    qtd = data.get('qtd','')



    if linha == '' or numeroop =='':
        linhas = pd.DataFrame([{'Mensagem':'Linha ou op nao selecionadas'}])

    else:
        linhas = LinhasPortal.UpdateOP(numeroop, linha, oper1, oper2,oper3,qtd,linhaNova)


    # Obtém os nomes das colunas

    # Obtém os nomes das colunas
    column_names = linhas.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in linhas.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data),200