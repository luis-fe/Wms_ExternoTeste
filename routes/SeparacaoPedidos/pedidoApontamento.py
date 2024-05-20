'''
Rotas Criadas para o Modulo de Separacao de Pedidos do WMS
'''
from flask import Blueprint, jsonify, request
from functools import wraps
from models.SeparacaoPedidos import pedidosApontamentoModels

pedidosApontamento_routes = Blueprint('pedidosApontamentoRoutes', __name__)

####################### TOKEN FIXO PARA ACESSO AO CONTEUDO
def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

####################### API POST para o usuario  Apontar as  Tags dos sku no  Pedido
@pedidosApontamento_routes.route('/api/ApontamentoTagPedido', methods=['POST'])
@token_required
def post_ApontamentoTagPedido():
    # Obtém os dados do corpo da requisição (JSON)
    datas = request.get_json()
    codusuario = datas['codUsuario']
    codpedido = datas['codpedido']
    enderecoApi = datas.get('endereço','-')
    codbarras = datas['codbarras']
    dataSeparacao = datas['dataHoraBipágem']
    Estornar = datas.get('estornar', False)  # Valor padrão: False, se 'estornar' não estiver presente no corpo
    print(f' usuario {codusuario} esse pedido: {codpedido}')

    Endereco_det = pedidosApontamentoModels.ApontamentoTagPedido(str(codusuario), codpedido, codbarras, dataSeparacao, enderecoApi,
                                                          Estornar)

    # Obtém os nomes das colunasok
    column_names = Endereco_det.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in Endereco_det.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)