from Service import necessidadeReposicaoModel, controle
from Service.AutomacaoWMS_CSW import RecarregaFilaTag, ReservaEnderecos, RecarregarPedidosCSWModel, AtualizarFilaGarantia
from flask import Blueprint, jsonify, request
from functools import wraps
import pandas as pd
from Service.configuracoes import empresaConfigurada





AutomacaoWMS_CSW_routes = Blueprint('AutomacaoWMS_CSW', __name__)




def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@AutomacaoWMS_CSW_routes.route('/api/RecarregarCodBarras', methods=['GET'])
@token_required
def RecarregarCodBarras():
    # Obtém os valores dos parâmetros DataInicial e DataFinal, se estiverem presentes na requisição
    codBarrasTag = request.args.get('codBarrasTag')

    #Relatorios.RelatorioSeparadoresLimite(10)
    TagReposicao = RecarregaFilaTag.RecarregarTagFila(codBarrasTag)



    # Obtém os nomes das colunas
    column_names = TagReposicao.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in TagReposicao.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)

@AutomacaoWMS_CSW_routes.route('/api/EstornarReservasEnderecos', methods=['PUT'])
@token_required
def EstornarReservasEnderecos():


    TagReposicao = ReservaEnderecos.EstornarReservasNaoAtribuidas()



    # Obtém os nomes das colunas
    column_names = TagReposicao.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in TagReposicao.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)

@AutomacaoWMS_CSW_routes.route('/api/AtribuirReservaPedido', methods=['GET'])
@token_required
def AtribuirReservaPedido():
    codpedido = request.args.get('codpedido')
    natureza = request.args.get('natureza')

    TagReposicao = ReservaEnderecos.AtribuirReserva(codpedido, natureza)



    # Obtém os nomes das colunas
    column_names = TagReposicao.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in TagReposicao.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)

@AutomacaoWMS_CSW_routes.route('/api/ReservaEndenrecos', methods=['GET'])
@token_required
def ReservaEndenrecos():
    empresa = request.args.get('empresa','1')
    natureza = request.args.get('natureza','5')
    consideraSobra = request.args.get('consideraSobra',False)
    ordem = request.args.get('ordem', 'asc')
    repeticao = request.args.get('repeticao', 12)

    TagReposicao = ReservaEnderecos.ReservaPedidosNaoRepostos(empresa,natureza,bool(consideraSobra), ordem,int(repeticao))
    TagReposicao2 = ReservaEnderecos.ReservaPedidosNaoRepostos(empresa,natureza,bool(consideraSobra),ordem,int(repeticao))



    # Obtém os nomes das colunas
    column_names = TagReposicao.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in TagReposicao.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)

# Nessa Api ocorre a recarcarga dos pedidos
@AutomacaoWMS_CSW_routes.route('/api/RecarregarPedidos', methods=['GET'])
@token_required
def RecarregarPedidos():
    emp = empresaConfigurada.EmpresaEscolhida()
    empresa = request.args.get('empresa',emp)
    natureza = request.args.get('natureza','5')
    consideraSobra = request.args.get('consideraSobra',False)
    ordem = request.args.get('ordem', 'asc')
    repeticao = request.args.get('repeticao', 12)

    TagReposicao = RecarregarPedidosCSWModel.RecarregarPedidos(empresa)
    ReservaEnderecos.ReservaPedidosNaoRepostos(empresa,natureza,bool(consideraSobra),ordem,int(5))
    ReservaEnderecos.ReservaPedidosNaoRepostos(empresa,natureza,bool(True),'desc',int(5))
    necessidadeReposicaoModel.RelatorioNecessidadeReposicaoDisponivel(empresa, natureza)






    # Obtém os nomes das colunas
    column_names = TagReposicao.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in TagReposicao.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)

@AutomacaoWMS_CSW_routes.route('/api/ExclusaoPedidosFat', methods=['DELETE'])
@token_required
def ExclusaoPedidosFat():
    empresa = request.args.get('empresa','1')


    TagReposicao = RecarregarPedidosCSWModel.ExcuindoPedidosNaoEncontrados(empresa)
    TagReposicao = pd.DataFrame([{'Mensagem':f'{TagReposicao} pedidos foram deletados pois foram faturados'}])



    # Obtém os nomes das colunas
    column_names = TagReposicao.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in TagReposicao.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)

    return jsonify(pedidos_data)

@AutomacaoWMS_CSW_routes.route('/api/atualizaStatusRetorna', methods=['PUT'])
@token_required
def atualizaStatusRetorna():
    empresa = request.args.get('empresa','1')


    TagReposicao = RecarregarPedidosCSWModel.Verificando_RetornaxConferido(empresa)

    TagReposicao = pd.DataFrame([{'Mensagem':f'{TagReposicao} foram atualizados para status NO RETORNA'}])



    # Obtém os nomes das colunas
    column_names = TagReposicao.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in TagReposicao.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)

    return jsonify(pedidos_data)

@AutomacaoWMS_CSW_routes.route('/api/DetalhaSkuPedido', methods=['GET'])
@token_required
def DetalhaSkuPedido():
    empresa = request.args.get('empresa','1')
    pedido = request.args.get('pedido', '1')

    TagReposicao = RecarregarPedidosCSWModel.DetalhandoPedidoSku(empresa, pedido)




    # Obtém os nomes das colunas
    column_names = TagReposicao.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in TagReposicao.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)

    return jsonify(pedidos_data)


# Rota utilizada para atualizar a fila de tags disponiveis na reposicao off
@AutomacaoWMS_CSW_routes.route('/api/AtualizacaoFilaOFF', methods=['GET'])
@token_required
def AtualizacaoFilaOFF():
    client_ip = request.remote_addr
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio,'atualiza tag off')
    limite = 15 * 60 #(limite de 15 minutos , convertido para segundos)

    op = request.args.get('op', '-')

    if op == '-':
        if tempo > limite:
            TagReposicao = AtualizarFilaGarantia.AtualizaFilaGarantia()
            controle.salvar('atualiza tag off',client_ip,datainicio)
        else:
            TagReposicao = pd.DataFrame([{'Status': True, 'mensagem':'tags atualizadas na Garantia com sucesso, atualizacao ta congelada  !'}])

    else:
        TagReposicao = AtualizarFilaGarantia.AtualizacaoFilaOFF_op(op)




    # Obtém os nomes das colunas
    column_names = TagReposicao.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in TagReposicao.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)

    return jsonify(pedidos_data)


