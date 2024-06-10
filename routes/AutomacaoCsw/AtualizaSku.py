import gc
from flask import Blueprint, jsonify, request
from functools import wraps
from models.AutomacaoWMS_CSW  import controle, AtualizaSku, PedidosPCP
from colorama import Fore
import psutil
import os


InformacosPCPServicos_routes = Blueprint('InformacoesPCP', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function
def memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss  # Retorna o uso de memória em bytes

@InformacosPCPServicos_routes.route('/api/AtualizarSKU', methods=['GET'])
@token_required
def AtualizarSKU():
    IntervaloAutomacao = request.args.get('IntervaloAutomacao', '1')
    empresa = request.args.get('empresa','1')

    cpu_percent = psutil.cpu_percent()
    memoria_antes = memory_usage()

    print(memoria_antes)
    print("Uso da CPU inicio do processo:", cpu_percent, "%")
    print(Fore.LIGHTYELLOW_EX+f'\nETAPA 1 - ATUALIZACAO DO AutomacaoCadastroSKU uso atual da cpu {cpu_percent}%')
    client_ip = 'automacao'
    rotina  = 'AtualizarSKU'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'AtualizarSKU')
    limite = int(IntervaloAutomacao) * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:
            print(f'\nETAPA {rotina}- Inicio: {controle.obterHoraAtual()}')
            controle.InserindoStatus(rotina,client_ip,datainicio)
            cpu_percent = psutil.cpu_percent()
            print("Uso da CPU:", cpu_percent, "%")
            AtualizaSku.CadastroSKU(rotina,datainicio)
            controle.salvarStatus(rotina,client_ip,datainicio)
            cpu_percent = psutil.cpu_percent()
            print("Uso da CPU final do processo:", cpu_percent, "%")
            print(f'ETAPA {rotina}- Fim : {controle.obterHoraAtual()}')
            gc.collect()
            memoria_depois = memory_usage()
            print(memoria_depois)
            return jsonify({"Mensagem": "Atualizado com sucesso", "status": True})


    else:
            return jsonify({
                "Mensagem": f" EXISTE UMA ATUALIZACAO {rotina}  EM MENOS DE {IntervaloAutomacao} MINUTOS",
                "status": False
            })

@InformacosPCPServicos_routes.route('/api/AtualizarPedidosPCP', methods=['GET'])
@token_required
def AtualizarPedidosPCP():
    IntervaloAutomacao = request.args.get('IntervaloAutomacao', '1')
    empresa = request.args.get('empresa','1')

    cpu_percent = psutil.cpu_percent()
    memoria_antes = memory_usage()

    print(memoria_antes)
    print("Uso da CPU inicio do processo:", cpu_percent, "%")
    print(Fore.LIGHTYELLOW_EX+f'\nETAPA 1 - ATUALIZACAO DO AutomacaoCadastroSKU uso atual da cpu {cpu_percent}%')
    client_ip = 'automacao'
    rotina  = 'AtualizarPedidosPCP'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'AtualizarSKU')
    limite = int(IntervaloAutomacao) * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:
            print(f'\nETAPA AtualizarPedidos- Inicio: {controle.obterHoraAtual()}')
            controle.InserindoStatus('pedidosItemgrade',client_ip,datainicio)
            PedidosPCP.IncrementarPedidos(rotina, datainicio)
            controle.salvarStatus('pedidosItemgrade', client_ip, datainicio)
            print(f'ETAPA AtualizarPedidos- FIM: {controle.obterHoraAtual()}')
            gc.collect()
            memoria_depois = memory_usage()
            print(memoria_depois)
            return jsonify({"Mensagem": "Atualizado com sucesso", "status": True})

    else:
            return jsonify({
                "Mensagem": f" EXISTE UMA ATUALIZACAO {rotina}  EM MENOS DE {IntervaloAutomacao} MINUTOS",
                "status": False
            })