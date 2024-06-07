## Funcao de Automacao 4 : Nessa etapa é acionada a API do CSW que faz o processo de AtualizaReserva das Sugestoes em aberto de acordo com a politica definida
import gc
import requests
from models.AutomacaoWMS_CSW import controle
from flask import Blueprint, jsonify, request
from functools import wraps
ReservaPreFaturamento_routes = Blueprint('ReservaPreFaturamento', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@ReservaPreFaturamento_routes.route('/api/AtualizaApiReservaFaruamento', methods=['GET'])
@token_required
def AtualizaApiReservaFaruamento():

    IntervaloAutomacao = request.args.get('IntervaloAutomacao',60)

    print('\nETAPA 4 - Atualiza Api ReservaFaruamento')
    client_ip = 'automacao'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, 'AtualizaApiReservaFaruamento')
    limite = int(IntervaloAutomacao) * 60  # (limite de 60 minutos , convertido para segundos)
    if tempo > limite:
        controle.InserindoStatus('AtualizaApiReservaFaruamento', client_ip, datainicio)
        print('ETAPA AtualizaApiReservaFaruamento- Inicio')
        url = 'http://192.168.0.183:8000/pcp/api/ReservaPreFaturamento'

        token = "a44pcp22"


        # Defina os parâmetros em um dicionário

        # Defina os headers
        headers = {
            'accept': 'application/json',
            'Authorization': f'{token}'
        }


        # Faça a requisição POST com parâmetros e headers usando o método requests.post()
        response = requests.get(url,  headers=headers,  verify=False)
        # Verificar se a requisição foi bem-sucedida
        if response.status_code == 200:
            # Converter os dados JSON em um dicionário
            dados_dict = response.json()
            etapa1 = controle.salvarStatus_Etapa1('AtualizaApiReservaFaruamento', 'automacao', datainicio, 'resposta 200 ok')
            controle.salvarStatus('AtualizaApiReservaFaruamento', client_ip, datainicio)
            print('ETAPA AtualizaApiReservaFaruamento- Fim')
            gc.collect()
            del dados_dict
            del etapa1

            return jsonify({"Mensagem": "Atualizado com sucesso", "status": True})

        else:
            print(f'AtualizaApiReservaFaruamento : erro  {response.status_code} ')
            etapa1 = controle.salvarStatus_Etapa1('AtualizaApiReservaFaruamento', 'automacao', datainicio, f'resposta {response.status_code}')
            controle.salvarStatus('AtualizaApiReservaFaruamento', client_ip, datainicio)
            gc.collect()
            del etapa1
            return jsonify({
                "Mensagem": f" EXISTE UMA ATUALIZACAO DA FILA TAGS OFF EM MENOS DE {IntervaloAutomacao} MINUTOS",
                "status": False
            })
