import gc
import pandas as pd
from models.AutomacaoWMS_CSW import RecarregaFilaTag, controle, SubstitutosSku
from flask import Blueprint, jsonify, request
from functools import wraps

AtualizaSubstitutosSku_routes = Blueprint('AtualizaSubstitutosSku_routes', __name__)


def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function



@AtualizaSubstitutosSku_routes.route('/api/AtualizaSubstitutosSku', methods=['GET'])
@token_required
def get_AtualizaSubstitutosSku():
    empresa = request.args.get('empresa','1')
    Endereco_det = AtualizarOPSDefeitoTecidos(15)


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

def AtualizarOPSDefeitoTecidos(IntervaloAutomacao):
        print('\n 11- ATUALIZA  OPS Defeito Tecidos')

        rotina = 'OPSDefeitoTecidos'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'OPSDefeitoTecidos')
        limite = IntervaloAutomacao * 60  # (limite de 60 minutos , convertido para segundos)
        if tempo > limite:
            print('\nETAPA Atualizar OPS Defeito Tecidos- Inicio')
            controle.InserindoStatus(rotina, client_ip, datainicio)
            SubstitutosSku.SubstitutosSkuOP(rotina, datainicio)
            controle.salvarStatus('OPSDefeitoTecidos', client_ip, datainicio)
            print('ETAPA Atualizar OPS Defeito Tecidos- Fim')
            gc.collect()
            return pd.DataFrame([{'status':True, 'Mensagem':'Atualizado com sucesso'}])



        else:
            print(f'JA EXISTE UMA ATUALIZACAO das OPS Defeito Tecidos   EM MENOS DE - {IntervaloAutomacao} MINUTOS')
            gc.collect()
            return pd.DataFrame([{'status':True, 'Mensagem':f'JA EXISTE UMA ATUALIZACAO das OPS Defeito Tecidos   EM MENOS DE - {IntervaloAutomacao} MINUTOS'}])
