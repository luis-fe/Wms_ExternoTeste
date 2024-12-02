'''
        Nesse arquivo .py é disponibilizado as Rotas de API para todos os processos envolvendo o modulo ReposicaoOFF
'''


import models.configuracoes.empresaConfigurada
import models.configuracoes.SkusSubstitutos
from models import ReposicaoQualidade, controle, Reposicao, Endereco, ReposicaoViaOFF, Configuracoes, Caixa
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
    subprocess.call(["python", "app.py"])




'''Rota da api para recarregar Endereco'''
@reposicao_qualidadeRoute.route('/api/RecarrearEndereco', methods=['POST'])
@token_required
def RecarrearEnderecoTeste():

        dados = request.get_json()
        Ncaixa = dados['Ncaixa']
        endereco = dados['endereco']
        usuario = dados.get('usuario','-')
        empresa = dados.get('empresa','1')


        # 1 - Instanciando o objeto Reposicao
        repor = Reposicao.Reposicao('',endereco,empresa,usuario,'',Ncaixa)


        # 2 : Valida se o Endereco existe no WMS
        StatusEndereco = Endereco.Endereco(repor.endereco,repor.empresa).validaEndereco()

        # 2.1 - Caso Nao existir NO WMS:
        if StatusEndereco['status'][0] == False:

            Retorno = StatusEndereco#'Mensagem':f'Erro! O endereco {endereco} nao esta cadastrado, contate o supervisor.

            column_names = Retorno.columns # Obtém os nomes das colunas
            enderecos_data = []
            for index, row in Retorno.iterrows():
                enderecos_dict = {}
                for column_name in column_names:
                    enderecos_dict[column_name] = row[column_name]
                enderecos_data.append(enderecos_dict)
            return jsonify(enderecos_data)


        # 2.2 - Caso Exista no WMS:
        else:
            # Estapa 2 : Extrai Informacos da caixa
            InfoCaixa = ReposicaoViaOFF.ReposicaoViaOFF('',repor.Ncaixa,repor.empresa).informcaoCaixaDetalhado()
            # Retorno: NumeroCaixa, codbarras, codreduzido, engenharia, descricao, Knatureza, emoresa, cor , tamanho , OP , usuario , DataReposicao, restricao

            reduzido = InfoCaixa['codreduzido'][0]
            # Cria uma variavel chamada reduzido com a informacao do codigo Reduzido (tambem chamado SKU):

            # Etapa 3 :Avalia se no endereco que o usario esta tentando repor esta vazio:
            StatusEnderecoOculpacao = repor.avalicaoOcupacaoEndereco()
            #Retorno : status: False - significa que está cheio , else : está Vazio

            #3.1 - Caso o endereco estiver cheio e o "codigo reduzido" for diferente ao do "codigo reduzido da Caixa"  - Nao permite a operacao
            if StatusEnderecoOculpacao['status'][0] == False and \
                    reduzido != StatusEnderecoOculpacao['codreduzido'][0]:

                Retorno = StatusEnderecoOculpacao
                Retorno.drop('codreduzido', axis=1, inplace=True)

                # Faz o registro da inconsistencia:
                repor.detalhaErro = 'endereco está cheio com outro sku'
                repor.put_registroPendenciasRecarregarEndereco()

                # Obtém os nomes das colunas
                column_names = Retorno.columns
                # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
                enderecos_data = []
                for index, row in Retorno.iterrows():
                    enderecos_dict = {}
                    for column_name in column_names:
                        enderecos_dict[column_name] = row[column_name]
                    enderecos_data.append(enderecos_dict)
                return jsonify(enderecos_data)


            # Etapa 4 : Caso o endereco estiver vazio ou o sku for o mesmo que está no endereco,
            #### o processo  continua e a proxima validacao é se a OP está baixada
            else:

                codigoOP = InfoCaixa['numeroop'][0] #retira a informacao do Numero da OP para consultar se a mesma foi baixada
                StatusOP = repor.validarSituacaoTags(InfoCaixa['codbarrastag'])



                # 4.1 Caso a OP ainda nao estiver baixada:
                if StatusOP['status'][0] == False:

                    Retorno = StatusOP #'Mesagem':f'Erro! A OP {numeroOP} da caixa ainda nao foi encerrada'
                    column_names = Retorno.columns  # Obtém os nomes das colunas
                    enderecos_data = []   # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes

                    for index, row in Retorno.iterrows():
                        enderecos_dict = {}
                        for column_name in column_names:
                            enderecos_dict[column_name] = row[column_name]
                        enderecos_data.append(enderecos_dict)
                    return jsonify(enderecos_data) # Devolve a resposta no Json: #'Mesagem':f'Erro! A OP {numeroOP} da caixa ainda nao foi encerrada'

                # 5: Caso a caixa "passe" por todas as validacoes e estiver autorizada a recarregar
                else:
                    #Verifica se o WMS esá configurado para tratar restricao especial:
                    configuracaoRestricao = Configuracoes.ConfiguracoesGerais(repor.empresa).consultaRegraDeEnderecoParaSubstituto()
                    #Retorno implenta_endereco_subs: sim ou nao


                    # 5.1 Restricao de endereco Especial

                    if configuracaoRestricao == 'sim' and InfoCaixa['restricao'][0] != '-' and InfoCaixa['restricao'][0] != 'veio csw':
                        print('etapa 5.1 Restricao de endereco Especial')

                        # 5.1.1 Verifica o endereco escolhido e notifica caso esse endereco estiver cheio, para nao misturar as OPs
                        # 5.1.2 verifica se o numero da OP que esta no endereco é o mesmo da Op que esta sendo reposta

                        if StatusEnderecoOculpacao['status'][0] == False \
                                and InfoCaixa['numeroop'][0] != repor.verificarOpsEndereco():

                            # Registra a incroguencia
                            repor.detalheErro = 'tentou incluir tag com restricao de substiuto misturado em outras'
                            repor.put_registroPendenciasRecarregarEndereco()


                            Retorno = pd.DataFrame([{'status': False,
                                'Mensagem':f'Erro! os itens a ser reposto sao substitutos e o endereco{repor.endereco} está cheio com outras OPs !'}])  # 'Mesagem'




                            column_names = Retorno.columns  # Obtém os nomes das colunas
                            enderecos_data = []  # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes

                            for index, row in Retorno.iterrows():
                                enderecos_dict = {}
                                for column_name in column_names:
                                    enderecos_dict[column_name] = row[column_name]
                                enderecos_data.append(enderecos_dict)
                            return jsonify(
                                enderecos_data)  # Devolve a resposta no Json com a mensagem de erro


                        # 5.1.2 Caso o endereco sugerido for igual ao informado pelo usuario
                        else:
                            repor.registrarTagsNoEndereco(InfoCaixa)

                            # Obtém os nomes das colunas

                            Retorno = pd.DataFrame([{'status': True, 'Mensagem': 'Endereco carregado com sucesso!'}])
                            column_names = Retorno.columns
                            # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
                            enderecos_data = []
                            for index, row in Retorno.iterrows():
                                enderecos_dict = {}
                                for column_name in column_names:
                                    enderecos_dict[column_name] = row[column_name]
                                enderecos_data.append(enderecos_dict)
                            return jsonify(enderecos_data)

                    # 5.2 Caso nao tiver  Restricao de endereco Especial
                    else:
                        repor.registrarTagsNoEndereco(InfoCaixa)

                        # Obtém os nomes das colunas

                        Retorno = pd.DataFrame([{'status': True, 'Mensagem': 'Endereco carregado com sucesso!'}])
                        column_names = Retorno.columns
                        # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
                        enderecos_data = []
                        for index, row in Retorno.iterrows():
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

@reposicao_qualidadeRoute.route('/api/LimparCaixa', methods=['DELETE'])
@token_required
def LimparCaixa():
    caixa = request.args.get('caixa','5')

    FilaReposicaoOP = ReposicaoQualidade.LimparCaixa(caixa)
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



@reposicao_qualidadeRoute.route('/api/CaixasAbertasGeral', methods=['GET'])
@token_required
def CaixasAbertasGeral():
    empresa = request.args.get('empresa','1')


    FilaReposicaoOP = Caixa.CaixaOFF('',empresa).caixasAbertas()
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

# API para consultar as CaixasEmAberto por usuario
@reposicao_qualidadeRoute.route('/api/CaixasAbertasUsuario', methods=['GET'])
@token_required
def CaixasAbertasUsuario():
    try:
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
    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro em CaixaAbertaUsuario."})


@reposicao_qualidadeRoute.route('/api/RelacaoDeOPs', methods=['GET'])
@token_required
def RelacaoDeOPs():
    empresa = request.args.get('empresa','1')

    datainicio = controle.obterHoraAtual()
    client_ip = request.remote_addr
    FilaReposicaoOP = ReposicaoQualidade.OPsAliberar(empresa)
    controle.salvar('OPsAliberar', client_ip, datainicio)

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
        emp = models.configuracoes.empresaConfigurada.EmpresaEscolhida()
        empresa = request.args.get('empresa',emp)
        numeroop = request.args.get('numeroop')

        datainicio = controle.obterHoraAtual()
        client_ip = request.remote_addr
        controle.salvar('DetalhaQuantidadeOP', client_ip, datainicio)
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
