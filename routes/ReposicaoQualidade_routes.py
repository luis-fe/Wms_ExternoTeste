import Service.configuracoes.empresaConfigurada
from Service import ReposicaoQualidade, controle
from Service.Processo_Reposicao_OFF import RecarregarEndereco, ApontarTag
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
        emp = Service.configuracoes.empresaConfigurada.EmpresaEscolhida()
        # Obtenha os dados do corpo da requisição
        novo_usuario = request.get_json()
        # Extraia os valores dos campos do novo usuário
        empresa = novo_usuario.get('empresa',emp)
        natureza = novo_usuario.get('natureza','5')
        codbarras = novo_usuario.get('codbarras', '5')
        NCaixa = novo_usuario.get('NCaixa', '')
        usuario = novo_usuario.get('usuario', '')
        estornar = novo_usuario.get('estornar', False)


        FilaReposicaoOP = Service.Processo_Reposicao_OFF.ApontarTag.ApontarTagCaixa(codbarras,NCaixa ,empresa,usuario, natureza, estornar)
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



#### Essa api RecarrearEndereco utiliza como referencia o arquivo Service.Processo_Reposicao_OFF.RecarregarEndereco deste projeto.
#### Repeti na funcao algums processos para um melhor acoplamento.
@reposicao_qualidadeRoute.route('/api/RecarrearEndereco', methods=['POST'])
@token_required
def RecarrearEnderecoTeste():

        dados = request.get_json()# Obtenha os dados do corpo da requisição
        Ncaixa = dados['Ncaixa']# Extraia os valores dos campos do novo usuário
        endereco = dados['endereco']
        usuario = dados.get('usuario','-')
        datainicio = controle.obterHoraAtual()

        # Funcao de contingencia para casos  que derem errado:
        RecarregarEndereco.UpdateEnderecoCAixa(Ncaixa,endereco,'ReposicaoIniciada')
        print(f'caixa da reposicao {Ncaixa}')

        # Etapa 1 : Valida se o Endereco existe
        StatusEndereco = RecarregarEndereco.ValidaEndereco(endereco)


        if StatusEndereco['status'][0] == False:

            Retorno = StatusEndereco

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
        else:


            # Estapa 2 : Extrai Informacos da caixa
            InfoCaixa = RecarregarEndereco.InfoCaixa(Ncaixa)


            reduzido = InfoCaixa['codreduzido'][0]

        # Etapa 3 :Avalia se no endereco a repor esta vazio:
            StatusEnderecoOculpacao = RecarregarEndereco.EnderecoOculpado(endereco)

            if StatusEnderecoOculpacao['status'][0] == False and reduzido != StatusEnderecoOculpacao['codreduzido'][0]:
                Retorno = StatusEnderecoOculpacao
                Retorno.drop('codreduzido', axis=1, inplace=True)

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
            # Etapa 4 : Caso o endereco estiver vazio, o processo irá continuar e a proxima validacao é se a OP está baixada
            else:
                codigoOP = InfoCaixa['numeroop'][0]
                client_ip = request.remote_addr

                StatusOP = RecarregarEndereco.ValidarSituacaoOPCSW(codigoOP)
                controle.salvar('ValidarSituacaoOPCSW', client_ip, datainicio)

                if StatusOP['status'][0] == False:

                    Retorno = StatusOP
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
                else:
                    epc = RecarregarEndereco.EPC_CSW_OP(InfoCaixa)
                    RecarregarEndereco.IncrementarCaixa(endereco,epc, usuario)
                    ## Limpeza retirada ate achar o erro
                    #RecarregarEndereco.LimpandoDuplicidadeFilaOFF()

                    # Obtém os nomes das colunas

                    Retorno = pd.DataFrame([{'status':True,'Mensagem':'Endereco carregado com sucesso!'}])
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
        emp = Service.configuracoes.empresaConfigurada.EmpresaEscolhida()
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
