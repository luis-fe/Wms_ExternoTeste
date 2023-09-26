from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import pandas as pd
import os
from functools import wraps
import PediosApontamento
import Relatorios
import Reposicao
import ReposicaoSku
from routes import routes_blueprint

app = Flask(__name__)
port = int(os.environ.get('PORT', 5000))


app.register_blueprint(routes_blueprint)
#Aqui registo todas as rotas , url's DO PROJETO, para acessar bastar ir na pasta "routes",
#duvidas o contato (62)99351-42-49 ou acessar a documentacao do projeto em:

CORS(app)

# Decorator para verificar o token fixo
def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@app.route('/api/ApontamentoReposicao', methods=['POST'])
@token_required
def get_ApontaReposicao():
    try:
        # Obtenha os dados do corpo da requisição
        data = request.get_json()
        codUsuario = data['codUsuario']
        codbarra = data['codbarra']
        endereco = data['endereco']
        dataHora = data['dataHora']
        estornar = data.get('estornar', False)  # Valor padrão: False, se 'estornar' não estiver presente no corpo
        natureza = data.get('natureza', '5')  # Valor padrão: False, se 'estornar' não estiver presente no corpo
        empresa = data.get('empresa', '1')  # Valor padrão: False, se 'estornar' não estiver presente no corpo



        # Verifica se existe atribuição
        Apontamento = Reposicao.RetornoLocalCodBarras(codUsuario, codbarra, endereco, dataHora, empresa, natureza)

        if Apontamento == 'Reposto':
            if estornar:
                Reposicao.EstornoApontamento(codbarra, empresa, natureza)
                return jsonify({'message': f'codigoBarras {codbarra} estornado!'})

            ender, ender2 = PediosApontamento.EndereçoTag(codbarra)
            return jsonify({'message': f'codigoBarras {codbarra} ja reposto no endereço {ender}'})

        if Apontamento is False:
            return jsonify({'message': False, 'Status': f'codigoBarras {codbarra} nao existe no Estoque'})

        return jsonify({'message': True, 'status': f'Salvo com Sucesso'})

    except KeyError as e:
        return jsonify({'message': 'Erro nos dados enviados.', 'error': str(e)}), 400

    except Exception as e:
        return jsonify({'message': 'Ocorreu um erro interno.', 'error': str(e)}), 500





@app.route('/api/ApontamentoTagPedido', methods=['POST'])
@token_required
def get_ApontamentoTagPedido():
    # Obtém os dados do corpo da requisição (JSON)
    datas = request.get_json()
    codusuario = datas['codUsuario']
    codpedido = datas['codpedido']
    enderecoApi = datas.get('endereço','-')
    codbarras = datas['codbarras']
    dataSeparacao = datas['dataHoraBipágem']
    Estornar = datas.get('estornar', False)  # Valor padrão: False, se 'estornar' não estiver presente no corpo
    print(f' usuario {codusuario} esse pedido: {codpedido}')

    Endereco_det = PediosApontamento.ApontamentoTagPedido(str(codusuario), codpedido, codbarras, dataSeparacao, enderecoApi,
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


@app.route('/api/ApontarTagReduzido', methods=['POST'])
@token_required
def get_ApontarTagReduzido():
    # Obtém os dados do corpo da requisição (JSON)
    datas = request.get_json()

    codusuario = datas['codUsuario']
    dataHora = datas['dataHora']
    endereco = datas['endereço']
    codbarra = datas['codbarras']
    Prosseguir = datas.get('Prosseguir', False)  # Valor padrão: False, se 'estornar' não estiver presente no corpo
    natureza = datas['natureza','5']
    empresa = datas['empresa','1']


    Endereco_det = ReposicaoSku.ApontarTagReduzido(codbarra, endereco, codusuario, 'dataHora', Prosseguir, natureza, empresa)

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


@app.route('/api/RelatorioEndereços', methods=['GET'])
def get_RelatorioEndereços():
    # Obtém os dados do corpo da requisição (JSON)

    Endereco_det = Relatorios.relatorioEndereços()

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


@app.route('/api/RelatorioFila', methods=['GET'])
def get_RelatorioFila():
    # Obtém os dados do corpo da requisição (JSON)
    Endereco_det = Relatorios.relatorioFila()

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

# Defina o diretório onde as imagens serão armazenadas
UPLOAD_FOLDER = 'imagens_chamado'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


@app.route('/api/get_image/<string:idchamado>', methods=['GET'])
def get_image(idchamado):
    directory = f'imagens_chamado/{idchamado}'

    # Verifique se o diretório existe
    if os.path.exists(directory):
        # Listar os arquivos dentro do diretório
        files = os.listdir(directory)

        # Verificar se há arquivos no diretório
        if files:
            # Escolher o primeiro arquivo da lista (você pode ajustar a lógica de escolha conforme necessário)
            filename = files[0]

            # Servir o arquivo escolhido
            return send_from_directory(directory, filename)

    # Se o diretório não existe ou não contém arequivos, retornar uma resposta adequada
    return jsonify({"Mensagem":"Arquivo não encontrado"}), 404


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)