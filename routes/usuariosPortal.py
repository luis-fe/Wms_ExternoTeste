
from flask import Blueprint, jsonify, request
from functools import wraps
from Service import usuariosGarantiaModel
from Service.configuracoes import empresaConfigurada

usuariosPortal_routes = Blueprint('usuariosPortal', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@usuariosPortal_routes.route('/api/UsuariosPortal', methods=['GET'])
@token_required
def UsuariosPortal():
    usuarios = usuariosGarantiaModel.PesquisarUsuariosPortal()
    # Obtém os nomes das colunas
    column_names = ['codigo', 'nome', 'funcao', 'situacao']
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    usuarios_data = []
    for row in usuarios:
        usuario_dict = dict(zip(column_names, row))
        usuarios_data.append(usuario_dict)
    return jsonify(usuarios_data)

@usuariosPortal_routes.route('/api/UsuarioSenhaRestricaoPortal', methods=['GET'])
@token_required
def UsuarioSenhaRestricaoPortal():
    usuarios = usuariosGarantiaModel.PesquisarSenha()

    # Obtém os nomes das colunas
    column_names = ['codigo', 'nome ', 'senha']

    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    usuarios_data = []
    for row in usuarios:
        usuario_dict = dict(zip(column_names, row))
        usuarios_data.append(usuario_dict)

    return jsonify(usuarios_data)
@usuariosPortal_routes.route('/api/UsuariosPortal/<int:codigo>', methods=['POST'])
@token_required
def update_usuarioPortal(codigo):
    # Obtém os dados do corpo da requisição (JSON)
    data = request.get_json()
    # Verifica se a coluna "funcao" está presente nos dados recebidos
    codigo = str(codigo)
    nome_ant, funcao_ant, situacao_ant  = usuariosGarantiaModel.PesquisarUsuariosCodigo(codigo)
    if 'funcao' in data:
        nova_funcao = data['funcao']
    else:
        nova_funcao = funcao_ant
    if 'nome' in data:
        nome_novo = data['nome']
    else:
        nome_novo = nome_ant
    if 'situacao' in data:
        situacao_novo = data['situacao']
    else:
        situacao_novo = situacao_ant
    usuariosGarantiaModel.AtualizarInformacoes(nome_novo, nova_funcao, situacao_novo, codigo)

    return jsonify({'message': f'Dados do Usuário {codigo} - {nome_novo} atualizado com sucesso'})


@usuariosPortal_routes.route('/api/UsuariosPortal', methods=['PUT'])
@token_required
def criar_usuarioPortal():
    # Obtenha os dados do corpo da requisição
    novo_usuario = request.get_json()
    # Extraia os valores dos campos do novo usuário
    codigo = novo_usuario.get('codigo')
    funcao = novo_usuario.get('funcao')
    nome = novo_usuario.get('nome')
    senha = novo_usuario.get('senha')
    situacao = novo_usuario.get('situacao')
    emp = empresaConfigurada.EmpresaEscolhida()
    empresa = novo_usuario.get('empresa',emp)
    # inserir o novo usuário no banco de dados
    c, n, f = usuariosGarantiaModel.PesquisarUsuariosCodigo(codigo)
    if c != 0:
        return jsonify({'message': f'Novo usuário:{codigo}- {nome} ja existe'}), 201
    else:
        usuariosGarantiaModel.InserirUsuario(codigo, funcao, nome, senha, situacao)
        # Retorne uma resposta indicando o sucesso da operação
        return jsonify({'message': f'Novo usuário:{codigo}- {nome} criado com sucesso'}), 201


# Rota com parametros para check do Usuario e Senha
@usuariosPortal_routes.route('/api/UsuarioSenhaPortal', methods=['GET'])
@token_required
def check_user_password():
    # Obtém o código do usuário e a senha dos parâmetros da URL
    codigo = request.args.get('codigo')
    senha = request.args.get('senha')

    # Verifica se o código do usuário e a senha foram fornecidos
    if codigo is None or senha is None:
        return jsonify({'message': 'Código do usuário e senha devem ser fornecidos.'}), 400

    # Consulta no banco de dados para verificar se o usuário e senha correspondem
    result = usuariosGarantiaModel.ConsultaUsuarioSenha(codigo, senha)


    # Verifica se o usuário existe
    if result == 1:
        # Consulta no banco de dados para obter informações adicionais do usuário

        nome, funcao, situacao = usuariosGarantiaModel.PesquisarUsuariosCodigo(codigo)

        # Verifica se foram encontradas informações adicionais do usuário
        if nome != 0:
            usuariosGarantiaModel.RegistroLog(codigo)
            # Retorna as informações adicionais do usuário
            return jsonify({
                "status": True,
                "message": "Usuário e senha VALIDADOS!",
                "nome": nome,
                "funcao": funcao,
                "situacao": situacao            })
        else:
            return jsonify({'message': 'Não foi possível obter informações adicionais do usuário.'}), 500
    else:
        return jsonify({"status": False,
                        "message": f'Usuário ou senha não existe'}), 401