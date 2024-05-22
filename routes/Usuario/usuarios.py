##### Nesse arquivo é fornecido a Api das operacoes envolvendo o login e cadastro de usuarios do WMS
from flask import Blueprint, jsonify, request
from functools import wraps
from models.Usuario import usuariosModel
from models.configuracoes import empresaConfigurada

usuarios_routes = Blueprint('usuarios', __name__) # Esse é o nome atribuido para o conjunto de rotas envolvendo usuario


# TOKEN FIXO PARA ACESSO AO CONTEUDO
def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


# URL para Informar todos os usuarios cadastrados no WMS
@usuarios_routes.route('/api/Usuarios', methods=['GET'])
@token_required
def get_usuarios():
    usuarios = usuariosModel.PesquisarUsuarios()
    # Obtém os nomes das colunas
    column_names = ['codigo', 'nome', 'funcao', 'situacao']
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    usuarios_data = []
    for row in usuarios:
        usuario_dict = dict(zip(column_names, row))
        usuarios_data.append(usuario_dict)
    return jsonify(usuarios_data)


@usuarios_routes.route('/api/UsuarioSenhaRestricao', methods=['GET'])
@token_required
def get_usuariosRestricao():
    usuarios = usuariosModel.PesquisarSenha()

    # Obtém os nomes das colunas
    column_names = ['codigo', 'nome ', 'senha']

    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    usuarios_data = []
    for row in usuarios:
        usuario_dict = dict(zip(column_names, row))
        usuarios_data.append(usuario_dict)

    return jsonify(usuarios_data)
@usuarios_routes.route('/api/Usuarios/<int:codigo>', methods=['POST'])
@token_required
def update_usuario(codigo):
    # Obtém os dados do corpo da requisição (JSON)
    data = request.get_json()
    # Verifica se a coluna "funcao" está presente nos dados recebidos
    nome_ant, funcao_ant, situacao_ant , empresa_ant = usuariosModel.PesquisarUsuariosCodigo(codigo)
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
    usuariosModel.AtualizarInformacoes(nome_novo, nova_funcao, situacao_novo, codigo)

    return jsonify({'message': f'Dados do Usuário {codigo} - {nome_novo} atualizado com sucesso'})


@usuarios_routes.route('/api/Usuarios', methods=['PUT'])
@token_required
def criar_usuario():
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
    c, n, f, g = usuariosModel.PesquisarUsuariosCodigo(codigo)
    if c != 0:
        return jsonify({'message': f'Novo usuário:{codigo}- {nome} ja existe'}), 201
    else:
        usuariosModel.InserirUsuario(codigo, funcao, nome, senha, situacao, empresa)
        # Retorne uma resposta indicando o sucesso da operação
        return jsonify({'message': f'Novo usuário:{codigo}- {nome} criado com sucesso'}), 201


# Rota com parametros para check do Usuario e Senha
@usuarios_routes.route('/api/UsuarioSenha', methods=['GET'])
@token_required
def check_user_password():
    # Obtém o código do usuário e a senha dos parâmetros da URL
    codigo = request.args.get('codigo')
    senha = request.args.get('senha')

    # Verifica se o código do usuário e a senha foram fornecidos
    if codigo is None or senha is None:
        return jsonify({'message': 'Código do usuário e senha devem ser fornecidos.'}), 400

    # Consulta no banco de dados para verificar se o usuário e senha correspondem
    result = usuariosModel.ConsultaUsuarioSenha(codigo, senha)


    # Verifica se o usuário existe
    if result == 1:
        # Consulta no banco de dados para obter informações adicionais do usuário

        nome, funcao, situacao, empresa1 = usuariosModel.PesquisarUsuariosCodigo(codigo)

        # Verifica se foram encontradas informações adicionais do usuário
        if nome != 0:
            usuariosModel.RegistroLog(codigo)
            # Retorna as informações adicionais do usuário
            return jsonify({
                "status": True,
                "message": "Usuário e senha VALIDADOS!",
                "nome": nome,
                "funcao": funcao,
                "situacao": situacao,
                "empresa":empresa1
            })
        else:
            return jsonify({'message': 'Não foi possível obter informações adicionais do usuário.'}), 500
    else:
        return jsonify({"status": False,
                        "message": f'Usuário ou senha não existe'}), 401