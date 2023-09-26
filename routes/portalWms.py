from flask import Blueprint, jsonify, request,render_template
from functools import wraps

portal_routes = Blueprint('portal', __name__)


def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

# Rota pagina inicial
@portal_routes.route('/')
def login():
    return render_template('Login.html')
@portal_routes.route('/Distribuicao')
def Distribuicao():
    return render_template('index.html')

@portal_routes.route('/Usuarios')
def Usuarios():
    return render_template('Usuarios.html')

@portal_routes.route('/Produtividade')
def Produtividade():
    return render_template('Produtividade.html')

@portal_routes.route('/Enderecos')
def Enderecos():
    return render_template('TelaEnderecos.html')
@portal_routes.route('/home')
def home():
    return render_template('TelaPrincipal.html')
@portal_routes.route('/Reposicao')
def Reposicao():
    return render_template('TelaFilaReposicao.html')
@portal_routes.route('/Chamados')
def Chamados():
    return render_template('TelaChamados.html')