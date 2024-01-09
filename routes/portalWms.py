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

@portal_routes.route('/Embalagens')
def Embalagens():
    return render_template('Embalagens.html')
@portal_routes.route('/CadastroCaixa')
def CadastroCaixa():
    return render_template('TelaQrCodeCaixas.html')

@portal_routes.route('/EnderecoTelaSilk')
def EnderecoTelaSilk():
    return render_template('TelaEnderecosSilk.html')

@portal_routes.route('/Inventarios')
def TelaAcompanhamentoInventário():
    return render_template('TelaAcompanhamentoInventário.html')

@portal_routes.route('/TelaLiberacao')
def TelaLiberacao():
    return render_template('TelaLiberacao.html')

@portal_routes.route('/TelaGrades')
def TelaGrades():
    return render_template('TelaGrades.html')

@portal_routes.route('/HomeGarantia') # Aqui é o link da pagina incial do Portal da Garantia
def HomeGarantia():
    return render_template('indexGarantia.html')

@portal_routes.route('/Login_Teste') # Aqui é o link da pagina incial do Portal da Garantia
def LoginTeste():
    return render_template('Login_Teste.html')

@portal_routes.route('/indexMatriz_Teste') # Aqui é o link da pagina incial do Portal da Garantia
def indexMatriz_Teste():
    return render_template('indexMatriz_Teste.html')

@portal_routes.route('/Usuarios_Teste') # Aqui é o link da pagina incial do Portal da Garantia
def Usuarios_Teste():
    return render_template('Usuarios_Teste.html')

@portal_routes.route('/CadastroDeEnderecos_Teste') # Aqui é o link da pagina incial do Portal da Garantia
def CadastroDeEnderecos_Teste():
    return render_template('CadastoDeEnderecos_Teste.html')

@portal_routes.route('/CadastroQrCaixas_Teste') # Aqui é o link da pagina incial do Portal da Garantia
def CadastroQrCaixas_Teste():
    return render_template('CadastroQrCaixas_Teste.html')

@portal_routes.route('/Chamados_Teste') # Aqui é o link da pagina incial do Portal da Garantia
def Chamados_Teste():
    return render_template('Chamados_Teste.html')

@portal_routes.route('/ConsumoEmbalagens_Teste') # Aqui é o link da pagina incial do Portal da Garantia
def ConsumoEmbalagens_Teste():
    return render_template('ConsumoEmbalagens_Teste.html')

@portal_routes.route('/DistribuicaoDePedidos_Teste') # Aqui é o link da pagina incial do Portal da Garantia
def DistribuicaoDePedidos_Teste():
    return render_template('DistribuicaoDePedidos_Teste.html')

@portal_routes.route('/FilaReposicao_Teste') # Aqui é o link da pagina incial do Portal da Garantia
def FilaReposicao_Teste():
    return render_template('FilaReposicao_Teste.html')

@portal_routes.route('/RelatorioInventario_Teste') # Aqui é o link da pagina incial do Portal da Garantia
def RelatorioInventario_Teste():
    return render_template('RelatorioInventario_Teste.html')
