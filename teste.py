from flask import Flask, jsonify, request
import jwt
import datetime

app = Flask(__name__)
app.config['SECRET_KEY'] = 'seu_segredo_aqui'  # Chave secreta para assinar o token

# Rota para gerar um token
@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    # Verifica as credenciais (neste exemplo, apenas checa se o username e password estão corretos)
    if data['username'] == 'usuario2' and data['password'] == 'senha2':
        # Gera o token
        token = jwt.encode({'username': data['username'], 'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=30)}, app.config['SECRET_KEY'])
        return jsonify({'token': token})
    else:
        return jsonify({'message': 'Credenciais inválidas'}), 401

# Rota protegida que requer o token
@app.route('/protegido', methods=['GET'])
def protegido():
    token = request.headers.get('Authorization')
    if not token:
        return jsonify({'message': 'Token ausente'}), 401

    try:
        data = jwt.decode(token, app.config['SECRET_KEY'])
        return jsonify({'message': 'Acesso permitido para {}'.format(data['username'])})
    except:
        return jsonify({'message': 'Token inválido'}), 401

if __name__ == '__main__':
    app.run(debug=True)
