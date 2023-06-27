from flask import Flask, render_template, jsonify, request
import pandas as pd
from functools import wraps

app = Flask(__name__)
port = int(os.environ.get('PORT', 5000))

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401
    return decorated_function

@app.route('/')
def home():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)

