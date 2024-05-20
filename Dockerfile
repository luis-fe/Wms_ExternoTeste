# Use a imagem oficial do Python como base
FROM python:3.9-slim

# Defina o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copie o arquivo de requisitos para o diretório de trabalho
COPY requirements.txt requirements.txt

# Instale as dependências
RUN pip install -r requirements.txt

# Copie o restante do código do aplicativo
COPY . .

# Defina a variável de ambiente para não criar bytecode (.pyc) do Python
ENV PYTHONDONTWRITEBYTECODE=1

# Defina a variável de ambiente para que o output do Python não seja bufferizado
ENV PYTHONUNBUFFERED=1

# Exponha a porta em que a aplicação estará rodando
EXPOSE 5000

# Comando para rodar a aplicação
CMD ["python", "app.py"]
