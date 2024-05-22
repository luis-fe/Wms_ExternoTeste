######## *PROJETO WMS - BackEnd* #############
    
    Nesse Repositório está o Projeto BackEnd do WMS,
    feito para a empresa MPL INDUSTRIA E COMERCIO DE ROUPAS.



######## *Organizacao Do Projeto* ########

    O Projeto foi Desenvolvido utilizando o frameWork FLASK da Linguagem Python.
    Estrutura: 
    Wms_InternoMPL/ (Diretorio Raiz)
    
    app.py (inicializacao do projeto)
    
    CacheDB.jar (Arquivo em Java para conectar JDBC com o Banco IntersystemCache)
    
    ConexaoCSW.py (Arquivo com as instrucoes de acesso ao Csw)

    BuscasSqlCsw.py (Arquivo com as clausuas sql para buscar informacoes do Csw)
    
    ConexaoPostgreMPL.py (Arquivo com intrucoes de acesso ao Postgre)

    Dockerfile (arquivo para construir a Image do Projeto)

    requirements.txt (arquvio contendo as bibliotecas python necessarias para rodar o projeto)

    /routes (Diretorio com as rotas de api RESTFUL)

    /models (Diretorio com a modelagem das classes )

    /colection (contem a colection do projeto para ser utilizada no POSTMAN.

    
######## *Instrucoes Deploy* ########

    2 Opcoes: Via Docker, implantacao Normal.
    
    
    2.2 Implantacao Normal
    
    Passo 1 comando: screen
    Seguido de "espaço" para criar uma screen (que é uma especie de tela congelada que o deixa o codigo rodando) 
    Passo 2 Apontar a pasta do Projeto: cd /home/grupompl/Wms_InternoMPL 
    Passo 3: Iniciando o Serviço : sudo python3 main.py 

    ***Caso já exista alguma screen reservada, basta listar todas as screen ativas
    comando: screen -ls

######## *Politica de Uso* ########

    O Software WMS fica sob posse da Empresa :
    Grupo MPL Industria e Comercio de Roupas, possuindo o código fonte da aplicação e
    os direitos de comercialização e uso.

    
    
