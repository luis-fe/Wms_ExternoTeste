##### Nesse Arquivo é feito as regras para exibir as Produtividades de cada processo que compoe o WMS.
import pytz
import ConexaoPostgreMPL
import pandas as pd
import locale
import datetime


# O Passo 1 : Obter funcao que retorna data e hora do sistema
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    return hora_str

# Produtividade Colaboradores do processo Recarregar Endereco
def ProdutividadeCarregarEndereco(dataInico, dataFim , horaInicio, horaFim):

    conn = ConexaoPostgreMPL.conexao() # Abrindo a conexao do Banco Postgre do WMS

    conn.close() #Encerrando a conexao do Banco Postgre do WMS
