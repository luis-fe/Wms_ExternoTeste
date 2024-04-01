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

    ## consultado a produtividade de carregar endereco
    consulta = pd.read_sql('select usuario, count(datareposicao) as Qtde '
    'from "Reposicao"."Reposicao"."ProducaoCargaEndereco" pce '
    'where datareposicao >= %s and datareposicao <= %s and horario >= %s and horario <= %s ',conn,
                           params=(dataInico,dataFim,horaInicio,horaFim,))

    ## Consultado os usuarios cadastrados
    Usuarios = pd.read_sql('Select codigo as usuario, nome from "Reposicao".cadusuarios ', conn)

    ## Fazendo o merge com os nomes do usuario:
    Usuarios['usuario'] = Usuarios['usuario'].astype(str) # Essas funcao foi utilizada para converter o codigo do usuario para string
    consulta = pd.merge(consulta, Usuarios, on='usuario', how='left')

    conn.close() #Encerrando a conexao do Banco Postgre do WMS

    return consulta


def ProdutividadeGarantiaEquipe(dataInico, dataFim , horaInicio, horaFim):
    conn = ConexaoPostgreMPL.conexao()

    consulta = pd.read_sql('select operador1, operador2, operador3 ,numeroop, qtd  '
                           'from "off"."ProdutividadeGarantiaEquipe1" pce '
                           'where dataapontamento >= %s and dataapontamento <= %s and horario >= %s and horario <= %s ',
                           conn,
                           params=(dataInico, dataFim, horaInicio, horaFim,))

    conn.close()
    consulta['qtd'].fillna(0,inplace=True)
    consulta['qtd'] = consulta['qtd'].astype(int)
    consulta  = consulta.groupby(['operador1','operador2','operador3'])['qtd'].sum().reset_index()




    return consulta

