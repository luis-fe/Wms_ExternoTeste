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

    consulta = pd.read_sql('select operador1, operador2, operador3 ,numeroop, qtd, linha  '
                           'from "off"."ProdutividadeGarantiaEquipe1" pce '
                           'where dataapontamento >= %s and dataapontamento <= %s and horario >= %s and horario <= %s ',
                           conn,
                           params=(dataInico, dataFim, horaInicio, horaFim,))

    conn.close()
    consulta['qtd'].fillna(0,inplace=True)
    consulta['qtd OP'] = 1
    consulta['qtd'] = consulta['qtd'].astype(float)
    consulta['linha'].fillna('-',inplace=True)
    consulta['linha'] = consulta['linha'].str.replace(' ','-')

    consulta  = consulta.groupby(['operador1','operador2','operador3','linha'])['qtd','qtd OP'].sum().reset_index()
    consulta = consulta.sort_values(by='qtd', ascending=False,
                                ignore_index=True)

    consulta['operador1'] = consulta['operador1'].str.split(' ').str.get(0)
    consulta['operador2'] = consulta['operador2'].str.split(' ').str.get(0)
    consulta['operador3'] = consulta['operador3'].str.split(' ').str.get(0)
    consulta['linha'] = consulta['linha'].str.split(' ').str.get(0)

    consulta['Media'] = consulta['qtd']/consulta['qtd OP']

    totalP = consulta['qtd'].sum()
    qtdOP = consulta['qtd OP'].sum()

    data = { '1.0- Total Peças': totalP,
             '1.1- Qtd OPs': qtdOP,
        '2.0- Detalhamento': consulta.to_dict(orient='records')}

    return [data]

def ProdutividadeGarantiaIndividual(dataInico, dataFim , horaInicio, horaFim):
    conn = ConexaoPostgreMPL.conexao()

    consulta1 = pd.read_sql('select operador1 as operador,  qtd  '
                           'from "off"."ProdutividadeGarantiaEquipe1" pce '
                           'where dataapontamento >= %s and dataapontamento <= %s and horario >= %s and horario <= %s ',
                           conn,
                           params=(dataInico, dataFim, horaInicio, horaFim,))
    consulta2 = pd.read_sql('select operador2 as operador,  qtd  '
                           'from "off"."ProdutividadeGarantiaEquipe1" pce '
                           'where dataapontamento >= %s and dataapontamento <= %s and horario >= %s and horario <= %s ',
                           conn,
                           params=(dataInico, dataFim, horaInicio, horaFim,))
    consulta3 = pd.read_sql('select operador3 as operador,  qtd  '
                           'from "off"."ProdutividadeGarantiaEquipe1" pce '
                           'where dataapontamento >= %s and dataapontamento <= %s and horario >= %s and horario <= %s ',
                           conn,
                           params=(dataInico, dataFim, horaInicio, horaFim,))

    conn.close()

    consulta = pd.concat([consulta1,consulta2,consulta3])

    consulta['qtd'].fillna(0,inplace=True)
    consulta['qtd OP'] = 1
    consulta['qtd'] = consulta['qtd'].astype(float)


    consulta  = consulta.groupby(['operador'])['qtd','qtd OP'].sum().reset_index()
    consulta = consulta.sort_values(by='qtd', ascending=False,
                                ignore_index=True)

    consulta['operador'] = consulta['operador'].str.split(' ').str.get(0)


    consulta['Media Pçs/OP'] = consulta['qtd']/consulta['qtd OP']

    totalP = consulta['qtd'].sum()
    qtdOP = consulta['qtd OP'].sum()

    data = { '1.0- Total Peças': totalP,
             '1.1- Qtd OPs': qtdOP,
        '2.0- Detalhamento': consulta.to_dict(orient='records')}

    return [data]