import pandas as pd
import ConexaoPostgreMPL
import socket


def MeuHost():
    # Obtém o nome da máquina
    hostname = socket.gethostname()

    # Obtém o endereço IP da máquina
    ip_address = socket.gethostbyname(hostname)

    return ip_address


def EmpresaEscolhida():
    ip_address = MeuHost()
    if ip_address == '192.168.0.183':
        return '1'
    elif ip_address == '192.168.0.184':
        return '4'
    elif ip_address == '10.62.39.23':
        return '1'
    else:
        return '1'


def RegraDeEnderecoParaSubstituto():
    conn = ConexaoPostgreMPL.conexao()
    empresa = pd.read_sql('Select implenta_endereco_subs from "Reposicao".configuracoes.empresa ',conn)
    conn.close()

    return empresa['implenta_endereco_subs'][0]