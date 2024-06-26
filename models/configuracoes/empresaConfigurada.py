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
        return '4'


def RegraDeEnderecoParaSubstituto():
    conn = ConexaoPostgreMPL.conexao()
    empresa = pd.read_sql('Select implenta_endereco_subs from "Reposicao".configuracoes.empresa ',conn)
    conn.close()

    return empresa['implenta_endereco_subs'][0]