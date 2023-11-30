import socket


def obter_ip():
    # Obtém o nome do host da máquina
    host_name = socket.gethostname()

    # Obtém o endereço IP associado ao nome do host
    endereco_ip = socket.gethostbyname(host_name)

    return endereco_ip


# Chamando a função para obter o endereço IP
ip_da_maquina = obter_ip()

print(f"O endereço IP da máquina é: {ip_da_maquina}")
