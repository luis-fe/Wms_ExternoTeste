import psutil
import socket

def obter_ip_rede():
    # Obtém todas as interfaces de rede
    interfaces = psutil.net_if_addrs()

    # Itera sobre as interfaces para encontrar a primeira interface com um endereço IPv4
    for interface, info in interfaces.items():
        for addr in info:
            if addr.family == socket.AF_INET:
                return addr.address

# Chamando a função para obter o endereço IP da rede
ip_rede = obter_ip_rede()

print(f"O endereço IP da rede local é: {ip_rede}")
