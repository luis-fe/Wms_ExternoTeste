import pandas as pd
from datetime import datetime

# Crie o DataFrame com os dados fornecidos
data = {
    'data': ['30/07/2023', '30/07/2023', '30/07/2023', '31/07/2023', '31/07/2023', '31/07/2023', '31/07/2023'],
    'nome': ['junior', 'augusto', 'junior', 'luis', 'luis', 'junior', 'luis'],
    'horario': ['10:00:01', '09:52:45', '09:51:15', '09:50:04', '09:49:18', '09:44:23', '09:43:24']
}

df = pd.DataFrame(data)

# Converta a coluna 'horario' para o formato de tempo
df['horario'] = pd.to_datetime(df['horario'], format='%H:%M:%S')

# Ordene o DataFrame pelo nome e data
df.sort_values(by=['nome', 'data', 'horario'], inplace=True)

# Calcule o ritmo de apontamento por nome e data
df['ritmo'] = df.groupby(['nome', 'data'])['horario'].diff().shift(-1)

# Crie uma função para converter o ritmo em um formato legível
def format_timedelta(td):
    if pd.notna(td):
        total_seconds = td.total_seconds()
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
    return "-"

# Aplique a função de formatação à coluna 'ritmo'
df['ritmo'] = df['ritmo'].apply(format_timedelta)

print(df)
