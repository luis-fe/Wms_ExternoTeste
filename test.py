import pandas as pd
import datetime

# Exemplo de DataFrame
data = {
    'nome': ['luis', 'luis', 'andre','andre'],
    'data':['2023-09-01','2023-09-01','2023-09-01','2023-09-01'],
    'horario':['10:00:21','10:00:11','10:00:01','10:00:02'],
}

df = pd.DataFrame(data)
df['horario'] = pd.to_datetime(df['horario']).dt.time
df = df.dropna(subset=['horario'])


def horario_centecimal(time):
    return time.hour + (time.minute / 60) + (time.second / 3600)



df['horario_centecimal'] = df['horario'].apply(horario_centecimal)
df.sort_values(by=['nome', 'data', 'horario'], inplace=True)

df['ritmo'] = df.groupby(['nome', 'data'])['horario_centecimal'].diff()
df['ritmo'] =df['ritmo'] *3600
print(df)