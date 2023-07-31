import pandas as pd

# Exemplo de DataFrame
data = {
    'pais': ['brasil', 'argentina', 'alemanha'],
}

df = pd.DataFrame(data)

# Extrai os 3 primeiros caracteres de cada valor da coluna "pais"
df['inicio_3'] = df['pais'].str.slice(0, 3)

# Extrai os caracteres 1, 3 e 5 de cada valor da coluna "pais"
df['posicoes_135'] = df['pais'].str.slice(0, None, 2)

print(df)
