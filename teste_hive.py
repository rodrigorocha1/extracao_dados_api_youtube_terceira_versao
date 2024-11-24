import pandas as pd

# Dados
data = [
    {"Título": "Fábricas ÚNICAS | EP 25 Hamilton | Cities: Skylines II GAMEPLAY PT-BR",
     "Período": "Noite", "Data": "2024-11-03 21:57:59", "Valor": 19.59011452682339},
    {"Título": "Fábricas ÚNICAS | EP 25 Hamilton | Cities: Skylines II GAMEPLAY PT-BR",
     "Período": "Noite", "Data": "2024-11-04 22:01:58", "Valor": 16.69049320943531},
    {"Título": "Fábricas ÚNICAS | EP 25 Hamilton | Cities: Skylines II GAMEPLAY PT-BR",
     "Período": "Noite", "Data": "2024-11-05 21:58:57", "Valor": 15.87752525252525},
    {"Título": "Fábricas ÚNICAS | EP 25 Hamilton | Cities: Skylines II GAMEPLAY PT-BR",
     "Período": "Noite", "Data": "2024-11-06 22:14:40", "Valor": 15.554899645808737},
    {"Título": "Fábricas ÚNICAS | EP 25 Hamilton | Cities: Skylines II GAMEPLAY PT-BR",
     "Período": "Noite", "Data": "2024-11-07 22:05:44", "Valor": 15.162659123055164},
    {"Título": "Fábricas ÚNICAS | EP 25 Hamilton | Cities: Skylines II GAMEPLAY PT-BR",
     "Período": "Noite", "Data": "2024-11-08 22:02:17", "Valor": 14.991717283268912},
]

# Criar o DataFrame
df = pd.DataFrame(data)

# Converter a coluna 'Data' para datetime
df['Data'] = pd.to_datetime(df['Data'])

# Exibir o DataFrame
df['TAXA_ENGAJAMENTO_DESLOCADO'] = df['Valor'].shift(1)
df.dropna(inplace=True)
df['TAXA_ENGAJAMENTO_DIA'] = df['Valor'] - df['TAXA_ENGAJAMENTO_DESLOCADO']
print(df)
