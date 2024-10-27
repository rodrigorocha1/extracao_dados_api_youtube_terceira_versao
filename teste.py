data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_hora_atual = pendulum.parse(data_hora_atual)
data_hora_busca = data_hora_atual.subtract(hours=12)
data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')

data_hora_formatada_api = data_hora_atual.strftime('%Y-%m-%d %H:%M:%S')
