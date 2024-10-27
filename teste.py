import pendulum


def obter_turno(hora: int):
    if 0 <= hora < 6:
        return '_madrugada'
    elif 6 <= hora < 12:
        return '_manha'
    elif 12 <= hora < 18:
        return '_tarde'
    else:
        return '_noite'


data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_hora_atual = pendulum.parse(data_hora_atual)
data_hora_busca = data_hora_atual.subtract(hours=12)
data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')

data_hora_formatada_api = data_hora_atual.strftime('%Y-%m-%d %H:%M:%S')
print(data_hora_formatada_api.replace(
    '-', '_').replace(':', '_').replace(' ', '_'))

print(data_hora_atual)
print(obter_turno(data_hora_atual.hour))
print(obter_turno(1))
print(obter_turno(11))

print(obter_turno(15))
print(obter_turno(19))
