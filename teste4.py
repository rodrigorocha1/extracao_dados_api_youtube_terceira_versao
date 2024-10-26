import xml.etree.ElementTree as ET

# Caminho para o arquivo XML
file_path = 'RM2801.xml'

tree = ET.parse(file_path)
root = tree.getroot()

# Encontra todas as tags <procurador> e verifica se estão vazias
empty_procuradores = []
for procurador in root.findall(".//procurador"):
    if not procurador.text or procurador.text.strip() == "":
        empty_procuradores.append(procurador)

# Exibe o resultado
if empty_procuradores:
    print("Tags <procurador> vazias encontradas:")
    for procurador in empty_procuradores:
        print(ET.tostring(procurador, encoding='unicode'))
else:
    print("Nenhuma tag <procurador> vazia encontrada.")
