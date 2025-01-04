import re

# String de exemplo
arquivo = "dados_processados_20250104_002706.csv"

# Expressão regular para capturar números e underscore
resultado = re.findall(r'[0-9_]+', arquivo)

# Juntando os elementos encontrados
resultado_final = ''.join(resultado)

print(resultado[1])