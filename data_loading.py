import os
import requests

def download_data(url, destino):
    resposta = requests.get(url, stream=True)
    if resposta.status_code == 200:
        with open(destino, 'wb') as f:
            for chunk in resposta.iter_content(1024):
                f.write(chunk)
        print(f'Download completo: {destino}')
    else:
        print(f'Erro ao baixar {url}: {resposta.status_code}')

# URLs dos arquivos
arquivos = {
    "order.json.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz",
    "consumer.csv.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/consumer.csv.gz",
    "restaurant.csv.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/restaurant.csv.gz",
    "ab_test_ref.tar.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/ab_test_ref.tar.gz"
}

# Criar pasta data
os.makedirs("data", exist_ok=True)

# Fazer download de todos os arquivos
for nome, url in arquivos.items():
    caminho = os.path.join("data", nome)
    if not os.path.exists(caminho):
        print(f"Baixando {nome}...")
        download_data(url, caminho)
    else:
        print(f"{nome} já existe. Pulando download.")