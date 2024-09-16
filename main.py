from downloader import Downloader

# Extract to
output = "./output"

# Where the folders are
url = "https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/"
whitelist = []

# Donwloading only the most recent month
Downloader(url, output, whitelist).async_recursive_download(start_range=-1)

# WARN: Throwing a 404 on async download despite having a status of 200
# url = "https://dados.rfb.gov.br/CNPJ/"
# Downloader(url, output_path).recursive_download(start_range=1)
