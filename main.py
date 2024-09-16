import os
import stat
from downloader import Downloader

# Where the folders are
url = "https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/"

# Extract to
output_path = "./output"

Downloader("https://dados.rfb.gov.br/CNPJ/regime_tributario/", output_path).async_download()
# Downloader(url, output_path).async_recursive_download(start_range=-1)  # Most recent month
# Downloader(url, output_path).recursive_download(start_range=-1)