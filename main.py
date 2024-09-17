import os
from downloader import Downloader
from sqlhandler import SQLHandler
import pandas as pd
from sqlalchemy import create_engine

# DOWNLOAD

# Extract to
output = "./output"

# Where the folders are
url = "https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/"
whitelist = []

# Donwloading only the most recent month
# Downloader(url, output, whitelist).async_recursive_download(start_range=-1)

# WARN: Throwing a 404 on async download despite having a status of 200
# url = "https://dados.rfb.gov.br/CNPJ/"
# Downloader(url, output_path).recursive_download(start_range=1)

# TO SQL

# Getting the most recently created folder
extracted_files = [os.path.join(output, d) for d in os.listdir(output)]
extracted_files = [d for d in extracted_files if os.path.isdir(d)]
extracted_files = sorted(
                        extracted_files,
                        key=lambda x: os.path.getctime(x),
                        reverse=True
                        )[0]

# List files
files = [n for n in os.listdir(extracted_files) if not n.endswith('.zip')]

# Grouping files
# Information os size as of 2024-08
simples = []  # 1 File - 2GB
cnae = []  # 1 File
moti = []  # 1 File
munic = []  # 1 File
natju = []  # 1 File
pais = []  # 1 File
quals = []  # 1 File
empre = []  # 10 Files - 4GB Total
estabele = []  # 10 Files - 13GB Total
socio = []  # 10 Files - 2GB Total

for i in files:
    if i.find("SIMPLES") > -1:
        simples.append(i)
    elif i.find("CNAE") > -1:
        cnae.append(i)
    elif i.find("MOTI") > -1:
        moti.append(i)
    elif i.find("MUNIC") > -1:
        munic.append(i)
    elif i.find("NATJU") > -1:
        natju.append(i)
    elif i.find("PAIS") > -1:
        pais.append(i)
    elif i.find("QUALS") > -1:
        quals.append(i)
    elif i.find("EMPRE") > -1:
        empre.append(i)
    elif i.find("ESTABELE") > -1:
        estabele.append(i)
    elif i.find("SOCIO") > -1:
        socio.append(i)
    else:
        print(i)

engine = create_engine("sqlite+pysqlite:///:memory:")

# Smaller Files

cnae_dtype = {'codigo': 'object', 'descricao': 'object'}

cnae_pd = SQLHandler(
    extracted_files,
    cnae,
    cnae_dtype
    ).to_sql_db('cnae', engine)

moti_dtype = {'codigo': 'object', 'descricao': 'object'}

moti_pd = SQLHandler(
    extracted_files,
    moti,
    moti_dtype
    ).to_sql_db('moti', engine)

munic_dtype = {'codigo': 'object', 'descricao': 'object'}

munic_pd = SQLHandler(
    extracted_files,
    munic,
    munic_dtype
    ).to_sql_db('munic', engine)

natju_dtype = {'codigo': 'object', 'descricao': 'object'}

natju_pd = SQLHandler(
    extracted_files,
    natju,
    natju_dtype
    ).to_sql_db('natju', engine)

pais_dtype = {'codigo': 'object', 'descricao': 'object'}

pais_pd = SQLHandler(
    extracted_files,
    pais,
    pais_dtype
    ).to_sql_db('pais', engine)

quals_dtype = {'codigo': 'object', 'descricao': 'object'}

quals_pd = SQLHandler(
    extracted_files,
    quals,
    quals_dtype
    ).to_sql_db('quals', engine)

# Bigger Files

# simples_dtype = {
#         'cnpj_basico': object,
#         'opcao_pelo_simples': object,
#         'data_opcao_simples': 'Int32',
#         'data_exclusao_simples': 'Int32',
#         'opcao_mei': object,
#         'data_opcao_mei': 'Int32',
#         'data_exclusao_mei': 'Int32'
#         }
# 
# simples_pd = SQLHandler(
#     extracted_files,
#     simples,
#     simples_dtype
#     ).to_sql_db('simples', engine)

# from sqlalchemy import MetaData
# 
# m = MetaData()
# m.reflect(engine)
# 
# for t in m.tables.values():
#     print(t.name)
#     for c in t.c:
#         print(" "*10 + c.name)
