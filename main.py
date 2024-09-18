import os
from downloader import Downloader
from sqlhandler import SQLHandler
import pandas as pd
from sqlalchemy import create_engine
import credentials

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

# Establishing connection to the SQL database
# Example:
# engine = create_engine("sqlite+pysqlite:///:memory:")
# "sqlite+pysqlite:///:memory:" stands for:
# 1. Type (sqlite)
# 2. Python Database API Specification (pysqlite)
# 3. Location (/:memory:) (in memory)

user = credentials.user  # os.getenv('DB_USER')
passw = credentials.passw  # os.getenv('DB_PASSWORD')
host = credentials.host  # os.getenv('DB_HOST')
port = credentials.port  # os.getenv('DB_PORT')
database = credentials.database  # os.getenv('DB_NAME')

# Connection to PostgreSQL
engine = create_engine('postgresql://'+user+':'+passw +
                       '@'+host+':'+port+'/'+database)

# Smaller Files

#  cnae_dtype = {'codigo': 'object', 'descricao': 'object'}
#  
#  cnae_pd = SQLHandler(
#      extracted_files,
#      cnae,
#      cnae_dtype
#      ).to_sql_db('cnae', engine)
#  
#  # ---
#  
#  moti_dtype = {'codigo': 'object', 'descricao': 'object'}
#  
#  moti_pd = SQLHandler(
#      extracted_files,
#      moti,
#      moti_dtype
#      ).to_sql_db('moti', engine)
#  
#  # ---
#  
#  munic_dtype = {'codigo': 'object', 'descricao': 'object'}
#  
#  munic_pd = SQLHandler(
#      extracted_files,
#      munic,
#      munic_dtype
#      ).to_sql_db('munic', engine)
#  
#  natju_dtype = {'codigo': 'object', 'descricao': 'object'}
#  
#  # ---
#  
#  natju_pd = SQLHandler(
#      extracted_files,
#      natju,
#      natju_dtype
#      ).to_sql_db('natju', engine)
#  
#  # ---
#  
#  pais_dtype = {'codigo': 'object', 'descricao': 'object'}
#  
#  pais_pd = SQLHandler(
#      extracted_files,
#      pais,
#      pais_dtype
#      ).to_sql_db('pais', engine)
#  
#  # ---
#  
#  quals_dtype = {'codigo': 'object', 'descricao': 'object'}
#  
#  quals_pd = SQLHandler(
#      extracted_files,
#      quals,
#      quals_dtype
#      ).to_sql_db('quals', engine)

# Bigger Files

simples_dtype = {
        'cnpj_basico': 'object',
        'opcao_pelo_simples': 'object',
        'data_opcao_simples': 'Int32',
        'data_exclusao_simples': 'Int32',
        'opcao_mei': 'object',
        'data_opcao_mei': 'Int32',
        'data_exclusao_mei': 'Int32'
        }

simples_pd = SQLHandler(
    extracted_files,
    simples,
    simples_dtype
    ).to_sql_db('simples', engine)

# ---

# socio_dtype = {
#     'cnpj_basico': 'object',
#     'identificador_socio': 'Int32',
#     'nome_socio_razao_social': 'object',
#     'cpf_cnpj_socio': 'object',
#     'qualificacao_socio': 'Int32',
#     'data_entrada_sociedade': 'Int32',
#     'pais': 'Int32',
#     'representante_legal': 'object',
#     'nome_do_representante': 'object',
#     'qualificacao_representante_legal': 'Int32',
#     'faixa_etaria': 'Int32'
# }
# 
# socio_pd = SQLHandler(
#     extracted_files,
#     socio,
#     socio_dtype
#     ).as_pdDataframe()

# ---

# empre_dtype = {
#     'cnpj_basico': 'object',
#     'razao_social': 'object',
#     'natureza_juridica': 'Int32',
#     'qualificacao_responsavel': 'Int32',
#     'capital_social': 'float32',
#     'porte_empresa': 'Int32',
#     'ente_federativo_responsavel': 'object'
# }
#
# empre_pd = SQLHandler(
#     extracted_files,
#     empre,
#     empre_dtype
#     ).to_sql_db()

# ---

# estabele_dtype = {
#     'cnpj_basico': 'object',
#     'cnpj_ordem': 'object',
#     'cnpj_dv': 'object',
#     'identificador_matriz_filial': 'Int32',
#     'nome_fantasia': 'object',
#     'situacao_cadastral': 'Int32',
#     'data_situacao_cadastral': 'Int32',
#     'motivo_situacao_cadastral': 'Int32',
#     'nome_cidade_exterior': 'object',
#     'pais': 'object',
#     'data_inicio_atividade': 'Int32',
#     'cnae_fiscal_principal': 'Int32',
#     'cnae_fiscal_secundaria': 'object',
#     'tipo_logradouro': 'object',
#     'logradouro': 'object',
#     'numero': 'object',
#     'complemento': 'object',
#     'bairro': 'object',
#     'cep': 'object',
#     'uf': 'object',
#     'municipio': 'Int32',
#     'ddd_1': 'object',
#     'telefone_1': 'object',
#     'ddd_2': 'object',
#     'telefone_2': 'object',
#     'ddd_fax': 'object',
#     'fax': 'object',
#     'correio_eletronico': 'object',
#     'situacao_especial': 'object',
#     'data_situacao_especial': 'Int32'
# }
#
# estabele_pd = SQLHandler(
#     extracted_files,
#     estabele,
#     estabele_dtype
#     ).to_sql_db()

# from sqlalchemy import MetaData
#
# m = MetaData()
# m.reflect(engine)
#
# for t in m.tables.values():
#     print(t.name)
#     for c in t.c:
#         print(" "*10 + c.name)

# pd.set_option("display.max_columns", 30)
