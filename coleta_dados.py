#%%
# importando bibliotecas
import os
import re
import wget
import zipfile
import sqlite3
import requests
import pandas as pd
import settings as st
from bs4 import BeautifulSoup
#%%
url_data_rf = "http://200.152.38.155/CNPJ/"

# carregando e lendo html
request = requests.get(url_data_rf)
html_text = request.text

soup = BeautifulSoup(html_text, "lxml")

# coletando todos os arquivos dipsonibilizados
files = soup.findAll('a', href = re.compile(".zip"))
file_names = [f.text for f in files]
#%%
# baixando arquivos
# OBS: layout dos dados em 'https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/arquivos/NOVOLAYOUTDOSDADOSABERTOSDOCNPJ.pdf'
for f in file_names:
    url_file = url_data_rf + f
    wget.download(url_file, out = st.PATH_BASES + "comprimido/")
#%%
# extraindo arquivos
for f in file_names:
    with zipfile.ZipFile(st.PATH_BASES + "comprimido/" + f, "r") as zip_ref:
        zip_ref.extractall(st.PATH_BASES + "extraido/")

#%%
# separando arquivos
arquivos = [f for f in os.listdir(st.PATH_BASES + "/extraido") if f.endswith('')]

switcher = {
    'empresa': 'EMPRE',
    'estabelecimento': 'ESTABELE',
    'socios': 'SOCIO',
    'simples': 'SIMPLES',
    'cnae': 'CNAE',
    'moti': 'MOTI',
    'munic': 'MUNIC',
    'natju': 'NATJU',
    'pais': 'PAIS',
    'quals': 'QUALS'
}

arquivos_separados = {
    k: [f for f in arquivos if s in f] for k, s in switcher.items()
}

#%%
# criando conexao com banco sqlite
conn = sqlite3.connect(st.PATH_BASES + "sqlite/dados_rf.sqlite")

#%%
# criando funcao pra inserir os dados
def insert_data(
    df_dict: dict, key: str, table: str, 
    db: sqlite3.Connection, colnames: list
    ) -> None:
    
    cur.execute("DROP TABLE IF EXISTS {tablename};".format(tablename = table))
    
    for i in range(len(df_dict[key])):
        print("Working on file" + " " + df_dict[key][i])

        df = pd.read_csv(
            st.PATH_BASES + "/extraido/" + df_dict[key][i],
            sep = ";",
            skiprows = 0,
            header = None
        )

        df = df.reset_index()

        del df["index"]

        df.columns = colnames

        colnames.to_sql(name = table, con = db, if_exists = "append", index = False)

#%%
# inserindo dados de empresas
insert_data(
    arquivos_separados,
    "empresa",
    "empresa",
    conn,
    [ "cnpj", "razao_social", "natureza_juridica", "qualificacao_responsavel",
    "capital_social", "porte_empresa", "ente_federativo_responsavel"]
    )

#%%
# inserindo dados de estabelecimento
insert_data(
    arquivos_separados,
    "estabelecimento",
    "estabelecimento",
    conn,
    ["cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial", "nome_fantasia", "situacao_cadastral", 
    "data_situacao_cadastral", "motivo_situacao_cadastral", "nome_cidade_exterior", "pais", "data_inicio_atividade",
    "cnae_fiscal_principal", "cnae_fiscal_secundaria", "tipo_logradouro", "logradouro", "numero", "complemento", 
    "bairro", "cep", "uf", "municipio", "ddd_1", "telefone_1", "ddd_2",
    "telefone_2", "ddd_fax", "fax", "correio_eletronico", "situacao_especial", "data_situacao_especial"]
)

#%%
# inserindo dados de socios
insert_data(
    arquivos_separados,
    "socios",
    "socios",
    conn,
    ["cnpj_basico", "identificador_socio", "nome_socio_razao_social", "cpf_cnpj_socio", "qualificacao_socio", "data_entrada_sociedade",
    "pais", "representante_legal", "nome_do_representante", "qualificacao_representante_legal", "faixa_etaria"]
)

#%%
# inserindo dados de mei
insert_data(
    arquivos_separados,
    "simples",
    "simples",
    conn,
    ["cnpj_basico", "opcao_pelo_simples", "data_opcao_simples", "data_exclusao_simples",
    "opcao_mei", "data_opcao_mei", "data_exclusao_mei"]
)

#%%
# inserindo dados de cnae
insert_data(
    arquivos_separados,
    "cnae",
    "cnae",
    conn,
    ["codigo", "descricao"]
)

#%%
# inserindo dados de moti
insert_data(
    arquivos_separados,
    "moti",
    "moti",
    conn,
    ["codigo", "descricao"]
) 

#%%
# inserindo dados de municipos
insert_data(
    arquivos_separados,
    "munic",
    "munic",
    conn,
    ["codigo", "descricao"]
)

#%%
# inserindo dados de natureza juridica
insert_data(
    arquivos_separados,
    "natju",
    "natju",
    conn,
    ["codigo", "descricao"]
)

#%%
# inserindo dados de pais
insert_data(
    arquivos_separados,
    "pais",
    "pais",
    conn,
    ["codigo", "descricao"]
)

#%%
# inserindo dados de qualificacao de soicos
insert_data(
    arquivos_separados,
    "quals",
    "quals",
    conn,
    ["codigo", "descricao"]
)

# %%
# criar indices nas bases de dados
cur = conn.cursor()

cur.executescript("""
CREATE INDEX empresa_cnpj ON empresa(cnpj);
CRATE INDEX estabelecimento_cnpj ON estabelecimento(cnpj_basico);
CREATE INDEX socios_cnpj ON socios(cnpj_basico);
CREATE INDEX simples_cnpj ON simples(cnpj_basico);
""")

conn.commit()