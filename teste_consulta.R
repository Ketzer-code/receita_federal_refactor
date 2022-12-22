# importando bibliotecas
library(DBI)
library(RSQLite)
library(tidyverse)

# criando conexao
con <- dbConnect(
    SQLite(),
    "/Users/bi005521/OneDrive - BANCO INTER SA/bases_receita_federal/sqlite/dados_rf.sqlite"
)

# testando consulta
db <- dbGetQuery(con, "SELECT * FROM empresa WHERE cnpj = '416968'")
