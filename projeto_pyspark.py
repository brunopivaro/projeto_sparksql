#Importando bibliotecas
import findspark
import psycopg2
import pandasql
import sqlalchemy
import pandas as pd
from pandasql import sqldf
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

#Carregando primeiro dataframe
#df1 - Nome dos jogadores
df1 = pd.read_csv("data/dataset1.csv", index_col = False)

print(df1.shape)
print(df1.dtypes)
print(df1.head())

#df2 - Clubes de futebol
df2 = pd.read_csv("data/dataset2.csv", index_col = False)

print(df2.shape)
print(df2.dtypes)
print(df2.head())

#df3 - Dados das seleções
df3 = pd.read_csv("data/dataset3.csv", index_col = False)

print(df3.shape)
print(df3.dtypes)
print(df3.head())

#df4 - Estatística dos jogadores
df4 = pd.read_csv("data/dataset4.csv", index_col = False)

print(df4.shape)
print(df4.dtypes)
print(df4.head())

#Conectando ao SGBD

#Criando conexão
pgconn = psycopg2.connect(host = 'localhost', user = 'postgres', password = 'dsa123')
pgcursor = pgconn.cursor()
pgcursor
pgconn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)  #Auto-commit
pgcursor.execute('DROP DATABASE IF EXISTS dbdsa') #Exclui banco caso exista
pgcursor.execute('CREATE DATABASE dbdsa') #Criando banco de dados
pgconn.close()

#Conectando ao banco de dados
pgconn = psycopg2.connect(host = 'localhost', database = 'dbdsa', user = 'postgres', password = 'dsa123')

#Criando engine no SQLAlchemy de conexão ao PostgreSQL no Docker
engine = create_engine('postgresql+psycopg2://postgres:dsa123@localhost/dbdsa')