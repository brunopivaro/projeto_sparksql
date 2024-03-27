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
engine = create_engine('postgresql+psycopg2://postgres:dsa123@localhost/dbdsa') #Motor de execução

#Importando dados dos nossos dataframes no banco de dados
df1.to_sql("tabela_df1", engine, if_exists = 'replace', index = False)
df2.to_sql("tabela_df2", engine, if_exists = 'replace', index = False)
df3.to_sql("tabela_df3", engine, if_exists = 'replace', index = False)
df4.to_sql("tabela_df4", engine, if_exists = 'replace', index = False)

#Carregando dados do PostgreSQL em dataframes do python utilizando o PandaSQL
print(pd.read_sql_query('select count(*) FROM tabela_df2', engine))
df_tabela_df4 = pd.read_sql('select * FROM tabela_df4', engine) #Salvando tabela_df4 em um dataframe no python
select_df = pd.read_sql('tabela_df4', engine, columns = ['Name', 'Age', 'Speed', 'Height', 'Weight']) #Filtrando colunas

#Aplicando funções de SQL nos dataframes
pysqldf = lambda q: sqldf(q, globals())
query = 'SELECT * FROM select_df LIMIT 5'
pysqldf(query)

df_sqldf_1 = pysqldf(query)
print(df_sqldf_1.head())

query = 'SELECT Age, AVG("Speed") AS mean_Speed FROM select_df GROUP BY Age LIMIT 10'
df_sqldf_2 = pysqldf(query)
print(df_sqldf_2.head())

#Carregando dados do PostgreSQL em dataframes do Apache Spark
spark = SparkSession.builder.master('local').appName('projeto_pyspark_sql').getOrCreate()
df = pd.read_sql('SELECT * FROM tabela_df1', engine)
df_spark = spark.createDataFrame(df)

#Manipulando dados no Spark SQL
df1 = pd.read_sql('SELECT * FROM tabela_df4', engine)
df_spark_1 = spark.createDataFrame(df1)

df_select = df_spark_1.select('Name', 'Nationality', 'Club_Joining', 'Height', 'Weight', 'Age', 'Speed', 'Reactions')
print(df_select.where((df_spark_1.Age < 25) & (df_spark_1.Speed > 80)).show())
print(df_select.agg({'Speed': 'Avg'}).show())

#Encerrando conexões
pgconn.close()
engine.dispose()