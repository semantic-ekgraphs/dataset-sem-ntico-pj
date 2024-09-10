import pyspark
from delta import *
import os
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

PATH_DATA = 'data/'
DELTA_TABLE_BASE_PATH = "/tmp/case-rfb/bronze/"

# Criar diretorios se nao existirem
os.makedirs(DELTA_TABLE_BASE_PATH, exist_ok=True)

table_names = [ 'razao_situacao', 'cnae', 'municipio', 'nat_ju', 'pais', 'qualif_socio', 'empresa', 'estabelecimento', 'socio', 'simei' ]

builder = (
    pyspark.sql.SparkSession.builder.appName("DeltaLakeHouseUFC")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

Schema = StructType([
    StructField("CODIGO", StringType(), True),
    StructField("DESCRICAO", StringType(), True),
])

Schema2 = StructType([
    StructField("CNAE", StringType(), True),
    StructField("DESCRICAO", StringType(), False)
])

Schema3 = StructType([
    StructField("COD_MUNICIPIO", StringType(), True),
    StructField("MUNICIPIO", StringType(), False)
])

Schema4 = StructType([
    StructField("CODIGO", StringType(), True),
    StructField("NATUREZA_JURIDICA", StringType(), False)
])

Schema5 = StructType([
    StructField("COD_PAIS", StringType(), True),
    StructField("NOME_PAIS", StringType(), False)
])

Schema6 = StructType([
    StructField("COD", StringType(), True),
    StructField("DESCRICAO", StringType(), False)
])

Schema7 = StructType([
    StructField("CNPJ_BASICO", StringType(), True),
    StructField("RAZAO_SOCIAL", StringType(), True),
    StructField("COD_NAT_JURIDICA", StringType(), True),
    StructField("QUALIF_RESP", StringType(), True),
    StructField("CAPITAL_SOCIAL", StringType(), False),
    StructField("PORTE", StringType(), False),
    StructField("ENTE_FEDERATIVO", StringType(), False)
])

Schema8 = StructType([
    StructField("CNPJ_BASICO", StringType(), True),
    StructField("CNPJ_ORDEM", StringType(), True),
    StructField("CNPJ_DV", StringType(), True),
    StructField("MATRIZ_FILIAL", StringType(), True),
    StructField("NOME_FANTASIA", StringType(), True), 
    StructField("SITUACAO", StringType(), True), 
    StructField("DATA_SITUACAO", StringType(), True),
    StructField("MOTIVO_SITUACAO", StringType(), True), 
    StructField("NM_CIDADE_EXTERIOR", StringType(), True), 
    StructField("COD_PAIS", StringType(), True), 
    StructField("DATA_INICIO_ATIV", StringType(), True), 
    StructField("CNAE_FISCAL", StringType(), True), 
    StructField("CNAE_FISCAL_SEC", StringType(), True), 
    StructField("TIPO_LOGRADOURO", StringType(), True), 
    StructField("LOGRADOURO", StringType(), True), 
    StructField("NUMERO", StringType(), False), 
    StructField("COMPLEMENTO", StringType(), False), 
    StructField("BAIRRO", StringType(), True), 
    StructField("CEP", StringType(), True), 
    StructField("UF", StringType(), False), 
    StructField("COD_MUNICIPIO", StringType(), True), 
    StructField("DDD_1", StringType(), False),
    StructField("TELEFONE_1", StringType(), False), 
    StructField("DDD_2", StringType(), False), 
    StructField("TELEFONE_2", StringType(), False), 
    StructField("DDD_FAX", StringType(), False), 
    StructField("NUM_FAX", StringType(), False), 
    StructField("EMAIL", StringType(), True), 
    StructField("SIT_ESPECIAL", StringType(), False), 
    StructField("DATA_SIT_ESPECIAL", StringType(), False) 
])

Schema9 = StructType([
    StructField("CNPJ_BASICO", StringType(), True),
    StructField("TIPO_SOCIO", StringType(), True),
    StructField("NOME_SOCIO", StringType(), True), 
    StructField("CNPJ_CPF_SOCIO", StringType(), True), 
    StructField("COD_QUALIFICACAO", StringType(), True), 
    StructField("DATA_ENTRADA", StringType(), True), 
    StructField("COD_PAIS_EXT", StringType(), False), 
    StructField("CPF_REPRES", StringType(), False), 
    StructField("NOME_REPRES", StringType(), False), 
    StructField("COD_QUALIF_REPRES", StringType(), False), 
    StructField("FAIXA_ETARIA", StringType(), False) 
])

Schema10 = StructType([
    StructField("CNPJ_BASICO", StringType(), True),
    StructField("OPC_SIMPLES", StringType(), False),
    StructField("DATA_OPC_SIMPLES", StringType(), False), 
    StructField("DATA_EXC_SIMPLES", StringType(), False),
    StructField("OPC_MEI", StringType(), False), 
    StructField("DATA_OPC_MEI", StringType(), False),
    StructField("DATA_EXC_MEI", StringType(), False) 
])

schemas = [ Schema, Schema2, Schema3, Schema4, Schema5, Schema6, Schema7, Schema8, Schema9, Schema10 ]

dir_to_schema = {
    "razao_situacao": Schema,
    "cnae": Schema2,
    "municipio": Schema3,
    "nat_ju": Schema4, 
    "pais": Schema5, 
    "qualif_socio": Schema6, 
    "empresa": Schema7, 
    "estabelecimento": Schema8, 
    "socio": Schema9, 
    "simei": Schema10
}

dir_mapping = {
    'moti': 'razao_situacao',
    'simples': 'simei',
    'socio': 'socio',
    'estabelecimento': 'estabelecimento',
    'empresa': 'empresa',
    'qual': 'qualif_socio',
    'pais': 'pais',
    'natj': 'nat_ju',
    'municipio': 'municipio',
    'cnae': 'cnae'
}

def activate_cdf(table_name):
    table_path = DELTA_TABLE_BASE_PATH + table_name
    try:
        spark.sql(f"ALTER TABLE delta.`{table_path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        print(f"CDF ativado para a tabela: {table_name}")
    except Exception as e:
        print(f"Erro ao ativar CDF para a tabela: {table_name}. Erro: {e}")

def clear_delta_tables(base_path):
    for table_dir in os.listdir(base_path):
        table_path = os.path.join(base_path, table_dir)
        if os.path.isdir(table_path):
            spark.sql(f"DELETE FROM delta.`{table_path}` WHERE true")
            print(f"Todos os registros foram removidos da tabela: {table_path}")

# Ativa o CDF para todas as tabelas na lista antes de iniciar o processo de carga
for table in table_names:
    activate_cdf(table)

# Limpar todas as Delta Tables antes de iniciar o processo de carga
clear_delta_tables(DELTA_TABLE_BASE_PATH)

status_carga = []
total_records_per_table = {}

for path_dir, schema in dir_to_schema.items():
    for file in os.listdir(PATH_DATA + path_dir):
        if any(key[:4].lower() in file.lower() for key in dir_mapping.keys()):
            file_csv = '{0}'.format(file)
            file_path = PATH_DATA + path_dir + '/' + file_csv
            df = spark.read.format("csv").options(header=False, sep=";", inferSchema=False, delimiter=";").schema(schema).load(file_path)
            df.show()
            df.write.format("delta").mode('append').save(DELTA_TABLE_BASE_PATH + path_dir)
            record_count = df.count()
            if path_dir in total_records_per_table:
                total_records_per_table[path_dir] += record_count
            else:
                total_records_per_table[path_dir] = record_count
            status_carga.append({
                "delta_table": path_dir,
                "file": file_csv,
                "record_count": record_count
            })

# Adicionar a data atual e salvar o status em um arquivo JSON
status_carga_info = {
    "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "status": status_carga,
    "total_records_per_table": total_records_per_table
}

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "status_carga.json"), "w") as status_file:
    json.dump(status_carga_info, status_file, indent=4)

print("Processo de carga concluido e status salvo em status_carga.json")

# Encerrar a sessao Spark
spark.stop()
