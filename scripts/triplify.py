from pyspark.sql import SparkSession
import rdflib
import pyspark
from rdflib.namespace import RDF, RDFS, XSD
import unicodedata
import re
from delta import *

# Funcao para remover acentos de uma string
def remove_acentos(input_str):
    if not isinstance(input_str, str):
        input_str = str(input_str)
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

# Funcao para normalizar URIs
def normalize_uri(value):
    if not isinstance(value, str):
        value = str(value)
    value = remove_acentos(value)
    value = value.upper()
    value = re.sub(r'\s+', '_', value)
    value = re.sub(r'[^A-Z0-9_]', '', value)  # Remove caracteres especiais
    return value

builder = (
    pyspark.sql.SparkSession.builder.appName("DeltaLakeHouseUFC")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "10g")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.maxResultSize", "4g")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Leitura do arquivo de mapeamento RDF (R2RML)
mapping_graph = rdflib.Graph()
mapping_graph.parse("rfb.ttl", format="ttl")

# Prefixos
prefixes = {
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "rr": "http://www.w3.org/ns/r2rml#",
    "ex": "http://example.com/ns#",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "vcard": "http://www.w3.org/2006/vcard/ns#",
    "dcterms": "http://purl.org/dc/terms/",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "rfb": "http://www.sefaz.ma.gov.br/ontology/",
    "cnae": "http://www.sefaz.ma.gov.br/ontology/",
    "map": "http://sistemas.sefaz.ma.gov.br/maps/RFB/2020-04-01",
    "rfb_resource": "http://www.sefaz.ma.gov.br/RFB/resource/",
    "sefazma": "http://www.sefaz.ma.gov.br/ontology/"
}

# Criacao do grafo RDF inicial com os prefixos
rdf_graph = rdflib.Graph()
for prefix, uri in prefixes.items():
    rdf_graph.bind(prefix, rdflib.Namespace(uri))

# Arquivo de saida
output_file = "outz.ttl"

# Salvando os prefixos no arquivo inicialmente (uma unica vez)
with open(output_file, "w") as f:
    f.write(rdf_graph.serialize(format="turtle"))

# Processar cada TriplesMap
for tm in mapping_graph.subjects(rdflib.RDF.type, rdflib.URIRef("http://www.w3.org/ns/r2rml#TriplesMap")):
    logical_table_node = None
    for _, _, o in mapping_graph.triples((tm, rdflib.URIRef("http://www.w3.org/ns/r2rml#logicalTable"), None)):
        logical_table_node = o

    if not logical_table_node:
        print(f"TriplesMap {tm} nao possui um logicalTable valido.")
        continue

    query = None
    for _, _, o in mapping_graph.triples((logical_table_node, rdflib.URIRef("http://www.w3.org/ns/r2rml#sqlQuery"), None)):
        query = str(o)

    if not query:
        print(f"TriplesMap {tm} nao possui uma consulta SQL valida.")
        continue

    table_name_match = re.findall(r"FROM\s+([^\s]+)", query, re.IGNORECASE)
    if not table_name_match:
        print(f"Nao foi possivel determinar o nome da tabela para a consulta: {query}")
        continue

    table_name = table_name_match[0]
    deltatable_path = f"/tmp/case-rfb/silver/{table_name}"
    df = spark.read.format("delta").load(deltatable_path)
    total_records = df.count()
    print(f"Total de registros na tabela {table_name}: {total_records}")

    subject_template = None
    predicates = {}
    object_templates = {}
    subject_classes = []

    for _, p, o in mapping_graph.triples((tm, rdflib.URIRef("http://www.w3.org/ns/r2rml#subjectMap"), None)):
        for _, _, template in mapping_graph.triples((o, rdflib.URIRef("http://www.w3.org/ns/r2rml#template"), None)):
            subject_template = str(template)
        for _, _, cls in mapping_graph.triples((o, rdflib.URIRef("http://www.w3.org/ns/r2rml#class"), None)):
            subject_classes.append(str(cls))

    # Adicione um log para verificar se o mapeamento esta correto
    print(f"TriplesMap {tm} com subjectTemplate {subject_template} e classes {subject_classes}")

    for _, p, o in mapping_graph.triples((tm, rdflib.URIRef("http://www.w3.org/ns/r2rml#predicateObjectMap"), None)):
        predicates_list = []
        column = None
        template = None
        datatype = XSD.string
        for _, p, obj in mapping_graph.triples((o, rdflib.URIRef("http://www.w3.org/ns/r2rml#predicate"), None)):
            predicates_list.extend([pred.strip() for pred in str(obj).split(',')])
        for _, p, obj in mapping_graph.triples((o, rdflib.URIRef("http://www.w3.org/ns/r2rml#objectMap"), None)):
            for _, p, col in mapping_graph.triples((obj, rdflib.URIRef("http://www.w3.org/ns/r2rml#column"), None)):
                column = str(col)
            for _, _, tmpl in mapping_graph.triples((obj, rdflib.URIRef("http://www.w3.org/ns/r2rml#template"), None)):
                template = str(tmpl)
            for _, p, dt in mapping_graph.triples((obj, rdflib.URIRef("http://www.w3.org/ns/r2rml#datatype"), None)):
                datatype = str(dt)

        for predicate in predicates_list:
            if column:
                if predicate in predicates:
                    predicates[predicate].append((column, datatype))
                else:
                    predicates[predicate] = [(column, datatype)]
            elif template:
                object_templates[predicate] = template

    # Verifica a existencia das colunas no DataFrame e ajusta a capitalizacao
    columns = [col.lower() for col in df.columns]
    predicates = {k: [(v[0].lower(), v[1]) for v in vals if v[0].lower() in columns] for k, vals in predicates.items()}

    # Processamento em lotes
    batch_size = 200000  # Ajustando o tamanho do lote para 100000
    num_batches = (total_records // batch_size) + 1

    # Dividindo o DataFrame em lotes usando randomSplit
    fractions = [float(batch_size) / total_records] * num_batches
    df_batches = df.randomSplit(fractions, seed=42)

    processed_records = 0  # Variavel para contar os registros processados

    for batch_idx, df_batch in enumerate(df_batches):
        # Criar um novo grafo RDF para cada lote
        rdf_batch_graph = rdflib.Graph()

        batch_records = 0  # Contador de registros no lote atual

        for row in df_batch.toLocalIterator():
            subject_values = {col.lower(): row[col] for col in row.asDict()}
            # Gera o sujeito com base nos valores do template
            subject_template_vars = re.findall(r"{(\w+)}", subject_template)
            subject_values_for_template = {var: normalize_uri(subject_values[var.lower()]) for var in subject_template_vars if var.lower() in subject_values}
            if not subject_values_for_template:
                print(f"Nao foi possivel gerar URI do sujeito para a linha: {row}")
                continue
            subject = rdflib.URIRef(subject_template.format(**subject_values_for_template))
            # Adiciona as classes do sujeito
            for cls in subject_classes:
                rdf_batch_graph.add((subject, RDF.type, rdflib.URIRef(cls)))
                print(f"Adicionado tipo {cls} para {subject}")
            for predicate, column_info_list in predicates.items():
                for column, datatype in column_info_list:
                    if column in subject_values:
                        value = subject_values[column]
                        if value:
                            rdf_batch_graph.add((subject, rdflib.URIRef(predicate), rdflib.Literal(value, datatype=datatype)))
            for predicate, template in object_templates.items():
                try:
                    object_value = template.format(**{col: normalize_uri(subject_values[col.lower()]) for col in re.findall(r"{(\w+)}", template)})
                    rdf_batch_graph.add((subject, rdflib.URIRef(predicate), rdflib.URIRef(object_value)))
                except KeyError as e:
                    print(f"Erro ao gerar a URI do objeto para a template {template}: {e}")

            batch_records += 1

        processed_records += batch_records
        print(f"Registros processados no lote {batch_idx + 1}/{num_batches}: {batch_records}")

        # Serializando o grafo do lote e salvando no arquivo de saida
        with open(output_file, "a") as f:
            f.write(rdf_batch_graph.serialize(format="turtle"))

    print(f"Total de registros processados: {processed_records}")
    if processed_records != total_records:
        print(f"Atencao: Foram processados {processed_records} registros, mas a tabela possui {total_records} registros. Verifique o processamento.")
    else:
        print("Todos os registros foram processados com sucesso.")

print("Triplas RDF geradas com sucesso!")
