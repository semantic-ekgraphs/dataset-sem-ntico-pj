import pyspark
from delta import *
import os

#path_list = [ 'razao_situacao', 'cnae', 'municipio', 'nat_ju', 'pais', 'qualif_socio', 'empresa', 'estabelecimento', 'socio', 'simei' ]

# Lista de nomes das tabelas
table_names = [
    'razao_situacao', 'cnae', 'municipio', 'nat_ju', 'pais', 
    'qualif_socio', 'empresa', 'estabelecimento', 'socio', 'simei'
]


builder = (
    pyspark.sql.SparkSession.builder.appName("DeltaLakeHouseUFC")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
   .config("spark.memory.offHeap.enabled","true") 
   .config("spark.memory.offHeap.size","16g")  
   .enableHiveSupport()
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

#startSilver()


def createEmpresa():
    query = """
    CREATE TABLE IF NOT EXISTS RFB_EMPRESA (
        SELECT 'http://www.sefaz.ma.gov.br/resource/RFB/Empresa/'||root_cnpj as URI --CONCAT(CONCAT(root_cnpj,'-'),razao_uri)
       ,root_cnpj AS CNPJ_RAIZ
       ,razao_social AS RAZAO_SOCIAL
       ,concat('http://www.sefaz.ma.gov.br/resource/RFB/Simples/',CONCAT(CONCAT(opc_simples,'-'),root_cnpj)) AS URI_OPCAO_SIMPLES
       ,concat('http://www.sefaz.ma.gov.br/resource/RFB/Mei/',CONCAT(CONCAT(opc_mei,'-'),root_cnpj)) AS URI_OPCAO_MEI
       ,capital_social AS CAPITAL_SOCIAL
       ,tem_porte AS TEM_PORTE
       ,ente_federativo AS ENTE_FEDERATIVO
       ,CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Qualificacao/',id_qualif) AS URI_QUALIFICACAO_REPRESENTANTE
       ,CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Natureza_Legal/',id_nat) AS URI_NATUREZA_LEGAL
       ,CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Estabelecimento/',uri_est_mat) as URI_ESTABELECIMENTO_MATRIZ
       ,CONCAT(CONCAT(opc_simples,'-'),root_cnpj) as ID_OPCAO_SIMPLES
       ,CONCAT(CONCAT(opc_mei,'-'),root_cnpj) as ID_OPCAO_MEI
       ,id_qualif AS ID_QUALIFICACAO_REPRESENTANTE
       ,id_nat AS ID_NATUREZA_LEGAL
       ,uri_est_mat AS ID_ESTABELECIMENTO_MATRIZ
FROM 
(
	SELECT  e.CNPJ_BASICO                                                    AS root_cnpj 
	       ,REPLACE(regexp_replace(nome_fantasia,'[^a-zA-Z ]',''),' ','_')   AS nome_uri 
	       ,REPLACE(regexp_replace(e.RAZAO_SOCIAL ,'[^a-zA-Z ]',''),' ','_') AS razao_uri 
	       ,e.RAZAO_SOCIAL                                                   AS razao_social
	       ,CASE WHEN s.OPC_SIMPLES = '' THEN 'OUTROS' 
	             WHEN s.OPC_SIMPLES = 'S' THEN 'SIM' 
	             WHEN s.OPC_SIMPLES = 'N' THEN 'NAO'  ELSE 'OUTROS' END      AS opc_simples 
	       ,CASE WHEN s.OPC_MEI = '' THEN 'OUTROS' 
	             WHEN s.OPC_MEI = 'S' THEN 'SIM' 
	             WHEN s.OPC_MEI = 'N' THEN 'NAO'  ELSE 'OUTROS' END          AS opc_mei 
	       ,q.id_uri                                                         AS id_qualif 
	       ,n.id_uri                                                         AS id_nat 
	       ,CASE WHEN e.CAPITAL_SOCIAL IS NULL THEN 0 END                    AS capital_social 
	       ,CASE WHEN e.PORTE = '1' OR e.PORTE = '01' THEN 'NAO_INFORMADO' 
	             WHEN e.PORTE = '2' OR e.PORTE = '02' THEN 'MICRO_EMPRESA' 
	             WHEN e.PORTE = '3' OR e.PORTE = '03' THEN 'EMPRESA_DE_PEQUENO_PORTE' 
	             WHEN e.PORTE = '5' OR e.PORTE = '05' THEN 'DEMAIS' END      AS tem_porte 
	       ,e.ENTE_FEDERATIVO                                                AS ente_federativo 
	       ,CONCAT(est.CNPJ_BASICO,CONCAT(est.CNPJ_ORDEM,est.CNPJ_DV)) AS uri_est_mat
            -- CONCAT(est.CNPJ_BASICO,CONCAT(est.CNPJ_ORDEM,CONCAT(est.CNPJ_DV,CONCAT('-',REPLACE(regexp_replace(est.NOME_FANTASIA,'[^a-zA-Z ]',''),' ','_'))))) AS uri_est_mat
	FROM delta.`/tmp/case-rfb/empresa` e
	INNER JOIN 
	(
		SELECT  cod ,descricao ,translate (replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
		FROM delta.`/tmp/case-rfb/qualif_socio` 
	) q
	ON e.qualif_resp = q.COD
	LEFT OUTER JOIN 
	(
		SELECT  codigo 
		       ,natureza_juridica 
		       ,translate ( replace(UPPER(natureza_juridica),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri 
		       ,replace(codigo,'-','') AS id
		FROM delta.`/tmp/case-rfb/nat_ju` 
	) n
	ON e.cod_nat_juridica = n.id
	INNER JOIN 
	(
		SELECT  CNPJ_BASICO 
		       ,CNPJ_ORDEM 
		       ,CNPJ_DV 
		       ,MATRIZ_FILIAL 
		       ,NOME_FANTASIA
               ,UF
		FROM delta.`/tmp/case-rfb/estabelecimento` 
        WHERE MATRIZ_FILIAL = '1'
        AND UF = 'MA'
	) est
	ON e.CNPJ_BASICO = est.CNPJ_BASICO
	LEFT OUTER JOIN 
	(
		SELECT  CNPJ_BASICO 
		       ,OPC_SIMPLES 
		       ,OPC_MEI
		FROM delta.`/tmp/case-rfb/simei` 
	) s
	ON e.CNPJ_BASICO = s.CNPJ_BASICO
)
);
"""
    spark.sql(query)
    print('RFB_EMPRESA createda com sucesso!')


    
def createEmpresaHolding():
    query = """
CREATE TABLE IF NOT EXISTS RFB_EMPRESA_HOLDING (
SELECT  DISTINCT RAZAO_SOCIAL, -- razao_social 
        CNPJ_SOCIO, 
        CNPJ_RAIZ
FROM 
(
	SELECT DISTINCT  
	       NOME_SOCIO AS RAZAO_SOCIAL,
           CNPJ_CPF_SOCIO AS CNPJ_SOCIO,
           soc.CNPJ_BASICO AS CNPJ_RAIZ
	FROM delta.`/tmp/case-rfb/socio` soc
    INNER JOIN delta.`/tmp/case-rfb/estabelecimento` EST
    ON MATRIZ_FILIAL = '1'
    AND UF = 'MA'
    AND soc.CNPJ_BASICO = est.CNPJ_BASICO
    WHERE TIPO_SOCIO = '1'  
)
);  
"""
    spark.sql(query)
    print('RFB_EMPRESA_HOLDING criada com sucesso!')


def createEndereco():
    query = """  
         CREATE TABLE IF NOT EXISTS RFB_ENDERECO (
     SELECT CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Endereco/',e.CNPJ) as uri_endereco,
        CASE e.UF WHEN 'AC' THEN 'ACRE' WHEN 'AL' THEN 'ALAGOAS' WHEN 'AP' THEN 'AMAPA' WHEN 'AM' THEN 'AMAZONAS' WHEN 'BA' THEN 'BAHIA' WHEN 'CE' THEN 'CEARA' WHEN 'ES' THEN 'ESPIRITO_SANTO' WHEN 'GO' THEN 'GOIAS' WHEN 'MA' THEN 'MARANHAO' WHEN 'MT' THEN 'MATO_GROSSO' WHEN 'MS' THEN 'MATO_GROSSO DO SUL' WHEN 'MG' THEN 'MINAS_GERAIS' WHEN 'PA' THEN 'PARA' WHEN 'PB' THEN 'PARAIBA' WHEN 'PR' THEN 'PARANA' WHEN 'PE' THEN 'PERNAMBUCO' WHEN 'PI' THEN 'PIAUI' WHEN 'RJ' THEN 'RIO_DE_JANEIRO' WHEN 'RN' THEN 'RIO_GRANDE_DO_NORTE' WHEN 'RS' THEN 'RIO_GRANDE_DO_SUL' WHEN 'RO' THEN 'RONDONIA' WHEN 'RR' THEN 'RORAIMA' WHEN 'SC' THEN 'SANTA_CATARINA' WHEN 'SP' THEN 'SAO_PAULO' WHEN 'SE' THEN 'SERGIPE' WHEN 'TO' THEN 'TOCANTINS' WHEN 'DF' THEN 'DISTRITO_FEDERAL' ELSE NULL END AS uf
               ,CASE WHEN m.MUNICIPIO = '' THEN NULL  ELSE regexp_replace(m.MUNICIPIO,'[^a-zA-Z ]','') END                            AS municipio 
               ,CASE WHEN e.LOGRADOURO = '' THEN NULL  ELSE regexp_replace(CONCAT(CONCAT(e.TIPO_LOGRADOURO,' '),e.LOGRADOURO),'[^a-zA-Z0-9 ]','') END AS logradouro 
               ,CASE WHEN e.NUMERO = '' THEN NULL  ELSE numero END                                                                                     AS numero 
               ,CASE WHEN e.COMPLEMENTO = '' THEN NULL  ELSE complemento END                                                                           AS complemento 
               ,CASE WHEN e.CEP = '' THEN NULL ELSE cep END                                                                                   AS cep 
               ,CASE WHEN e.BAIRRO = '' THEN NULL  ELSE regexp_replace(e.BAIRRO,'[^a-zA-Z ]','') END                                  AS bairro 
               ,CASE WHEN p.NOME_PAIS = '' OR p.NOME_PAIS IS NULL THEN 'BRASIL'  ELSE regexp_replace(p.NOME_PAIS,'[^a-zA-Z ]','') END AS nome_pais 
               ,e.CNPJ AS id_endereco
           ,REGEXP_REPLACE(regexp_replace(m.MUNICIPIO,'[^a-zA-Z ]',''),' ','_')||'-'||e.UF AS id_cidade
           ,e.CEP||'-'||UPPER(REGEXP_REPLACE(TRANSLATE(regexp_replace(CONCAT(CONCAT(e.TIPO_LOGRADOURO,' '),e.LOGRADOURO),'[^a-zA-Z0-9 ]',''), '¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
        'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy'),' ','_'))||'-'||UPPER(REGEXP_REPLACE(TRANSLATE(regexp_replace(e.BAIRRO,'[^a-zA-Z ]',''), '¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
        'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy'),' ','_')) AS id_logradouro
           ,UPPER(REGEXP_REPLACE(TRANSLATE(regexp_replace(e.BAIRRO,'[^a-zA-Z ]',''), '¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
        'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy'),' ','_'))||'-'||UPPER(REGEXP_REPLACE(TRANSLATE(regexp_replace(m.MUNICIPIO,'[^a-zA-Z ]',''), '¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
        'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy'),' ','_')) AS id_bairro
        ,e.UF AS uf_abreviado
            FROM 
            (
               SELECT CEP, LOGRADOURO, BAIRRO, NUMERO, UF, COMPLEMENTO, COD_PAIS, TIPO_LOGRADOURO, COD_MUNICIPIO, CONCAT(CNPJ_BASICO,CONCAT(CNPJ_ORDEM,CNPJ_DV)) AS CNPJ FROM delta.`/tmp/case-rfb/estabelecimento` 
               WHERE NM_CIDADE_EXTERIOR = '' or NM_CIDADE_EXTERIOR IS NULL
               AND UF = 'MA'
            ) e
            INNER JOIN
            (
                SELECT DISTINCT COD_PAIS 
                   ,NOME_PAIS
                FROM delta.`/tmp/case-rfb/pais` 
                where cod_pais in (105,106)--somente brasil
            ) p
            ON (case when e.COD_PAIS is null then 105 end) = to_number(p.COD_PAIS,99999)
            INNER JOIN
            (
                SELECT DISTINCT COD_MUNICIPIO 
                   ,MUNICIPIO
                FROM delta.`/tmp/case-rfb/municipio` 
            ) m
            ON e.COD_MUNICIPIO = to_number(m.COD_MUNICIPIO,99999999)
);"""
    spark.sql(query)
    print('RFB_ENDERECO criada com sucesso!')


def createEnderecoExterior():
    query = """CREATE TABLE IF NOT EXISTS RFB_ENDERECO_EXTERIOR (
    SELECT DISTINCT CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Endereco/',aux.id_endereco) as uri   
       ,MAX(aux.cep) -- cep OK 
       ,MAX(aux.nome_pais) -- pais OK 
       --,MAX(CASE WHEN aux.nm_cidade_exterior IS NULL THEN NULL ELSE CONCAT(CONCAT(aux.nm_cidade_exterior,'-'),aux.nome_pais) END) AS city_ext
       ,MAX(CONCAT(CONCAT(aux.nm_cidade_exterior,'-'),aux.nome_pais)) AS city_ext
       ,aux.id_endereco AS ID_ENDERECO
    FROM 
    (
        SELECT  DISTINCT CASE WHEN e.CEP = '' THEN NULL ELSE cep END                                                                                   AS cep 
	       ,CASE WHEN p.NOME_PAIS = '' OR p.NOME_PAIS IS NULL THEN 'BRASIL'  ELSE REPLACE(regexp_replace(p.NOME_PAIS,'[^a-zA-Z ]',''),' ','_') END AS nome_pais 
	       , CONCAT(CNPJ_BASICO,CONCAT(CNPJ_ORDEM,CNPJ_DV)) AS ID_ENDERECO
           ,e.NM_CIDADE_EXTERIOR
        FROM delta.`/tmp/case-rfb/estabelecimento` e
        INNER JOIN 
        (
            SELECT  COD_PAIS 
		       ,NOME_PAIS
            FROM delta.`/tmp/case-rfb/pais` 
        ) p
        ON e.COD_PAIS = p.COD_PAIS
        WHERE e.NM_CIDADE_EXTERIOR != '' 
        or e.NM_CIDADE_EXTERIOR IS NOT NULL  
    ) aux
    GROUP BY  aux.id_endereco
    );
"""
    spark.sql(query)
    print('RFB_ENDERECO_EXTERIOR criada com sucesso!')



def createEstabelecimento():
    query = """
    CREATE TABLE IF NOT EXISTS RFB_ESTABELECIMENTO (
    SELECT
    DISTINCT
       cnpj  
       ,razao_social
       ,nome_fantasia 
       ,email 
       ,matriz_filial as tipo_estabelecimento 
       ,CONCAT ('http://www.sefaz.ma.gov.br/resource/RFB/Situacao_Cadastral/',cnae_fiscal) as uri_atividade_economica
       ,data_inicio_ativ 
       ,concat('http://www.sefaz.ma.gov.br/resource/RFB/Endereco/',id_endereco) as uri_endereco_nacional
       ,NULL 
       ,phone_1 as telefone_1
       ,phone_2 as telefone_2
       ,fax 
       ,CONCAT ('http://www.sefaz.ma.gov.br/resource/RFB/Situacao_Cadastral/',CONCAT(CONCAT(CONCAT(CONCAT(situacao,'-'),cnpj),'-'),data_situacao)) as uri_situacao
       ,CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Estabelecimento/',cnpj) as uri  
       ,cnae_fiscal as id_atividade_economica
       ,id_endereco
       ,CONCAT(CONCAT(CONCAT(CONCAT(situacao,'-'),cnpj),'-'),data_situacao) as id_situacao 
FROM 
(
	SELECT CONCAT(est.CNPJ_BASICO,CONCAT(est.CNPJ_ORDEM,est.CNPJ_DV))                                AS cnpj 
	       ,REPLACE(regexp_replace(est.NOME_FANTASIA,'[^a-zA-Z ]',''),' ','_')                                                                       AS nome_uri 
	       ,e.RAZAO_SOCIAL                                                                                                                            AS razao_social 
	       ,est.UF as UF
         ,CASE est.NOME_FANTASIA WHEN '' THEN NULL ELSE est.NOME_FANTASIA END                                                                               AS nome_fantasia 
	       ,CASE WHEN length(data_situacao) < 8 THEN NULL WHEN data_situacao like '00000000' THEN NULL ELSE data_situacao END as data_situacao
	       ,CASE est.SITUACAO WHEN '01' THEN 'NULA' WHEN '1' THEN 'NULA' WHEN '02' THEN 'ATIVA' WHEN '2' THEN 'ATIVA' WHEN '03' THEN 'SUSPENSA' WHEN '3' THEN 'SUSPENSA' WHEN '04' THEN 'INAPTA' WHEN '4' THEN 'INAPTA' WHEN '08' THEN 'BAIXADA' WHEN '8' THEN 'BAIXADA' END AS situacao 
	       ,CASE est.EMAIL WHEN '' THEN NULL ELSE est.EMAIL END                                                                                               AS email 
	       ,CASE est.MATRIZ_FILIAL WHEN '1' THEN 'MATRIZ' ELSE 'FILIAL' END                                                                               AS matriz_filial 
	       ,est.CNAE_FISCAL 
	       ,CONCAT(CONCAT(est.CNPJ_BASICO,est.CNPJ_ORDEM),est.CNPJ_DV) AS id_endereco 
         ,CASE WHEN length( est.DATA_INICIO_ATIV) < 8 THEN NULL WHEN  est.DATA_INICIO_ATIV like '00000000' THEN NULL ELSE est.DATA_INICIO_ATIV END                                           AS data_inicio_ativ 
	       ,CASE WHEN est.TELEFONE_1 IS NULL THEN NULL 
	             WHEN est.TELEFONE_1='' THEN NULL  ELSE '('||est.DDD_1||') '||est.TELEFONE_1 END                                                                  AS phone_1 
	       ,CASE WHEN est.TELEFONE_2 IS NULL THEN NULL 
	             WHEN est.TELEFONE_2 ='' THEN NULL  ELSE '('||est.DDD_2||') '||est.TELEFONE_2 END                                                                 AS phone_2 
	       ,CASE WHEN est.NUM_FAX IS NULL THEN NULL 
	             WHEN est.NUM_FAX='' THEN NULL  ELSE '('||est.DDD_FAX||') '||est.NUM_FAX END                                                                      AS fax
	FROM delta.`/tmp/case-rfb/estabelecimento` est
	INNER JOIN 
	(
		SELECT  CNPJ_BASICO
		       ,RAZAO_SOCIAL
		FROM delta.`/tmp/case-rfb/empresa`
	) e
	ON e.CNPJ_BASICO = est.CNPJ_BASICO
    AND UF = 'MA'
 INNER JOIN 
  (
    SELECT COD_PAIS, NOME_PAIS
    FROM delta.`/tmp/case-rfb/pais`
    WHERE COD_PAIS = 105 OR COD_PAIS = 106 or COD_PAIS IS NULL
  ) p 
  ON  est.COD_PAIS = to_number(p.COD_PAIS,999999) OR (est.COD_PAIS IS NULL AND est.NM_CIDADE_EXTERIOR IS NULL)
  left OUTER JOIN 
  (
    SELECT COD_MUNICIPIO, MUNICIPIO
    FROM delta.`/tmp/case-rfb/municipio` 
  ) m ON est.COD_MUNICIPIO = to_number(m.COD_MUNICIPIO,9999999)
  WHERE UF = 'MA'
)
);
"""
    spark.sql(query)
    print('RFB_ESTABELECIMENTO criada com sucesso!')


def createEstabelecimentoExterior():
    query = """
        CREATE TABLE IF NOT EXISTS RFB_ESTABELECIMENTO_EXTERIOR (
	SELECT CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Estabelecimento/',est.CNPJ_BASICO),est.CNPJ_ORDEM),est.CNPJ_DV),'-'),UF) as uri
       ,CONCAT(est.CNPJ_BASICO,CONCAT(est.CNPJ_ORDEM,est.CNPJ_DV))                                AS cnpj 
       ,razao_social 
       ,REPLACE(regexp_replace(est.NOME_FANTASIA,'[^a-zA-Z ]',''),' ','_') as nome_fantasia 
       ,CASE est.EMAIL WHEN '' THEN NULL ELSE est.EMAIL END  AS email 
       ,CASE est.MATRIZ_FILIAL WHEN '1' THEN 'MATRIZ' ELSE 'FILIAL' END as matriz_filial 
       ,'http://www.sefaz.ma.gov.br/resource/RFB/Estabelecimento/'||cnae_fiscal as uri_atividade_economica
       ,CASE WHEN length(DATA_INICIO_ATIV) < 8 THEN NULL WHEN DATA_INICIO_ATIV like '00000000' THEN NULL ELSE /*to_date(est.DATA_INICIO_ATIV,'YYYY-MM-DD')*/est.DATA_INICIO_ATIV END                         AS data_inicio_ativ 
       ,'http://www.sefaz.ma.gov.br/resource/RFB/Endereco/'||REPLACE(regexp_replace(est.NM_CIDADE_EXTERIOR,'[^a-zA-Z ]',''),' ','_')||est.COD_PAIS ||p.NOME_PAIS||est.CEP AS uri_endereco
       ,CASE WHEN est.TELEFONE_1 IS NULL THEN NULL 
	             WHEN est.TELEFONE_1='' THEN NULL  ELSE '('||est.DDD_1||') '||est.TELEFONE_1 END as phone_1 
       ,CASE WHEN est.TELEFONE_2 IS NULL THEN NULL 
	             WHEN est.TELEFONE_2 ='' THEN NULL  ELSE '('||est.DDD_2||') '||est.TELEFONE_2 END as phone_2  
       ,CASE WHEN est.NUM_FAX IS NULL THEN NULL 
	             WHEN est.NUM_FAX='' THEN NULL  ELSE '('||est.DDD_FAX||') '||est.NUM_FAX END as fax 
       ,CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Situacao_Cadastral/',situacao),'-'),est.CNPJ_BASICO),est.CNPJ_ORDEM),est.CNPJ_DV),'-'),data_situacao) AS uri_situacao_cadastral
       ,cnae_fiscal as id_atividade_economica
       ,REPLACE(regexp_replace(est.NM_CIDADE_EXTERIOR,'[^a-zA-Z ]',''),' ','_')||est.COD_PAIS||p.NOME_PAIS||est.CEP                           AS ID_ENDERECO
       ,CASE est.SITUACAO WHEN '01' THEN 'NULA' WHEN '1' THEN 'NULA' WHEN '02' THEN 'ATIVA' WHEN '2' THEN 'ATIVA' WHEN '03' THEN 'SUSPENSA' WHEN '3' THEN 'SUSPENSA' WHEN '04' THEN 'INAPTA' WHEN '4' THEN 'INAPTA' WHEN '08' THEN 'BAIXADA' WHEN '8' THEN 'BAIXADA' END AS situacao
	FROM delta.`/tmp/case-rfb/estabelecimento` est
	INNER JOIN 
	(
		SELECT  CNPJ_BASICO 
		       ,RAZAO_SOCIAL
		FROM delta.`/tmp/case-rfb/empresa` 
	) e
	ON e.CNPJ_BASICO = est.CNPJ_BASICO
	INNER JOIN
	(
		SELECT  COD_PAIS, NOME_PAIS
		FROM delta.`/tmp/case-rfb/pais` 
        WHERE TO_NUMBER(COD_PAIS,99999999) != 105 AND TO_NUMBER(COD_PAIS,99999999) != 106
	) p
    ON est.COD_PAIS = p.COD_PAIS
    left outer JOIN 
	(
		SELECT  COD_MUNICIPIO
		       ,MUNICIPIO
		FROM delta.`/tmp/case-rfb/municipio` 
	) m
	ON est.COD_MUNICIPIO = TO_NUMBER(m.COD_MUNICIPIO,99999999)
    
);
"""

    spark.sql(query)
    print('RFB_ESTABELECIMENTO_EXTERIOR criada com sucesso!')


def createEstrangeiro():
    query = """CREATE TABLE IF NOT EXISTS RFB_ESTRANGEIRO (
    SELECT  'http://www.sefaz.ma.gov.br/resource/RFB/Agente/'||uri_nome as uri_socio
       ,MAX(nome_socio) as nome_socio
       ,max(nome_pais) as nome_pais
       ,uri_nome id_socio
    FROM 
    (
        SELECT  DISTINCT CONCAT(CONCAT('_','-'),REPLACE(regexp_replace(nome_socio,'[^a-zA-Z ]',''),' ','_')) AS uri_nome 
	       ,nome_socio 
	       ,REPLACE(regexp_replace(p.NOME_PAIS,'[^a-zA-Z ]',''),' ','_')                                 AS nome_pais
        FROM delta.`/tmp/case-rfb/socio` s
         
        LEFT OUTER JOIN
        (
            SELECT  COD_PAIS 
		       ,NOME_PAIS
            FROM delta.`/tmp/case-rfb/pais` 
        ) p
        ON p.COD_PAIS = s.COD_PAIS_EXT
        WHERE S.TIPO_SOCIO = '3'  
    ) 
    GROUP BY uri_nome
    );
"""
    spark.sql(query)
    print('RFB_ESTRANGEIRO criada com sucesso!')



def createFaixaEtaria():
    query = """ CREATE TABLE IF NOT EXISTS RFB_FAIXA_ETARIA (
		uri_faixa_etaria INT,
		codigo INT,
		descricao VARCHAR(200)
);
   
"""

    spark.sql(query)
    query2 = """INSERT INTO RFB_FAIXA_ETARIA (uri_faixa_etaria, codigo, descricao) 
   VALUES (1, 1, 'Entre 0 a 12 anos'),
   (2, 2, 'Entre 13 a 20 anos'),
   (3, 3, 'Entre 21 a 30 anos'),
   (4, 4, 'Entre 31 a 40 anos'),
   (5, 5, 'Entre 41 a 50 anos'),
   (6, 6, 'Entre 51 a 60 anos'),
   (7, 7, 'Entre 61 a 70 anos'),
   (8, 8, 'Entre 71 a 80 anos'),
   (9, 9, 'Maiores de 80 anos'),
   (0, 0, 'Não se aplica');
    """
    spark.sql(query2)
    print('RFB_FAIXA_ETARIA criada com sucesso!')



def createNaturezaLegal():
    query = """CREATE TABLE IF NOT EXISTS RFB_NATUREZA_LEGAL (
SELECT  CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Natureza_Legal/',codigo) AS id_uri
       ,codigo
       ,natureza_juridica
       ,id_uri as ID_NATUREZA_LEGAL
FROM 
(
	SELECT  DISTINCT codigo 
	       ,id_uri 
	       ,natureza_juridica
	FROM 
	(
		SELECT  CODIGO 
		       ,NATUREZA_JURIDICA 
		       ,translate ( replace(UPPER(NATUREZA_JURIDICA),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri 
		       --,replace(CODIGO,'-','') AS id
		FROM delta.`/tmp/case-rfb/nat_ju` 
	) nat_jur 
) 
);
"""
    spark.sql(query)
    print('RFB_NATUREZA_LEGAL criada com sucesso!')



def createOpcaoMei():
    query = """CREATE TABLE IF NOT EXISTS RFB_OPCAO_MEI (
SELECT  CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Mei/',CONCAT(CONCAT(OPC_MEI ,'-'),root_cnpj)) AS uri 
       ,tipo_mei 
       ,data_exc_mei 
       ,data_opc_mei 
       ,CONCAT(CONCAT(OPC_MEI ,'-'),root_cnpj) as id_opcao_mei
FROM 
(
SELECT DISTINCT s.CNPJ_BASICO                                                                                                                     AS root_cnpj 
	,OPC_MEI                                                                                                                         AS tipo_mei 
	,CASE WHEN OPC_MEI = '' THEN 'OUTROS' 
			WHEN OPC_MEI = 'S' THEN 'SIM' 
			WHEN OPC_MEI = 'N' THEN 'NAO'  ELSE 'OUTROS' END                                                                      AS OPC_MEI 
	,CASE DATA_OPC_MEI WHEN '' THEN NULL WHEN '00000000' THEN NULL ELSE DATA_OPC_MEI END AS data_OPC_MEI 
	,CASE DATA_EXC_MEI WHEN '' THEN NULL WHEN '00000000' THEN NULL ELSE DATA_EXC_MEI END AS data_EXC_MEI
FROM delta.`/tmp/case-rfb/simei` s 
INNER JOIN
delta.`/tmp/case-rfb/estabelecimento` e
ON
s.CNPJ_BASICO = e.CNPJ_BASICO
AND
UF = 'MA'
) );

"""

    spark.sql(query)
    print('RFB_OPCAO_MEI criada com sucesso!')


def createOpcaoSimples():
    query = """CREATE TABLE IF NOT EXISTS RFB_OPCAO_SIMPLES (
SELECT  CONCAT(CONCAT(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Simples/',opc_simples),'-'),root_cnpj) as uri
       ,simples_type 
       ,class_simples
       ,data_exc_simples 
       ,data_opc_simples
       ,CONCAT(CONCAT(opc_simples,'-'),root_cnpj) as id_opcao_simples
FROM 
(
	SELECT DISTINCT s.CNPJ_BASICO                                                                                                                             AS root_cnpj 
	       ,OPC_SIMPLES                                                                                                                             AS simples_type 
	       ,CASE WHEN OPC_SIMPLES = '' THEN 'OUTROS' 
	             WHEN OPC_SIMPLES = 'S' THEN 'SIM' 
	             WHEN OPC_SIMPLES = 'N' THEN 'NAO'  ELSE 'OUTROS' END                                                                          AS opc_simples 
	       ,CASE DATA_OPC_SIMPLES WHEN '' THEN NULL WHEN '00000000' THEN NULL ELSE DATA_OPC_SIMPLES END AS data_opc_simples 
	       ,CASE DATA_EXC_SIMPLES WHEN '' THEN NULL WHEN '00000000' THEN NULL ELSE DATA_EXC_SIMPLES END AS data_exc_simples 
	       ,CASE WHEN OPC_SIMPLES = '' THEN 'OTHERS' 
	             WHEN OPC_SIMPLES = 'S' THEN 'YES' 
	             WHEN OPC_SIMPLES = 'N' THEN 'NO'  ELSE 'OUTROS' END                                                                           AS class_simples
	FROM delta.`/tmp/case-rfb/simei` s
    INNER JOIN
delta.`/tmp/case-rfb/estabelecimento` e
ON
s.CNPJ_BASICO = e.CNPJ_BASICO
AND
UF = 'MA' 
) );
"""

    spark.sql(query)
    print('RFB_OPCAO_SIMPLES criada com sucesso!')



def createPessoa():
    query = """CREATE TABLE IF NOT EXISTS RFB_PESSOA (
    SELECT DISTINCT CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Pessoa/',CONCAT(CONCAT(max(uri_cpf),'-'),max(uri_nome))) as uri
       ,cnpj_cpf_socio  
       ,nome_socio 
       ,max(faixa_etaria)
       ,CONCAT(CONCAT(max(uri_cpf),'-'),max(uri_nome)) as id_pessoa
    FROM 
    (
        SELECT  regexp_replace(CNPJ_CPF_SOCIO,'[^0-9]','_')                 AS uri_cpf 
                ,CNPJ_CPF_SOCIO
                ,REPLACE(regexp_replace(NOME_SOCIO,'[^a-zA-Z ]',''),' ','_') AS uri_nome 
                ,NOME_SOCIO 
                ,max(FAIXA_ETARIA)                                                AS faixa_etaria
        FROM delta.`/tmp/case-rfb/socio` s
        LEFT JOIN
        delta.`/tmp/case-rfb/estabelecimento` e
        ON
        s.CNPJ_BASICO = e.CNPJ_BASICO
        AND
        UF = 'MA'
        WHERE tipo_socio = '2'
        and nome_socio is not null
        group by cnpj_cpf_socio, nome_socio UNION 
        SELECT regexp_replace(CPF_REPRES,'[^0-9]','_')                   AS uri_cpf 
	       ,CPF_REPRES                                                   AS cnpj_cpf_socio 
	       ,REPLACE(regexp_replace(NOME_REPRES,'[^a-zA-Z ]',''),' ','_') AS uri_nome 
	       ,CASE NOME_REPRES WHEN '' THEN NULL ELSE NOME_REPRES END      AS nome_socio 
	       ,case when nome_repres like max(nome_socio) then max(faixa_etaria) else '0' end AS faixa_etaria
        FROM delta.`/tmp/case-rfb/socio`
        --WHERE cpf_repres != ''  
        WHERE nome_repres is not null
        group by cpf_repres, nome_repres --order by 2
    )
    group by cnpj_cpf_socio, nome_socio
    );
"""
    spark.sql(query)
    print('RFB_PESSOA criada com sucesso!')


def createQualificacaoSocio():
    query = """CREATE TABLE IF NOT EXISTS RFB_QUALIFICACAO (
SELECT CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Qualificacao/',id_uri) AS id_uri, cast(COD as int), DESCRICAO, id_uri as id_qualificacao
FROM 
(SELECT DISTINCT id_uri,COD,DESCRICAO 
FROM (
  select COD, DESCRICAO,
			translate(replace(UPPER(TRIM(DESCRICAO)),' ','_'),
				'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
				'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') as id_uri 
		from delta.`/tmp/case-rfb/qualif_socio`
    ) ) 
    );
"""
    spark.sql(query)
    print('RFB_QUALIFICACAO criada com sucesso!')  


def createRazaoSituacaoCadastral():
    query = """
        CREATE TABLE IF NOT EXISTS RFB_RAZAO_SITUACAO_CADASTRAL (
SELECT CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Razao_Situacao_Cadastral/',id_uri) as id_uri, Descricao,Codigo, id_uri as id_razao_situacao_cadastral FROM (
SELECT DISTINCT id_uri,Descricao,Codigo FROM 
(
    select Codigo, Descricao, translate(replace(UPPER(trim(descricao)),' ','_'),
        '¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') as id_uri from delta.`/tmp/case-rfb/razao_situacao`
)
)
);
"""
    spark.sql(query)
    print('RFB_RAZAO_SITUACAO_CADASTRAL criada com sucesso!')


def createSituacaoCadastral():
    query = """CREATE TABLE IF NOT EXISTS RFB_SITUACAO_CADASTRAL (
SELECT CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Situacao_Cadastral/',situacao),'-'),cnpj),'-'),data_situacao) AS uri,val_data_situacao,situacao, (situacao||'-'||cnpj||'-'||data_situacao) as id_situacao_cadastral, id_uri  FROM (
SELECT DISTINCT CONCAT(CNPJ_BASICO,CONCAT(CNPJ_ORDEM,CNPJ_DV)) as cnpj,
  REPLACE(regexp_replace(nome_fantasia, '[^a-zA-Z ]', ''),' ','_') as nome_uri,
				CASE WHEN length(data_situacao) < 8 THEN NULL WHEN data_situacao = '00000000' THEN NULL ELSE data_situacao END as data_situacao,
                CASE WHEN length(data_situacao) < 8 THEN NULL WHEN data_situacao = '00000000' THEN NULL ELSE data_situacao END as val_data_situacao,
                CASE situacao WHEN '1' THEN 'NULA' when '01' then 'NULA'
					WHEN '2' THEN 'ATIVA' WHEN '02' THEN 'ATIVA'
					WHEN '3' THEN 'SUSPENSA' WHEN '03' THEN 'SUSPENSA'
					WHEN '4' THEN 'INAPTA' WHEN '04' THEN 'INAPTA'
					WHEN '8' THEN 'BAIXADA' WHEN '08' THEN 'BAIXADA'
					ELSE 'NULL'
				END as situacao,
				id_uri
			FROM delta.`/tmp/case-rfb/estabelecimento` LEFT OUTER JOIN 
            (select codigo, descricao, translate(replace(UPPER(trim(descricao)),' ','_'),
                '¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
                'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') as id_uri from delta.`/tmp/case-rfb/razao_situacao`)
            motiv_situacao ON delta.`/tmp/case-rfb/estabelecimento`.motivo_situacao = motiv_situacao.codigo
	    WHERE UF = 'MA' ) 
        );
"""
    spark.sql(query)
    print('RFB_SITUACAO_CADASTRAL criada com sucesso!')

def createSituacaoEspecial():
    query = """CREATE TABLE IF NOT EXISTS RFB_SITUACAO_ESPECIAL (
SELECT  CONCAT(CONCAT(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Situacao_Especial/',root_cnpj),'-'),data_sit_especial) AS uri
       ,data_sit  
       ,sit_especial
       ,(root_cnpj||'-'||data_sit_especial) as id_situacao_especial
FROM 
(
	SELECT  DISTINCT CNPJ_BASICO                                                                                                    AS root_cnpj 
	       ,CASE DATA_SIT_ESPECIAL WHEN '00000000' THEN NULL ELSE DATA_SIT_ESPECIAL END AS data_sit_especial 
	       ,CASE DATA_SIT_ESPECIAL WHEN '00000000' THEN NULL ELSE DATA_SIT_ESPECIAL END                       AS data_sit 
	       ,SIT_ESPECIAL
	FROM delta.`/tmp/case-rfb/estabelecimento`
	WHERE MATRIZ_FILIAL = '1' 
	AND SIT_ESPECIAL is not null 
	AND UF = 'MA'
)
);
"""
    spark.sql(query)
    print('RFB_SITUACAO_ESPECIAL criada com sucesso!')


def createSociedadeEstrangeiro():
    query = """
        CREATE TABLE IF NOT EXISTS RFB_SOCIEDADE_COM_ESTRANGEIRO (
SELECT  MAX(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Sociedade/',id_uri),'-'),root_cnpj),'-'),uri_socio)) AS uri 
       ,MAX(data_entrada) as data_inicio_sociedade
       ,MAX(descricao)  as rotulo
       ,MAX(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Qualificacao/',rep_quali)) as uri_qualificacao_representante
       ,MAX(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Pessoa/',(uri_socio)))  as uri_socio
       ,MAX (CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Pessoa/',(CONCAT(CONCAT(cpf_rep,'-'),nome_rep)))) as uri_representante
       ,MAX(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Qualificacao/',(id_uri))) as uri_qualificacao_socio
       ,MAX(rep_quali) AS id_qualificacao_representante
       ,MAX(uri_socio) AS id_socio
       ,MAX(id_uri) AS id_uri
       ,MAX(root_cnpj) AS cnpj_raiz
       ,MAX(CONCAT(CONCAT(cpf_rep,'-'),nome_rep)) AS id_representante
       ,id_sociedade

FROM 
(
	SELECT  DISTINCT s.DATA_ENTRADA as data_entrada
				 ,CONCAT(CONCAT(q.DESCRICAO,': '),s.NOME_SOCIO)                                                        AS descricao 
	       ,r.id_uri                                                                                             AS rep_quali 
	       ,REPLACE(regexp_replace(s.NOME_REPRES,'[^a-zA-Z ]',''),' ','_')                                       AS nome_rep 
	       ,regexp_replace(s.CPF_REPRES,'[^0-9]','_')                                                            AS cpf_rep 
	       ,s.CNPJ_BASICO                                                                                        AS root_cnpj 
	       ,q.id_uri                                                                                             AS id_uri 
	       ,CONCAT(CONCAT('_','-'),REPLACE(regexp_replace(s.NOME_SOCIO,'[^a-zA-Z ]',''),' ','_'))                AS uri_socio
		,q.id_uri||'-'||root_cnpj||'-'||CONCAT(CONCAT('_','-'),REPLACE(regexp_replace(s.NOME_SOCIO,'[^a-zA-Z ]',''),' ','_')) as id_sociedade
	FROM delta.`/tmp/case-rfb/socio` s
	INNER JOIN 
	(
		SELECT  cod 
		       ,descricao 
		       ,translate (replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
		FROM delta.`/tmp/case-rfb/qualif_socio` 
	) q
	ON s.cod_qualificacao = q.COD
	LEFT OUTER JOIN -- Para permitir que representantes possam ser nulos
	(
		SELECT  cod 
		       ,descricao 
		       ,translate (replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
		FROM delta.`/tmp/case-rfb/qualif_socio` 
	) r
	ON s.cod_qualif_repres = r.COD
	LEFT OUTER JOIN
	delta.`/tmp/case-rfb/estabelecimento` e
	ON
	s.CNPJ_BASICO = e.CNPJ_BASICO
	AND
	UF = 'MA'
	WHERE tipo_socio = '3'  
)
GROUP BY  (id_sociedade)
);

"""
    spark.sql(query)
    print('RFB_SOCIEDADE_COM_ESTRANGEIRO criada com sucesso!')

def createSociedadeHolding():
    query = """CREATE TABLE IF NOT EXISTS RFB_SOCIEDADE_COM_HOLDING (
    SELECT  MAX(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Sociedade/',id_uri),'-'),root_cnpj),'-'),cnpj_cpf_socio)) AS uri 
       ,MAX(descricao) as descricao
       ,MAX(data_entrada) as data_inicio_sociedade 
       ,MAX(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Qualificacao/',rep_quali)) as uri_qualificacao_representante
       ,MAX(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Empresa/',cnpj_cpf_socio)) as uri_socio
       -- MAX(CONCAT(CONCAT(cnpj_cpf_socio,'-'),nome_socio)) as uri_representante
       ,MAX(CONCAT ('http://www.sefaz.ma.gov.br/resource/RFB/Pessoa/',(CONCAT(CONCAT(cpf_rep,'-'),nome_rep)))) as uri_representante
       ,MAX(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Qualificacao/',(id_uri))) as uri_qualificacao_socio
       ,MAX((id_uri||'-'||root_cnpj||'-'||cnpj_cpf_socio)) AS id_sociedade
       ,MAX(id_uri) AS id_qualificacao_socio
       ,MAX(rep_quali) AS id_qualificacao_representante
       ,MAX(cnpj_cpf_socio) AS id_socio
       ,MAX(CONCAT(CONCAT(cpf_rep,'-'),nome_rep)) AS id_representante
    FROM 
    (
        SELECT  DISTINCT CASE WHEN length(s.DATA_ENTRADA) < 8 THEN NULL WHEN s.DATA_ENTRADA like '00000000' THEN NULL ELSE s.DATA_ENTRADA END as data_entrada
	       ,CONCAT(CONCAT(q.DESCRICAO,': '),s.nome_socio)                                                        AS descricao 
	       ,r.id_uri                                                                                             AS rep_quali 
	       ,REPLACE(regexp_replace(s.nome_repres,'[^a-zA-Z ]',''),' ','_')                                       AS nome_rep 
	       ,regexp_replace(s.cpf_repres,'[^0-9]','_')                                                            AS cpf_rep 
	       ,s.CNPJ_BASICO                                                                                        AS root_cnpj 
	       ,q.id_uri                                                                                             AS id_uri 
	       ,s.cnpj_cpf_socio                                                                         AS cnpj_cpf_socio 
	       ,REPLACE(regexp_replace(s.nome_socio,'[^a-zA-Z ]',''),' ','_')                                        AS nome_socio
        FROM 
        (
            select cnpj_basico, cnpj_cpf_socio, nome_socio, cpf_repres, nome_repres, data_entrada, cod_qualificacao, cod_qualif_repres
            from delta.`/tmp/case-rfb/socio` 
            WHERE tipo_socio = '1'
        ) s
        INNER JOIN 
        (
            SELECT  cod
		       ,descricao 
		       ,translate (replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
            FROM delta.`/tmp/case-rfb/qualif_socio` 
        ) q
        ON s.cod_qualificacao = q.COD
    --or s.cod_qualif_repres = q.COD
        LEFT OUTER JOIN -- Para permitir que representantes possam ser nulos
        (
            SELECT  cod
		       ,descricao 
		       ,translate (replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
            FROM delta.`/tmp/case-rfb/qualif_socio` 
        ) r
        ON s.cod_qualif_repres = r.COD
	LEFT OUTER JOIN
	delta.`/tmp/case-rfb/estabelecimento` e
	ON
	s.CNPJ_BASICO = e.CNPJ_BASICO
	AND
	UF = 'MA'

    )
    GROUP BY  (id_uri||'-'||root_cnpj||'-'||cnpj_cpf_socio)
    );
"""
    spark.sql(query)
    print('RFB_SOCIEDADE_COM_HOLDING criada com sucesso!')


def createSociedadePessoaFisica():
    query = """    CREATE TABLE IF NOT EXISTS RFB_SOCIEDADE_COM_PESSOA_FISICA
    SELECT 'http://www.sefaz.ma.gov.br/resource/RFB/Sociedade/'||(id_uri||'-'||root_cnpj||'-'||cnpj_cpf_socio||'-'||nome_socio) AS uri
       ,descricao as ROTULO
       ,data_entrada as DATA_INICIO_SOCIEDADE
       ,CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Pessoa/',(CONCAT(CONCAT(cnpj_cpf_socio,'-'),nome_socio))) AS uri_socio
       ,CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Qualificacao/',id_uri) AS uri_qualificacao_socio
       ,CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Pessoa/',CONCAT(CONCAT(cnpj_cpf_representante,'-'),nome_representante)) AS uri_representante
       ,CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Qualificacao/',id_uri_representante) AS uri_qualificacao_representante
       ,(id_uri||'-'||root_cnpj||'-'||cnpj_cpf_socio||'-'||nome_socio) AS id_sociedade
       ,id_uri AS id_qualificacao_socio
       ,id_uri_representante AS id_qualificacao_representante
       ,CONCAT(CONCAT(cnpj_cpf_representante,'-'),nome_representante) AS id_representante
       ,(cnpj_cpf_socio||'-'||nome_socio) AS id_socio
    FROM 
    (
        
        SELECT CASE WHEN length(s.DATA_ENTRADA) < 8 THEN NULL WHEN s.DATA_ENTRADA like '00000000' THEN NULL ELSE s.DATA_ENTRADA END as data_entrada
	       ,s.CNPJ_BASICO                                                                                        AS root_cnpj 
	       ,CONCAT(CONCAT(q.DESCRICAO,': '),s.nome_socio)                                                        AS descricao 
	       ,q.id_uri                                                                                             AS id_uri 
	       ,regexp_replace(s.cnpj_cpf_socio,'[^0-9]','_')                                                        AS cnpj_cpf_socio 
	       ,REPLACE(regexp_replace(s.nome_socio,'[^a-zA-Z ]',''),' ','_')                                        AS nome_socio 
	       ,regexp_replace(s.CPF_REPRES,'[^0-9]','_')                                                            AS cnpj_cpf_representante 
	       ,REPLACE(regexp_replace(s.NOME_REPRES,'[^a-zA-Z ]',''),' ','_')                                       AS nome_representante 
	       ,q.id_uri                                                                                             AS id_uri_representante
        FROM 
        (
            select  cod_qualificacao, cod_qualif_repres, cnpj_basico, nome_socio, cnpj_cpf_socio, cpf_repres, nome_repres, data_entrada
            from delta.`/tmp/case-rfb/socio`
            WHERE tipo_socio = '2'  
        ) s
        INNER JOIN 
        (
            SELECT cod
		       ,descricao 
		       ,translate (replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
            FROM delta.`/tmp/case-rfb/qualif_socio` 
        ) q
        ON s.cod_qualificacao = q.COD /*or s.cod_qualif_repres = q.COD*/
        LEFT OUTER JOIN -- Para permitir que representantes possam ser nulos
        (
            SELECT cod 
		       ,descricao 
		       ,translate (replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
            FROM delta.`/tmp/case-rfb/qualif_socio` 
        ) r
        ON s.cod_qualif_repres = r.COD
	INNER JOIN
	delta.`/tmp/case-rfb/estabelecimento` e
	ON
	s.CNPJ_BASICO = e.CNPJ_BASICO
	AND
	UF = 'MA'

            WHERE nome_socio IS NOT NULL 
    ) aux ;

"""
    spark.sql(query)
    print('RFB_SOCIEDADE_COM_PESSOA_FISICA criada com sucesso!')        

def createTemAtividadeEconomicaSecundaria():
    query = """
DECLARE
    cursor c_new is select cnpj_basico, cnpj_ordem, cnpj_dv, uf, cnae_fiscal_sec from delta.`/tmp/case-rfb/estabelecimento` WHERE UF = 'MA';

    --declaracao das variaveis emp
    cnpj_basico delta.`/tmp/case-rfb/estabelecimento`.cnpj_basico%TYPE;
    cnpj_ordem delta.`/tmp/case-rfb/estabelecimento`.cnpj_ordem%TYPE;
    cnpj_dv delta.`/tmp/case-rfb/estabelecimento`.cnpj_dv%TYPE;
    uf delta.`/tmp/case-rfb/estabelecimento`.UF%type;
    cnae delta.`/tmp/case-rfb/estabelecimento`.cnae_fiscal_sec%type;
    indice_i integer := 1;
    indice_f integer := 1;
    open c_new;
	fetch c_new into cnpj_basico, cnpj_ordem, cnpj_dv,	uf, cnae;

    loop
        exit when c_new%notfound;
        if cnae is not null then
            CREATE TABLE IF NOT EXISTS RFB_TEM_ATIV_ECONOMICA_SECUNDARIA VALUES ('http://www.sefaz.ma.gov.br/resource/RFB/Estabelecimento/'||concat(CNPJ_BASICO,concat(CNPJ_ORDEM, CNPJ_DV)), 'http://www.sefaz.ma.gov.br/resource/RFB/Atividade_Economica/'||SUBSTR(cnae,0,7), concat(CNPJ_BASICO,concat(CNPJ_ORDEM, CNPJ_DV)), SUBSTR(cnae,0,7));
            --DBMS_OUTPUT.put_line('primeiro cnae '|| SUBSTR(cnae,0,7));
            indice_f := instr(cnae,',',1);
            while indice_f > 0 loop
                --DBMS_OUTPUT.put_line('indice i '|| indice_i);
                --DBMS_OUTPUT.put_line('indice f '|| indice_f);
                --sys.DBMS_OUTPUT.PUT_LINE('loop 1 cnpj '|| substr(cnae, indice_i+8, 7));
                --CREATE TABLE IF NOT EXISTS RFB_TEM_ATIV_ECONOMICA_SECUNDARIA VALUES ('http://www.sefaz.ma.gov.br/resource/RFB/Atividade_Economica/'||concat(CNPJ_BASICO,concat(CNPJ_ORDEM, concat(CNPJ_DV, concat('-',UF)))), substr(cnae, indice_i+8, 7));
                insert into RFB_TEM_ATIV_ECONOMICA_SECUNDARIA VALUES ('http://www.sefaz.ma.gov.br/resource/RFB/Estabelecimento/'||concat(CNPJ_BASICO,concat(CNPJ_ORDEM, CNPJ_DV)), 'http://www.sefaz.ma.gov.br/resource/RFB/Atividade_Economica/'||substr(cnae, indice_i+8, 7), concat(CNPJ_BASICO,concat(CNPJ_ORDEM, CNPJ_DV)), substr(cnae, indice_i+8, 7));
                indice_i := indice_f +1;
                indice_f := instr(cnae,',',indice_i);
            end loop;
            indice_i := 1;
        end if;
        fetch c_new into cnpj_basico, cnpj_ordem, cnpj_dv,	uf, cnae;
        exit when c_new%notfound;
    end loop;
    END;
"""
    spark.sql(query)
    print('RFB_TEM_ATIVIDADE_ECONOMICA_SECUNDARIA criada com sucesso!')

def createTemEstabelecimento():
    query = """CREATE TABLE IF NOT EXISTS RFB_TEM_ESTABELECIMENTO (
SELECT  'http://www.sefaz.ma.gov.br/resource/RFB/Empresa/'||root_cnpj_e as uri_empresa
       ,'http://www.sefaz.ma.gov.br/resource/RFB/Estabelecimento/'||cnpj_f as uri_estabelecimento
       ,root_cnpj_e as id_empresa
       ,cnpj_f as id_estabelecimento
FROM 
(
	SELECT  DISTINCT e.CNPJ_RAIZ AS root_cnpj_e 
	       ,f.cnpj             AS cnpj_f 
	FROM RFB_EMPRESA e
	INNER JOIN RFB_ESTABELECIMENTO f
	ON e.CNPJ_RAIZ = SUBSTR(f.CNPJ, 0, 8)
	--WHERE f.matriz_filial = '2'  
) 
);
"""
    spark.sql(query)
    print('RFB_TEM_ESTABELECIMENTO createdo com sucesso!')

def createTemSituacaoEspecial():
    query = """CREATE TABLE IF NOT EXISTS RFB_TEM_SITUACAO_ESPECIAL (
SELECT  CONCAT ('http://www.sefaz.ma.gov.br/resource/RFB/Empresa/',root_cnpj) as uri_empresa
       ,CONCAT ('http://www.sefaz.ma.gov.br/resource/RFB/Situacao_especial/',CONCAT(CONCAT(root_cnpj,'-'),data_sit_especial)) as uri_situacao_especial
       ,root_cnpj AS id_empresa
       ,CONCAT(CONCAT(root_cnpj,'-'),data_sit_especial) AS id_situacao_especial
FROM 
(
	SELECT  e.CNPJ_RAIZ                                                                                                                  AS root_cnpj 
	       ,REPLACE(regexp_replace(e.razao_social,'[^a-zA-Z ]',''),' ','_')                                                                         AS razao_uri 
	       ,CASE est.data_sit_especial WHEN '00000000' THEN null ELSE est.data_sit_especial END AS data_sit_especial
	FROM 
    delta.`/tmp/case-rfb/estabelecimento` est
    INNER JOIN RFB_EMPRESA e
	ON est.cnpj_basico = e.CNPJ_RAIZ
	WHERE est.matriz_filial = '1' 
	AND est.sit_especial is not null 
) 
);
"""

    spark.sql(query)
    print('RFB_TEM_SITUACAO_ESPECIAL criada com sucesso!')


def createTemSociedadeEstrangeiro():
    query = """CREATE TABLE IF NOT EXISTS RFB_TEM_SOCIEDADE_ESTRANGEIRA
SELECT  CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Empresa/',root_cnpj) as uri_empresa
       ,CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Sociedade/',(id_uri||'-'||root_cnpj||'-'||uri_socio)) as uri_sociedade
       ,root_cnpj AS id_empresa
       ,(id_uri||'-'||root_cnpj||'-'||uri_socio) AS id_socio 
FROM 
(
	SELECT  DISTINCT s.CNPJ_BASICO                                                                AS root_cnpj 
	       ,REPLACE(regexp_replace(e.RAZAO_SOCIAL,'[^a-zA-Z ]',''),' ','_')                       AS razao_uri 
	       ,q.id_uri                                                                              AS id_uri 
	       ,CONCAT(CONCAT('_','-'),REPLACE(regexp_replace(s.NOME_SOCIO,'[^a-zA-Z ]',''),' ','_')) AS uri_socio
	FROM delta.`/tmp/case-rfb/socio` s
	INNER JOIN RFB_EMPRESA e
	ON e.CNPJ_RAIZ = s.CNPJ_BASICO
	INNER JOIN 
	(
		SELECT  cod 
		       ,descricao 
		       ,translate (replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
		FROM delta.`/tmp/case-rfb/qualif_socio`
	) q
	ON s.COD_QUALIFICACAO = q.COD
	WHERE tipo_socio = '3'  
) aux;

"""

    spark.sql(query)
    print('RFB_TEM_SOCIEDADE_ESTRANGEIRA criada com sucesso!')


def createTemSociedadeFisica():
    query = """CREATE TABLE IF NOT EXISTS RFB_TEM_SOCIEDADE_FISICA (
SELECT 'http://www.sefaz.ma.gov.br/resource/RFB/Empresa/'||root_cnpj as uri_empresa
       ,'http://www.sefaz.ma.gov.br/resource/RFB/Sociedade/'||(id_uri||'-'||root_cnpj||'-'||cnpj_cpf_socio||'-'||nome_socio) as uri_sociedade
       ,root_cnpj AS id_empresa
       ,(id_uri||'-'||root_cnpj||'-'||cnpj_cpf_socio||'-'||nome_socio) AS id_socio 
FROM 
(
	SELECT  s.CNPJ_BASICO                                        AS root_cnpj 
	       ,REPLACE(regexp_replace(razao_social,'[^a-zA-Z ]',''),' ','_') AS razao_uri 
	       ,q.id_uri                                                      AS id_uri 
	       ,regexp_replace(s.CNPJ_CPF_SOCIO,'[^0-9]','_')                 AS cnpj_cpf_socio 
	       ,REPLACE(regexp_replace(s.NOME_SOCIO,'[^a-zA-Z ]',''),' ','_') AS nome_socio
	FROM delta.`/tmp/case-rfb/socio` s
	INNER JOIN RFB_EMPRESA e
	ON e.CNPJ_RAIZ = s.CNPJ_BASICO
    AND tipo_socio = '2'
	LEFT JOIN 
	(
		SELECT  cod
		       ,descricao  
		       ,translate (replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
		FROM delta.`/tmp/case-rfb/qualif_socio` 
	) q
	ON s.COD_QUALIFICACAO = q.COD
) 
);
"""
    spark.sql(query)
    print('RFB_TEM_SOCIEDADE_FISICA criada com sucesso!')
    

def createTemSociedadeJuridica():
    query = """CREATE TABLE IF NOT EXISTS RFB_TEM_SOCIEDADE_JURIDICA (
SELECT  'http://www.sefaz.ma.gov.br/resource/RFB/Empresa/'||root_cnpj as uri_empresa
       ,'http://www.sefaz.ma.gov.br/resource/RFB/Sociedade/'||(id_uri||'-'||root_cnpj||'-'||cnpj_cpf_socio) as uri_sociedade
       ,root_cnpj AS id_empresa
       ,(id_uri||'-'||root_cnpj||'-'||cnpj_cpf_socio) AS id_socio 
FROM 
(
	SELECT  DISTINCT s.CNPJ_BASICO                                  AS root_cnpj
	       ,REPLACE(regexp_replace(e.RAZAO_SOCIAL,'[^a-zA-Z ]',''),' ','_') AS razao_uri
	       ,q.id_uri                                                      AS id_uri
	       ,SUBSTR(s.CNPJ_CPF_SOCIO,0,8)                                    AS cnpj_cpf_socio
	       ,REPLACE(regexp_replace(s.NOME_SOCIO,'[^a-zA-Z ]',''),' ','_')   AS nome_socio
	FROM delta.`/tmp/case-rfb/socio` s
	INNER JOIN RFB_EMPRESA e
	ON e.CNPJ_RAIZ = s.CNPJ_BASICO
	INNER JOIN 
	(
		SELECT  cod
		       ,descricao
		       ,translate (replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
		FROM delta.`/tmp/case-rfb/qualif_socio`
	) q
	ON s.COD_QUALIFICACAO = q.COD
	WHERE tipo_socio = '1'  
)
);

"""
    spark.sql(query)
    print('RFB_TEM_SOCIEDADE_JURIDICA criada com sucesso!')


def insertAllTables():
    insertRFB_EMPRESA()
    insertRFB_EMPRESA_HOLDING()
    insertRFB_ENDERECO()
    insertRFB_ENDERECO_EXTERIOR()
    insertRFB_ESTABELECIMENTO()
    insertRFB_ESTABELECIMENTO_EXTERIOR()
    insertRFB_ESTRANGEIRO()
    insertRFB_FAIXA_ETARIA()
    insertRFB_NATUREZA_LEGAL()
    insertRFB_OPCAO_MEI()
    insertRFB_OPCAO_SIMPLES()
    insertRFB_PESSOA()
    insertRFB_QUALIFICACAO()
    insertRFB_RAZAO_SITUACAO_CADASTRAL()
    insertRFB_SITUACAO_CADASTRAL()
    insertRFB_SITUACAO_ESPECIAL()
    insertRFB_SOCIEDADE_COM_ESTRANGEIRO()
    insertRFB_SOCIEDADE_COM_HOLDING()
    insertRFB_SOCIEDADE_COM_PESSOA_FISICA()
   # insertRFB_TEM_ATIV_ECONOMICA_SECUNDARIA()
    insertRFB_TEM_ESTABELECIMENTO()
    insertRFB_TEM_SITUACAO_ESPECIAL()
    insertRFB_TEM_SOCIEDADE_ESTRANGEIRA()
    insertRFB_TEM_SOCIEDADE_FISICA()
    insertRFB_TEM_SOCIEDADE_JURIDICA()
    print("Todas as tabelas foram criadas com sucesso!")

def insertRFB_EMPRESA():
    query = """
    INSERT INTO RFB_EMPRESA 
    SELECT 'http://www.sefaz.ma.gov.br/resource/RFB/Empresa/'||root_cnpj as URI,
           root_cnpj AS CNPJ_RAIZ,
           razao_social AS RAZAO_SOCIAL,
           concat('http://www.sefaz.ma.gov.br/resource/RFB/Simples/',CONCAT(CONCAT(opc_simples,'-'),root_cnpj)) AS URI_OPCAO_SIMPLES,
           concat('http://www.sefaz.ma.gov.br/resource/RFB/Mei/',CONCAT(CONCAT(opc_mei,'-'),root_cnpj)) AS URI_OPCAO_MEI,
           capital_social AS CAPITAL_SOCIAL,
           tem_porte AS TEM_PORTE,
           ente_federativo AS ENTE_FEDERATIVO,
           CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Qualificacao/',id_qualif) AS URI_QUALIFICACAO_REPRESENTANTE,
           CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Natureza_Legal/',id_nat) AS URI_NATUREZA_LEGAL,
           CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Estabelecimento/',uri_est_mat) as URI_ESTABELECIMENTO_MATRIZ,
           CONCAT(CONCAT(opc_simples,'-'),root_cnpj) as ID_OPCAO_SIMPLES,
           CONCAT(CONCAT(opc_mei,'-'),root_cnpj) as ID_OPCAO_MEI,
           id_qualif AS ID_QUALIFICACAO_REPRESENTANTE,
           id_nat AS ID_NATUREZA_LEGAL,
           uri_est_mat AS ID_ESTABELECIMENTO_MATRIZ
    FROM (
        SELECT e.CNPJ_BASICO AS root_cnpj,
               REPLACE(regexp_replace(nome_fantasia,'[^a-zA-Z ]',''),' ','_') AS nome_uri,
               REPLACE(regexp_replace(e.RAZAO_SOCIAL ,'[^a-zA-Z ]',''),' ','_') AS razao_uri,
               e.RAZAO_SOCIAL AS razao_social,
               CASE WHEN s.OPC_SIMPLES = '' THEN 'OUTROS' 
                    WHEN s.OPC_SIMPLES = 'S' THEN 'SIM' 
                    WHEN s.OPC_SIMPLES = 'N' THEN 'NAO' 
                    ELSE 'OUTROS' END AS opc_simples,
               CASE WHEN s.OPC_MEI = '' THEN 'OUTROS' 
                    WHEN s.OPC_MEI = 'S' THEN 'SIM' 
                    WHEN s.OPC_MEI = 'N' THEN 'NAO' 
                    ELSE 'OUTROS' END AS opc_mei,
               q.id_uri AS id_qualif,
               n.id_uri AS id_nat,
               CASE WHEN e.CAPITAL_SOCIAL IS NULL THEN 0 END AS capital_social,
               CASE WHEN e.PORTE = '1' OR e.PORTE = '01' THEN 'NAO_INFORMADO' 
                    WHEN e.PORTE = '2' OR e.PORTE = '02' THEN 'MICRO_EMPRESA' 
                    WHEN e.PORTE = '3' OR e.PORTE = '03' THEN 'EMPRESA_DE_PEQUENO_PORTE' 
                    WHEN e.PORTE = '5' OR e.PORTE = '05' THEN 'DEMAIS' END AS tem_porte,
               e.ENTE_FEDERATIVO AS ente_federativo,
               CONCAT(est.CNPJ_BASICO,CONCAT(est.CNPJ_ORDEM,est.CNPJ_DV)) AS uri_est_mat
        FROM delta.`/tmp/case-rfb/empresa` e
        INNER JOIN (SELECT cod, descricao, translate(replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
                    FROM delta.`/tmp/case-rfb/qualif_socio`) q ON e.qualif_resp = q.COD
        LEFT OUTER JOIN (SELECT codigo, natureza_juridica, translate(replace(UPPER(natureza_juridica),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
                         FROM delta.`/tmp/case-rfb/nat_ju`) n ON e.cod_nat_juridica = n.id
        INNER JOIN (SELECT CNPJ_BASICO, CNPJ_ORDEM, CNPJ_DV, MATRIZ_FILIAL, NOME_FANTASIA, UF 
                    FROM delta.`/tmp/case-rfb/estabelecimento` 
                    WHERE MATRIZ_FILIAL = '1' AND UF = 'MA') est ON e.CNPJ_BASICO = est.CNPJ_BASICO
        LEFT OUTER JOIN (SELECT CNPJ_BASICO, OPC_SIMPLES, OPC_MEI 
                         FROM delta.`/tmp/case-rfb/simei`) s ON e.CNPJ_BASICO = s.CNPJ_BASICO
    )
    """
    spark.sql(query)
    print('Dados inseridos em RFB_EMPRESA com sucesso!')

# Repita o padrao acima para as demais tabelas
def insertRFB_EMPRESA_HOLDING():
    query = """
    INSERT INTO RFB_EMPRESA_HOLDING 
    SELECT DISTINCT RAZAO_SOCIAL, CNPJ_SOCIO, CNPJ_RAIZ
    FROM (
        SELECT DISTINCT NOME_SOCIO AS RAZAO_SOCIAL, CNPJ_CPF_SOCIO AS CNPJ_SOCIO, soc.CNPJ_BASICO AS CNPJ_RAIZ
        FROM delta.`/tmp/case-rfb/socio` soc
        INNER JOIN delta.`/tmp/case-rfb/estabelecimento` EST ON MATRIZ_FILIAL = '1' AND UF = 'MA' AND soc.CNPJ_BASICO = est.CNPJ_BASICO
        WHERE TIPO_SOCIO = '1'
    )
    """
    spark.sql(query)
    print('Dados inseridos em RFB_EMPRESA_HOLDING com sucesso!')

def insertRFB_ENDERECO():
    query = """
    INSERT INTO RFB_ENDERECO 
    SELECT CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Endereco/',e.CNPJ) as uri_endereco,
           CASE e.UF WHEN 'AC' THEN 'ACRE' WHEN 'AL' THEN 'ALAGOAS' WHEN 'AP' THEN 'AMAPA' WHEN 'AM' THEN 'AMAZONAS'
                     WHEN 'BA' THEN 'BAHIA' WHEN 'CE' THEN 'CEARA' WHEN 'ES' THEN 'ESPIRITO_SANTO' WHEN 'GO' THEN 'GOIAS'
                     WHEN 'MA' THEN 'MARANHAO' WHEN 'MT' THEN 'MATO_GROSSO' WHEN 'MS' THEN 'MATO_GROSSO DO SUL'
                     WHEN 'MG' THEN 'MINAS_GERAIS' WHEN 'PA' THEN 'PARA' WHEN 'PB' THEN 'PARAIBA' WHEN 'PR' THEN 'PARANA'
                     WHEN 'PE' THEN 'PERNAMBUCO' WHEN 'PI' THEN 'PIAUI' WHEN 'RJ' THEN 'RIO_DE_JANEIRO'
                     WHEN 'RN' THEN 'RIO_GRANDE_DO_NORTE' WHEN 'RS' THEN 'RIO_GRANDE_DO_SUL' WHEN 'RO' THEN 'RONDONIA'
                     WHEN 'RR' THEN 'RORAIMA' WHEN 'SC' THEN 'SANTA_CATARINA' WHEN 'SP' THEN 'SAO_PAULO'
                     WHEN 'SE' THEN 'SERGIPE' WHEN 'TO' THEN 'TOCANTINS' WHEN 'DF' THEN 'DISTRITO_FEDERAL' ELSE NULL END AS uf,
           CASE WHEN m.MUNICIPIO = '' THEN NULL ELSE regexp_replace(m.MUNICIPIO,'[^a-zA-Z ]','') END AS municipio,
           CASE WHEN e.LOGRADOURO = '' THEN NULL ELSE regexp_replace(CONCAT(CONCAT(e.TIPO_LOGRADOURO,' '),e.LOGRADOURO),'[^a-zA-Z0-9 ]','') END AS logradouro,
           CASE WHEN e.NUMERO = '' THEN NULL ELSE numero END AS numero, CASE WHEN e.COMPLEMENTO = '' THEN NULL ELSE complemento END AS complemento,
           CASE WHEN e.CEP = '' THEN NULL ELSE cep END AS cep, CASE WHEN e.BAIRRO = '' THEN NULL ELSE regexp_replace(e.BAIRRO,'[^a-zA-Z ]','') END AS bairro,
           CASE WHEN p.NOME_PAIS = '' OR p.NOME_PAIS IS NULL THEN 'BRASIL' ELSE regexp_replace(p.NOME_PAIS,'[^a-zA-Z ]','') END AS nome_pais,
           e.CNPJ AS id_endereco, REGEXP_REPLACE(regexp_replace(m.MUNICIPIO,'[^a-zA-Z ]',''),' ','_')||'-'||e.UF AS id_cidade,
           e.CEP||'-'||UPPER(REGEXP_REPLACE(TRANSLATE(regexp_replace(CONCAT(CONCAT(e.TIPO_LOGRADOURO,' '),e.LOGRADOURO),'[^a-zA-Z0-9 ]',''), '¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
            'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy'),' ','_'))||'-'||UPPER(REGEXP_REPLACE(TRANSLATE(regexp_replace(e.BAIRRO,'[^a-zA-Z ]',''), '¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
            'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy'),' ','_')) AS id_logradouro,
           UPPER(REGEXP_REPLACE(TRANSLATE(regexp_replace(e.BAIRRO,'[^a-zA-Z ]',''), '¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
            'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy'),' ','_'))||'-'||UPPER(REGEXP_REPLACE(TRANSLATE(regexp_replace(m.MUNICIPIO,'[^a-zA-Z ]',''), '¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
            'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy'),' ','_')) AS id_bairro, e.UF AS uf_abreviado
    FROM (SELECT CEP, LOGRADOURO, BAIRRO, NUMERO, UF, COMPLEMENTO, COD_PAIS, TIPO_LOGRADOURO, COD_MUNICIPIO, CONCAT(CNPJ_BASICO,CONCAT(CNPJ_ORDEM,CNPJ_DV)) AS CNPJ
          FROM delta.`/tmp/case-rfb/estabelecimento` WHERE NM_CIDADE_EXTERIOR = '' or NM_CIDADE_EXTERIOR IS NULL AND UF = 'MA') e
    INNER JOIN (SELECT DISTINCT COD_PAIS, NOME_PAIS FROM delta.`/tmp/case-rfb/pais` where cod_pais in (105,106)) p ON (case when e.COD_PAIS is null then 105 end) = to_number(p.COD_PAIS,99999)
    INNER JOIN (SELECT DISTINCT COD_MUNICIPIO, MUNICIPIO FROM delta.`/tmp/case-rfb/municipio`) m ON e.COD_MUNICIPIO = to_number(m.COD_MUNICIPIO,99999999)
    """
    spark.sql(query)
    print('Dados inseridos em RFB_ENDERECO com sucesso!')

def insertRFB_ENDERECO_EXTERIOR():
    query = """
    INSERT INTO RFB_ENDERECO_EXTERIOR 
    SELECT DISTINCT CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Endereco/',aux.id_endereco) as uri, MAX(aux.cep), MAX(aux.nome_pais),
           MAX(CONCAT(CONCAT(aux.nm_cidade_exterior,'-'),aux.nome_pais)) AS city_ext, aux.id_endereco AS ID_ENDERECO
    FROM (
        SELECT DISTINCT CASE WHEN e.CEP = '' THEN NULL ELSE cep END AS cep,
               CASE WHEN p.NOME_PAIS = '' OR p.NOME_PAIS IS NULL THEN 'BRASIL' ELSE REPLACE(regexp_replace(p.NOME_PAIS,'[^a-zA-Z ]',''),' ','_') END AS nome_pais,
               CONCAT(CNPJ_BASICO,CONCAT(CNPJ_ORDEM,CNPJ_DV)) AS ID_ENDERECO, e.NM_CIDADE_EXTERIOR
        FROM delta.`/tmp/case-rfb/estabelecimento` e
        INNER JOIN (SELECT COD_PAIS, NOME_PAIS FROM delta.`/tmp/case-rfb/pais`) p ON e.COD_PAIS = p.COD_PAIS
        WHERE e.NM_CIDADE_EXTERIOR != '' or e.NM_CIDADE_EXTERIOR IS NOT NULL
    ) aux GROUP BY aux.id_endereco
    """
    spark.sql(query)
    print('Dados inseridos em RFB_ENDERECO_EXTERIOR com sucesso!')

def insertRFB_ESTABELECIMENTO():
    query = """
    INSERT INTO RFB_ESTABELECIMENTO 
    SELECT DISTINCT cnpj, razao_social, nome_fantasia, email, matriz_filial as tipo_estabelecimento,
           CONCAT ('http://www.sefaz.ma.gov.br/resource/RFB/Situacao_Cadastral/',cnae_fiscal) as uri_atividade_economica, data_inicio_ativ,
           concat('http://www.sefaz.ma.gov.br/resource/RFB/Endereco/',id_endereco) as uri_endereco_nacional, NULL, phone_1 as telefone_1,
           phone_2 as telefone_2, fax, CONCAT ('http://www.sefaz.ma.gov.br/resource/RFB/Situacao_Cadastral/',CONCAT(CONCAT(CONCAT(CONCAT(situacao,'-'),cnpj),'-'),data_situacao)) as uri_situacao,
           CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Estabelecimento/',cnpj) as uri, cnae_fiscal as id_atividade_economica, id_endereco,
           CONCAT(CONCAT(CONCAT(CONCAT(situacao,'-'),cnpj),'-'),data_situacao) as id_situacao 
    FROM (
        SELECT CONCAT(est.CNPJ_BASICO,CONCAT(est.CNPJ_ORDEM,est.CNPJ_DV)) AS cnpj, REPLACE(regexp_replace(est.NOME_FANTASIA,'[^a-zA-Z ]',''),' ','_') AS nome_uri,
               e.RAZAO_SOCIAL AS razao_social, est.UF as UF, CASE est.NOME_FANTASIA WHEN '' THEN NULL ELSE est.NOME_FANTASIA END AS nome_fantasia,
               CASE WHEN length(data_situacao) < 8 THEN NULL WHEN data_situacao like '00000000' THEN NULL ELSE data_situacao END as data_situacao,
               CASE est.SITUACAO WHEN '01' THEN 'NULA' WHEN '1' THEN 'NULA' WHEN '02' THEN 'ATIVA' WHEN '2' THEN 'ATIVA'
                    WHEN '03' THEN 'SUSPENSA' WHEN '3' THEN 'SUSPENSA' WHEN '04' THEN 'INAPTA' WHEN '4' THEN 'INAPTA'
                    WHEN '08' THEN 'BAIXADA' WHEN '8' THEN 'BAIXADA' END AS situacao, CASE est.EMAIL WHEN '' THEN NULL ELSE est.EMAIL END AS email,
               CASE est.MATRIZ_FILIAL WHEN '1' THEN 'MATRIZ' ELSE 'FILIAL' END AS matriz_filial, est.CNAE_FISCAL,
               CONCAT(CONCAT(est.CNPJ_BASICO,est.CNPJ_ORDEM),est.CNPJ_DV) AS id_endereco, CASE WHEN length( est.DATA_INICIO_ATIV) < 8 THEN NULL WHEN  est.DATA_INICIO_ATIV like '00000000' THEN NULL ELSE est.DATA_INICIO_ATIV END AS data_inicio_ativ,
               CASE WHEN est.TELEFONE_1 IS NULL THEN NULL WHEN est.TELEFONE_1='' THEN NULL ELSE '('||est.DDD_1||') '||est.TELEFONE_1 END AS phone_1,
               CASE WHEN est.TELEFONE_2 IS NULL THEN NULL WHEN est.TELEFONE_2 ='' THEN NULL ELSE '('||est.DDD_2||') '||est.TELEFONE_2 END AS phone_2,
               CASE WHEN est.NUM_FAX IS NULL THEN NULL WHEN est.NUM_FAX='' THEN NULL ELSE '('||est.DDD_FAX||') '||est.NUM_FAX END AS fax
        FROM delta.`/tmp/case-rfb/estabelecimento` est
        INNER JOIN (SELECT CNPJ_BASICO, RAZAO_SOCIAL FROM delta.`/tmp/case-rfb/empresa`) e ON e.CNPJ_BASICO = est.CNPJ_BASICO AND UF = 'MA'
        INNER JOIN (SELECT COD_PAIS, NOME_PAIS FROM delta.`/tmp/case-rfb/pais` WHERE COD_PAIS = 105 OR COD_PAIS = 106 or COD_PAIS IS NULL) p ON est.COD_PAIS = to_number(p.COD_PAIS,999999) OR (est.COD_PAIS IS NULL AND est.NM_CIDADE_EXTERIOR IS NULL)
        LEFT OUTER JOIN (SELECT COD_MUNICIPIO, MUNICIPIO FROM delta.`/tmp/case-rfb/municipio`) m ON est.COD_MUNICIPIO = to_number(m.COD_MUNICIPIO,9999999)
        WHERE UF = 'MA'
    )
    """
    spark.sql(query)
    print('Dados inseridos em RFB_ESTABELECIMENTO com sucesso!')

def insertRFB_ESTABELECIMENTO_EXTERIOR():
    query = """
    INSERT INTO RFB_ESTABELECIMENTO_EXTERIOR
    SELECT CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Estabelecimento/',est.CNPJ_BASICO),est.CNPJ_ORDEM),est.CNPJ_DV),'-'),UF) as uri,
           CONCAT(est.CNPJ_BASICO,CONCAT(est.CNPJ_ORDEM,est.CNPJ_DV)) AS cnpj,
           razao_social, REPLACE(regexp_replace(est.NOME_FANTASIA,'[^a-zA-Z ]',''),' ','_') as nome_fantasia,
           CASE est.EMAIL WHEN '' THEN NULL ELSE est.EMAIL END AS email,
           CASE est.MATRIZ_FILIAL WHEN '1' THEN 'MATRIZ' ELSE 'FILIAL' END as matriz_filial,
           'http://www.sefaz.ma.gov.br/resource/RFB/Estabelecimento/'||cnae_fiscal as uri_atividade_economica,
           CASE WHEN length(DATA_INICIO_ATIV) < 8 THEN NULL WHEN DATA_INICIO_ATIV like '00000000' THEN NULL ELSE est.DATA_INICIO_ATIV END AS data_inicio_ativ,
           'http://www.sefaz.ma.gov.br/resource/RFB/Endereco/'||REPLACE(regexp_replace(est.NM_CIDADE_EXTERIOR,'[^a-zA-Z ]',''),' ','_')||est.COD_PAIS ||p.NOME_PAIS||est.CEP AS uri_endereco,
           CASE WHEN est.TELEFONE_1 IS NULL THEN NULL 
                WHEN est.TELEFONE_1='' THEN NULL ELSE '('||est.DDD_1||') '||est.TELEFONE_1 END as phone_1,
           CASE WHEN est.TELEFONE_2 IS NULL THEN NULL 
                WHEN est.TELEFONE_2 ='' THEN NULL ELSE '('||est.DDD_2||') '||est.TELEFONE_2 END as phone_2,
           CASE WHEN est.NUM_FAX IS NULL THEN NULL 
                WHEN est.NUM_FAX='' THEN NULL ELSE '('||est.DDD_FAX||') '||est.NUM_FAX END as fax,
           CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Situacao_Cadastral/',situacao),'-'),est.CNPJ_BASICO),est.CNPJ_ORDEM),est.CNPJ_DV),'-'),data_situacao) AS uri_situacao_cadastral,
           cnae_fiscal as id_atividade_economica,
           REPLACE(regexp_replace(est.NM_CIDADE_EXTERIOR,'[^a-zA-Z ]',''),' ','_')||est.COD_PAIS||p.NOME_PAIS||est.CEP AS ID_ENDERECO,
           CASE est.SITUACAO WHEN '01' THEN 'NULA' WHEN '1' THEN 'NULA' WHEN '02' THEN 'ATIVA' WHEN '2' THEN 'ATIVA'
                WHEN '03' THEN 'SUSPENSA' WHEN '3' THEN 'SUSPENSA' WHEN '04' THEN 'INAPTA' WHEN '4' THEN 'INAPTA'
                WHEN '08' THEN 'BAIXADA' WHEN '8' THEN 'BAIXADA' END AS situacao
    FROM delta.`/tmp/case-rfb/estabelecimento` est
    INNER JOIN delta.`/tmp/case-rfb/empresa` e ON e.CNPJ_BASICO = est.CNPJ_BASICO
    INNER JOIN delta.`/tmp/case-rfb/pais` p ON est.COD_PAIS = p.COD_PAIS
    LEFT OUTER JOIN delta.`/tmp/case-rfb/municipio` m ON est.COD_MUNICIPIO = TO_NUMBER(m.COD_MUNICIPIO,99999999)
    WHERE TO_NUMBER(p.COD_PAIS,99999999) != 105 AND TO_NUMBER(p.COD_PAIS,99999999) != 106
    """
    spark.sql(query)
    print('Dados inseridos em RFB_ESTABELECIMENTO_EXTERIOR com sucesso!')

def insertRFB_ESTRANGEIRO():
    query = """
    INSERT INTO RFB_ESTRANGEIRO
    SELECT 'http://www.sefaz.ma.gov.br/resource/RFB/Agente/'||uri_nome as uri_socio,
           MAX(nome_socio) as nome_socio,
           max(nome_pais) as nome_pais,
           uri_nome id_socio
    FROM (
        SELECT DISTINCT CONCAT(CONCAT('_','-'),REPLACE(regexp_replace(nome_socio,'[^a-zA-Z ]',''),' ','_')) AS uri_nome,
               nome_socio,
               REPLACE(regexp_replace(p.NOME_PAIS,'[^a-zA-Z ]',''),' ','_') AS nome_pais
        FROM delta.`/tmp/case-rfb/socio` s
        LEFT OUTER JOIN delta.`/tmp/case-rfb/pais` p ON p.COD_PAIS = s.COD_PAIS_EXT
        WHERE S.TIPO_SOCIO = '3'
    )
    GROUP BY uri_nome
    """
    spark.sql(query)
    print('Dados inseridos em RFB_ESTRANGEIRO com sucesso!')

def insertRFB_FAIXA_ETARIA():
    query = """
    INSERT INTO RFB_FAIXA_ETARIA (uri_faixa_etaria, codigo, descricao) 
    VALUES (1, 1, 'Entre 0 a 12 anos'),
           (2, 2, 'Entre 13 a 20 anos'),
           (3, 3, 'Entre 21 a 30 anos'),
           (4, 4, 'Entre 31 a 40 anos'),
           (5, 5, 'Entre 41 a 50 anos'),
           (6, 6, 'Entre 51 a 60 anos'),
           (7, 7, 'Entre 61 a 70 anos'),
           (8, 8, 'Entre 71 a 80 anos'),
           (9, 9, 'Maiores de 80 anos'),
           (0, 0, 'Nao se aplica')
    """
    spark.sql(query)
    print('Dados inseridos em RFB_FAIXA_ETARIA com sucesso!')

def insertRFB_NATUREZA_LEGAL():
    query = """
    INSERT INTO RFB_NATUREZA_LEGAL
    SELECT CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Natureza_Legal/',codigo) AS id_uri,
           codigo,
           natureza_juridica,
           id_uri as ID_NATUREZA_LEGAL
    FROM (
        SELECT DISTINCT codigo,
               translate(replace(UPPER(natureza_juridica),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
                'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri,
               natureza_juridica
        FROM delta.`/tmp/case-rfb/nat_ju`
    )
    """
    spark.sql(query)
    print('Dados inseridos em RFB_NATUREZA_LEGAL com sucesso!')

def insertRFB_OPCAO_MEI():
    query = """
    INSERT INTO RFB_OPCAO_MEI
    SELECT CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Mei/',CONCAT(CONCAT(OPC_MEI ,'-'),root_cnpj)) AS uri,
           tipo_mei,
           data_exc_mei,
           data_opc_mei,
           CONCAT(CONCAT(OPC_MEI ,'-'),root_cnpj) as id_opcao_mei
    FROM (
        SELECT DISTINCT s.CNPJ_BASICO AS root_cnpj,
               CASE OPC_MEI WHEN '' THEN 'OUTROS' WHEN 'S' THEN 'SIM' WHEN 'N' THEN 'NAO' ELSE 'OUTROS' END AS OPC_MEI,
               CASE DATA_OPC_MEI WHEN '' THEN NULL WHEN '00000000' THEN NULL ELSE DATA_OPC_MEI END AS data_OPC_MEI,
               CASE DATA_EXC_MEI WHEN '' THEN NULL WHEN '00000000' THEN NULL ELSE DATA_EXC_MEI END AS data_EXC_MEI,
               CASE OPC_MEI WHEN '' THEN 'OUTROS' WHEN 'S' THEN 'SIM' WHEN 'N' THEN 'NAO' ELSE 'OUTROS' END AS tipo_mei
        FROM delta.`/tmp/case-rfb/simei` s
        INNER JOIN delta.`/tmp/case-rfb/estabelecimento` e ON s.CNPJ_BASICO = e.CNPJ_BASICO AND UF = 'MA'
    )
    """
    spark.sql(query)
    print('Dados inseridos em RFB_OPCAO_MEI com sucesso!')

def insertRFB_OPCAO_SIMPLES():
    query = """
    INSERT INTO RFB_OPCAO_SIMPLES
    SELECT CONCAT(CONCAT(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Simples/',opc_simples),'-'),root_cnpj) as uri,
           simples_type,
           class_simples,
           data_exc_simples,
           data_opc_simples,
           CONCAT(CONCAT(opc_simples,'-'),root_cnpj) as id_opcao_simples
    FROM (
        SELECT DISTINCT s.CNPJ_BASICO AS root_cnpj,
               CASE OPC_SIMPLES WHEN '' THEN 'OUTROS' WHEN 'S' THEN 'SIM' WHEN 'N' THEN 'NAO' ELSE 'OUTROS' END AS opc_simples,
               CASE DATA_OPC_SIMPLES WHEN '' THEN NULL WHEN '00000000' THEN NULL ELSE DATA_OPC_SIMPLES END AS data_opc_simples,
               CASE DATA_EXC_SIMPLES WHEN '' THEN NULL WHEN '00000000' THEN NULL ELSE DATA_EXC_SIMPLES END AS data_exc_simples,
               CASE OPC_SIMPLES WHEN '' THEN 'OTHERS' WHEN 'S' THEN 'YES' WHEN 'N' THEN 'NO' ELSE 'OUTROS' END AS class_simples,
               CASE OPC_SIMPLES WHEN '' THEN 'OUTROS' WHEN 'S' THEN 'SIM' WHEN 'N' THEN 'NAO' ELSE 'OUTROS' END AS simples_type
        FROM delta.`/tmp/case-rfb/simei` s
        INNER JOIN delta.`/tmp/case-rfb/estabelecimento` e ON s.CNPJ_BASICO = e.CNPJ_BASICO AND UF = 'MA'
    )
    """
    spark.sql(query)
    print('Dados inseridos em RFB_OPCAO_SIMPLES com sucesso!')

def insertRFB_PESSOA():
    query = """
    INSERT INTO RFB_PESSOA
    SELECT DISTINCT CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Pessoa/',CONCAT(CONCAT(max(uri_cpf),'-'),max(uri_nome))) as uri,
           cnpj_cpf_socio,
           nome_socio,
           max(faixa_etaria),
           CONCAT(CONCAT(max(uri_cpf),'-'),max(uri_nome)) as id_pessoa
    FROM (
        SELECT regexp_replace(CNPJ_CPF_SOCIO,'[^0-9]','_') AS uri_cpf,
               CNPJ_CPF_SOCIO,
               REPLACE(regexp_replace(NOME_SOCIO,'[^a-zA-Z ]',''),' ','_') AS uri_nome,
               NOME_SOCIO,
               max(FAIXA_ETARIA) AS faixa_etaria
        FROM delta.`/tmp/case-rfb/socio` s
        LEFT JOIN delta.`/tmp/case-rfb/estabelecimento` e ON s.CNPJ_BASICO = e.CNPJ_BASICO AND UF = 'MA'
        WHERE tipo_socio = '2' and nome_socio is not null
        GROUP BY cnpj_cpf_socio, nome_socio 
        UNION 
        SELECT regexp_replace(CPF_REPRES,'[^0-9]','_') AS uri_cpf,
               CPF_REPRES AS cnpj_cpf_socio,
               REPLACE(regexp_replace(NOME_REPRES,'[^a-zA-Z ]',''),' ','_') AS uri_nome,
               CASE NOME_REPRES WHEN '' THEN NULL ELSE NOME_REPRES END AS nome_socio,
               case when nome_repres like max(nome_socio) then max(faixa_etaria) else '0' end AS faixa_etaria
        FROM delta.`/tmp/case-rfb/socio`
        WHERE nome_repres is not null
        GROUP BY cpf_repres, nome_repres 
    )
    GROUP BY cnpj_cpf_socio, nome_socio
    """
    spark.sql(query)
    print('Dados inseridos em RFB_PESSOA com sucesso!')

def insertRFB_QUALIFICACAO():
    query = """
    INSERT INTO RFB_QUALIFICACAO
    SELECT CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Qualificacao/',id_uri) AS id_uri,
           cast(COD as int),
           DESCRICAO,
           id_uri as id_qualificacao
    FROM (
        SELECT DISTINCT id_uri, COD, DESCRICAO
        FROM (
            SELECT COD, DESCRICAO,
                   translate(replace(UPPER(TRIM(DESCRICAO)),' ','_'),
                   '¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
                   'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') as id_uri
            FROM delta.`/tmp/case-rfb/qualif_socio`
        )
    )
    """
    spark.sql(query)
    print('Dados inseridos em RFB_QUALIFICACAO com sucesso!')

def insertRFB_RAZAO_SITUACAO_CADASTRAL():
    query = """
    INSERT INTO RFB_RAZAO_SITUACAO_CADASTRAL
    SELECT CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Razao_Situacao_Cadastral/',id_uri) as id_uri,
           Descricao,
           Codigo,
           id_uri as id_razao_situacao_cadastral
    FROM (
        SELECT DISTINCT id_uri, Descricao, Codigo
        FROM (
            SELECT Codigo, Descricao,
                   translate(replace(UPPER(trim(descricao)),' ','_'),
                   '¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
                   'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') as id_uri
            FROM delta.`/tmp/case-rfb/razao_situacao`
        )
    )
    """
    spark.sql(query)
    print('Dados inseridos em RFB_RAZAO_SITUACAO_CADASTRAL com sucesso!')

def insertRFB_SITUACAO_CADASTRAL():
    query = """
    INSERT INTO RFB_SITUACAO_CADASTRAL
    SELECT CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Situacao_Cadastral/',situacao),'-'),cnpj),'-'),data_situacao) AS uri,
           val_data_situacao,
           situacao,
           (situacao||'-'||cnpj||'-'||data_situacao) as id_situacao_cadastral,
           id_uri
    FROM (
        SELECT DISTINCT CONCAT(CNPJ_BASICO,CONCAT(CNPJ_ORDEM,CNPJ_DV)) as cnpj,
               REPLACE(regexp_replace(nome_fantasia, '[^a-zA-Z ]', ''),' ','_') as nome_uri,
               CASE WHEN length(data_situacao) < 8 THEN NULL WHEN data_situacao = '00000000' THEN NULL ELSE data_situacao END as data_situacao,
               CASE WHEN length(data_situacao) < 8 THEN NULL WHEN data_situacao = '00000000' THEN NULL ELSE data_situacao END as val_data_situacao,
               CASE situacao WHEN '1' THEN 'NULA' when '01' then 'NULA'
                    WHEN '2' THEN 'ATIVA' WHEN '02' THEN 'ATIVA'
                    WHEN '3' THEN 'SUSPENSA' WHEN '03' THEN 'SUSPENSA'
                    WHEN '4' THEN 'INAPTA' WHEN '04' THEN 'INAPTA'
                    WHEN '8' THEN 'BAIXADA' WHEN '08' THEN 'BAIXADA'
                    ELSE 'NULL' END as situacao,
               id_uri
        FROM delta.`/tmp/case-rfb/estabelecimento`
        LEFT OUTER JOIN (select codigo, descricao, translate(replace(UPPER(trim(descricao)),' ','_'),
            '¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
            'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') as id_uri from delta.`/tmp/case-rfb/razao_situacao`) motiv_situacao
        ON delta.`/tmp/case-rfb/estabelecimento`.motivo_situacao = motiv_situacao.codigo
        WHERE UF = 'MA'
    )
    """
    spark.sql(query)
    print('Dados inseridos em RFB_SITUACAO_CADASTRAL com sucesso!')

def insertRFB_SITUACAO_ESPECIAL():
    query = """
    INSERT INTO RFB_SITUACAO_ESPECIAL
    SELECT CONCAT(CONCAT(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Situacao_Especial/',root_cnpj),'-'),data_sit_especial) AS uri,
           data_sit,
           sit_especial,
           (root_cnpj||'-'||data_sit_especial) as id_situacao_especial
    FROM (
        SELECT DISTINCT CNPJ_BASICO AS root_cnpj,
               CASE DATA_SIT_ESPECIAL WHEN '00000000' THEN NULL ELSE DATA_SIT_ESPECIAL END AS data_sit_especial,
               CASE DATA_SIT_ESPECIAL WHEN '00000000' THEN NULL ELSE DATA_SIT_ESPECIAL END AS data_sit,
               SIT_ESPECIAL
        FROM delta.`/tmp/case-rfb/estabelecimento`
        WHERE MATRIZ_FILIAL = '1' AND SIT_ESPECIAL is not null AND UF = 'MA'
    )
    """
    spark.sql(query)
    print('Dados inseridos em RFB_SITUACAO_ESPECIAL com sucesso!')

def insertRFB_SOCIEDADE_COM_ESTRANGEIRO():
    query = """
    INSERT INTO RFB_SOCIEDADE_COM_ESTRANGEIRO
    SELECT MAX(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Sociedade/',id_uri),'-'),root_cnpj),'-'),uri_socio)) AS uri,
           MAX(data_entrada) as data_inicio_sociedade,
           MAX(descricao) as rotulo,
           MAX(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Qualificacao/',rep_quali)) as uri_qualificacao_representante,
           MAX(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Pessoa/',(uri_socio))) as uri_socio,
           MAX(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Pessoa/',(CONCAT(CONCAT(cpf_rep,'-'),nome_rep)))) as uri_representante,
           MAX(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Qualificacao/',(id_uri))) as uri_qualificacao_socio,
           MAX(rep_quali) AS id_qualificacao_representante,
           MAX(uri_socio) AS id_socio,
           MAX(id_uri) AS id_uri,
           MAX(root_cnpj) AS cnpj_raiz,
           MAX(CONCAT(CONCAT(cpf_rep,'-'),nome_rep)) AS id_representante,
           id_sociedade
    FROM (
        SELECT DISTINCT s.DATA_ENTRADA as data_entrada,
               CONCAT(CONCAT(q.DESCRICAO,': '),s.NOME_SOCIO) AS descricao,
               r.id_uri AS rep_quali,
               REPLACE(regexp_replace(s.NOME_REPRES,'[^a-zA-Z ]',''),' ','_') AS nome_rep,
               regexp_replace(s.CPF_REPRES,'[^0-9]','_') AS cpf_rep,
               s.CNPJ_BASICO AS root_cnpj,
               q.id_uri AS id_uri,
               CONCAT(CONCAT('_','-'),REPLACE(regexp_replace(s.NOME_SOCIO,'[^a-zA-Z ]',''),' ','_')) AS uri_socio,
               q.id_uri||'-'||root_cnpj||'-'||CONCAT(CONCAT('_','-'),REPLACE(regexp_replace(s.NOME_SOCIO,'[^a-zA-Z ]',''),' ','_')) as id_sociedade
        FROM delta.`/tmp/case-rfb/socio` s
        INNER JOIN (SELECT cod, descricao, translate(replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
                    FROM delta.`/tmp/case-rfb/qualif_socio`) q ON s.cod_qualificacao = q.COD
        LEFT OUTER JOIN (SELECT cod, descricao, translate(replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
                         FROM delta.`/tmp/case-rfb/qualif_socio`) r ON s.cod_qualif_repres = r.COD
        LEFT OUTER JOIN delta.`/tmp/case-rfb/estabelecimento` e ON s.CNPJ_BASICO = e.CNPJ_BASICO AND UF = 'MA'
        WHERE tipo_socio = '3'
    )
    GROUP BY id_sociedade
    """
    spark.sql(query)
    print('Dados inseridos em RFB_SOCIEDADE_COM_ESTRANGEIRO com sucesso!')

def insertRFB_SOCIEDADE_COM_HOLDING():
    query = """
    INSERT INTO RFB_SOCIEDADE_COM_HOLDING
    SELECT MAX(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Sociedade/',id_uri),'-'),root_cnpj),'-'),cnpj_cpf_socio)) AS uri,
           MAX(descricao) as descricao,
           MAX(data_entrada) as data_inicio_sociedade,
           MAX(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Qualificacao/',rep_quali)) as uri_qualificacao_representante,
           MAX(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Empresa/',cnpj_cpf_socio)) as uri_socio,
           MAX(CONCAT ('http://www.sefaz.ma.gov.br/resource/RFB/Pessoa/',(CONCAT(CONCAT(cpf_rep,'-'),nome_rep)))) as uri_representante,
           MAX(CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Qualificacao/',(id_uri))) as uri_qualificacao_socio,
           MAX((id_uri||'-'||root_cnpj||'-'||cnpj_cpf_socio)) AS id_sociedade,
           MAX(id_uri) AS id_qualificacao_socio,
           MAX(rep_quali) AS id_qualificacao_representante,
           MAX(cnpj_cpf_socio) AS id_socio,
           MAX(CONCAT(CONCAT(cpf_rep,'-'),nome_rep)) AS id_representante
    FROM (
        SELECT DISTINCT CASE WHEN length(s.DATA_ENTRADA) < 8 THEN NULL WHEN s.DATA_ENTRADA like '00000000' THEN NULL ELSE s.DATA_ENTRADA END as data_entrada,
               CONCAT(CONCAT(q.DESCRICAO,': '),s.nome_socio) AS descricao,
               r.id_uri AS rep_quali,
               REPLACE(regexp_replace(s.nome_repres,'[^a-zA-Z ]',''),' ','_') AS nome_rep,
               regexp_replace(s.cpf_repres,'[^0-9]','_') AS cpf_rep,
               s.CNPJ_BASICO AS root_cnpj,
               q.id_uri AS id_uri,
               s.cnpj_cpf_socio AS cnpj_cpf_socio,
               REPLACE(regexp_replace(s.nome_socio,'[^a-zA-Z ]',''),' ','_') AS nome_socio
        FROM delta.`/tmp/case-rfb/socio` s
        INNER JOIN (SELECT cod, descricao, translate(replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
                    FROM delta.`/tmp/case-rfb/qualif_socio`) q ON s.cod_qualificacao = q.COD
        LEFT OUTER JOIN (SELECT cod, descricao, translate(replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
                         FROM delta.`/tmp/case-rfb/qualif_socio`) r ON s.cod_qualif_repres = r.COD
        INNER JOIN delta.`/tmp/case-rfb/estabelecimento` e ON s.CNPJ_BASICO = e.CNPJ_BASICO AND UF = 'MA'
    )
    GROUP BY (id_uri||'-'||root_cnpj||'-'||cnpj_cpf_socio)
    """
    spark.sql(query)
    print('Dados inseridos em RFB_SOCIEDADE_COM_HOLDING com sucesso!')

def insertRFB_SOCIEDADE_COM_PESSOA_FISICA():
    query = """
    INSERT INTO RFB_SOCIEDADE_COM_PESSOA_FISICA
    SELECT 'http://www.sefaz.ma.gov.br/resource/RFB/Sociedade/'||(id_uri||'-'||root_cnpj||'-'||cnpj_cpf_socio||'-'||nome_socio) AS uri,
           descricao as ROTULO,
           data_entrada as DATA_INICIO_SOCIEDADE,
           CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Pessoa/',(CONCAT(CONCAT(cnpj_cpf_socio,'-'),nome_socio))) AS uri_socio,
           CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Qualificacao/',id_uri) AS uri_qualificacao_socio,
           CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Pessoa/',CONCAT(CONCAT(cnpj_cpf_representante,'-'),nome_representante)) AS uri_representante,
           CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Qualificacao/',id_uri_representante) AS uri_qualificacao_representante,
           (id_uri||'-'||root_cnpj||'-'||cnpj_cpf_socio||'-'||nome_socio) AS id_sociedade,
           id_uri AS id_qualificacao_socio,
           id_uri_representante AS id_qualificacao_representante,
           CONCAT(CONCAT(cnpj_cpf_representante,'-'),nome_representante) AS id_representante,
           (cnpj_cpf_socio||'-'||nome_socio) AS id_socio
    FROM (
        SELECT CASE WHEN length(s.DATA_ENTRADA) < 8 THEN NULL WHEN s.DATA_ENTRADA like '00000000' THEN NULL ELSE s.DATA_ENTRADA END as data_entrada,
               s.CNPJ_BASICO AS root_cnpj,
               CONCAT(CONCAT(q.DESCRICAO,': '),s.nome_socio) AS descricao,
               q.id_uri AS id_uri,
               regexp_replace(s.cnpj_cpf_socio,'[^0-9]','_') AS cnpj_cpf_socio,
               REPLACE(regexp_replace(s.nome_socio,'[^a-zA-Z ]',''),' ','_') AS nome_socio,
               regexp_replace(s.CPF_REPRES,'[^0-9]','_') AS cnpj_cpf_representante,
               REPLACE(regexp_replace(s.NOME_REPRES,'[^a-zA-Z ]',''),' ','_') AS nome_representante,
               q.id_uri AS id_uri_representante
        FROM delta.`/tmp/case-rfb/socio` s
        INNER JOIN (SELECT cod, descricao, translate(replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
                    FROM delta.`/tmp/case-rfb/qualif_socio`) q ON s.cod_qualificacao = q.COD
        LEFT OUTER JOIN (SELECT cod, descricao, translate(replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
                    'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
                         FROM delta.`/tmp/case-rfb/qualif_socio`) r ON s.cod_qualif_repres = r.COD
        INNER JOIN delta.`/tmp/case-rfb/estabelecimento` e ON s.CNPJ_BASICO = e.CNPJ_BASICO AND UF = 'MA'
        WHERE nome_socio IS NOT NULL
    )
    """
    spark.sql(query)
    print('Dados inseridos em RFB_SOCIEDADE_COM_PESSOA_FISICA com sucesso!')

def insertRFB_TEM_ATIV_ECONOMICA_SECUNDARIA():
    query = """
    INSERT INTO RFB_TEM_ATIV_ECONOMICA_SECUNDARIA
    SELECT 'http://www.sefaz.ma.gov.br/resource/RFB/Estabelecimento/'||concat(CNPJ_BASICO,concat(CNPJ_ORDEM, CNPJ_DV)),
           'http://www.sefaz.ma.gov.br/resource/RFB/Atividade_Economica/'||SUBSTR(cnae,0,7),
           concat(CNPJ_BASICO,concat(CNPJ_ORDEM, CNPJ_DV)),
           SUBSTR(cnae,0,7)
    FROM (
        SELECT cnpj_basico, cnpj_ordem, cnpj_dv, uf, cnae_fiscal_sec FROM delta.`/tmp/case-rfb/estabelecimento` WHERE UF = 'MA'
    ) e
    WHERE cnae_fiscal_sec IS NOT NULL
    """
    spark.sql(query)
    print('Dados inseridos em RFB_TEM_ATIV_ECONOMICA_SECUNDARIA com sucesso!')

def insertRFB_TEM_ESTABELECIMENTO():
    query = """
    INSERT INTO RFB_TEM_ESTABELECIMENTO
    SELECT 'http://www.sefaz.ma.gov.br/resource/RFB/Empresa/'||root_cnpj_e as uri_empresa,
           'http://www.sefaz.ma.gov.br/resource/RFB/Estabelecimento/'||cnpj_f as uri_estabelecimento,
           root_cnpj_e as id_empresa,
           cnpj_f as id_estabelecimento
    FROM (
        SELECT DISTINCT e.CNPJ_RAIZ AS root_cnpj_e, f.cnpj AS cnpj_f
        FROM RFB_EMPRESA e
        INNER JOIN RFB_ESTABELECIMENTO f ON e.CNPJ_RAIZ = SUBSTR(f.CNPJ, 0, 8)
    )
    """
    spark.sql(query)
    print('Dados inseridos em RFB_TEM_ESTABELECIMENTO com sucesso!')

def insertRFB_TEM_SITUACAO_ESPECIAL():
    query = """
    INSERT INTO RFB_TEM_SITUACAO_ESPECIAL
    SELECT CONCAT ('http://www.sefaz.ma.gov.br/resource/RFB/Empresa/',root_cnpj) as uri_empresa,
           CONCAT ('http://www.sefaz.ma.gov.br/resource/RFB/Situacao_especial/',CONCAT(CONCAT(root_cnpj,'-'),data_sit_especial)) as uri_situacao_especial,
           root_cnpj AS id_empresa,
           CONCAT(CONCAT(root_cnpj,'-'),data_sit_especial) AS id_situacao_especial
    FROM (
        SELECT e.CNPJ_RAIZ AS root_cnpj,
               CASE est.data_sit_especial WHEN '00000000' THEN null ELSE est.data_sit_especial END AS data_sit_especial
        FROM delta.`/tmp/case-rfb/estabelecimento` est
        INNER JOIN RFB_EMPRESA e ON est.cnpj_basico = e.CNPJ_RAIZ
        WHERE est.matriz_filial = '1' AND est.sit_especial is not null
    )
    """
    spark.sql(query)
    print('Dados inseridos em RFB_TEM_SITUACAO_ESPECIAL com sucesso!')

def insertRFB_TEM_SOCIEDADE_ESTRANGEIRA():
    query = """
    INSERT INTO RFB_TEM_SOCIEDADE_ESTRANGEIRA
    SELECT CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Empresa/',root_cnpj) as uri_empresa,
           CONCAT('http://www.sefaz.ma.gov.br/resource/RFB/Sociedade/',(id_uri||'-'||root_cnpj||'-'||uri_socio)) as uri_sociedade,
           root_cnpj AS id_empresa,
           (id_uri||'-'||root_cnpj||'-'||uri_socio) AS id_socio
    FROM (
        SELECT DISTINCT s.CNPJ_BASICO AS root_cnpj,
               REPLACE(regexp_replace(e.RAZAO_SOCIAL,'[^a-zA-Z ]',''),' ','_') AS razao_uri,
               q.id_uri AS id_uri,
               CONCAT(CONCAT('_','-'),REPLACE(regexp_replace(s.NOME_SOCIO,'[^a-zA-Z ]',''),' ','_')) AS uri_socio
        FROM delta.`/tmp/case-rfb/socio` s
        INNER JOIN RFB_EMPRESA e ON e.CNPJ_RAIZ = s.CNPJ_BASICO
        INNER JOIN (SELECT cod, descricao, translate(replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
                  'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
              FROM delta.`/tmp/case-rfb/qualif_socio`) q ON s.COD_QUALIFICACAO = q.COD
        WHERE tipo_socio = '3'
    )
    """
    spark.sql(query)
    print('Dados inseridos em RFB_TEM_SOCIEDADE_ESTRANGEIRA com sucesso!')

def insertRFB_TEM_SOCIEDADE_FISICA():
    query = """
    INSERT INTO RFB_TEM_SOCIEDADE_FISICA
    SELECT 'http://www.sefaz.ma.gov.br/resource/RFB/Empresa/'||root_cnpj as uri_empresa,
           'http://www.sefaz.ma.gov.br/resource/RFB/Sociedade/'||(id_uri||'-'||root_cnpj||'-'||cnpj_cpf_socio||'-'||nome_socio) as uri_sociedade,
           root_cnpj AS id_empresa,
           (id_uri||'-'||root_cnpj||'-'||cnpj_cpf_socio||'-'||nome_socio) AS id_socio
    FROM (
        SELECT s.CNPJ_BASICO AS root_cnpj,
               REPLACE(regexp_replace(razao_social,'[^a-zA-Z ]',''),' ','_') AS razao_uri,
               q.id_uri AS id_uri,
               regexp_replace(s.CNPJ_CPF_SOCIO,'[^0-9]','_') AS cnpj_cpf_socio,
               REPLACE(regexp_replace(s.NOME_SOCIO,'[^a-zA-Z ]',''),' ','_') AS nome_socio
        FROM delta.`/tmp/case-rfb/socio` s
        INNER JOIN RFB_EMPRESA e ON e.CNPJ_RAIZ = s.CNPJ_BASICO AND tipo_socio = '2'
        LEFT JOIN (SELECT cod, descricao, translate(replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
                  'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
              FROM delta.`/tmp/case-rfb/qualif_socio`) q ON s.COD_QUALIFICACAO = q.COD
    )
    """
    spark.sql(query)
    print('Dados inseridos em RFB_TEM_SOCIEDADE_FISICA com sucesso!')

def insertRFB_TEM_SOCIEDADE_JURIDICA():
    query = """
    INSERT INTO RFB_TEM_SOCIEDADE_JURIDICA
    SELECT 'http://www.sefaz.ma.gov.br/resource/RFB/Empresa/'||root_cnpj as uri_empresa,
           'http://www.sefaz.ma.gov.br/resource/RFB/Sociedade/'||(id_uri||'-'||root_cnpj||'-'||cnpj_cpf_socio) as uri_sociedade,
           root_cnpj AS id_empresa,
           (id_uri||'-'||root_cnpj||'-'||cnpj_cpf_socio) AS id_socio
    FROM (
        SELECT DISTINCT s.CNPJ_BASICO AS root_cnpj,
               REPLACE(regexp_replace(e.RAZAO_SOCIAL,'[^a-zA-Z ]',''),' ','_') AS razao_uri,
               q.id_uri AS id_uri,
               SUBSTR(s.CNPJ_CPF_SOCIO,0,8) AS cnpj_cpf_socio,
               REPLACE(regexp_replace(s.NOME_SOCIO,'[^a-zA-Z ]',''),' ','_') AS nome_socio
        FROM delta.`/tmp/case-rfb/socio` s
        INNER JOIN RFB_EMPRESA e ON e.CNPJ_RAIZ = s.CNPJ_BASICO
        INNER JOIN (SELECT cod, descricao, translate(replace(UPPER(TRIM(descricao)),' ','_'),'¿¿¿¿YÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
                  'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy') AS id_uri
              FROM delta.`/tmp/case-rfb/qualif_socio`) q ON s.COD_QUALIFICACAO = q.COD
        WHERE tipo_socio = '1'
    )
    """
    spark.sql(query)
    print('Dados inseridos em RFB_TEM_SOCIEDADE_JURIDICA com sucesso!')

def table_exists(table_name):
    try:
        spark.sql(f"SELECT 1 FROM {table_name} LIMIT 1")
        return True
    except:
        return False

def cleanAllTables():
    tables = [
        "RFB_EMPRESA", "RFB_EMPRESA_HOLDING", "RFB_ENDERECO", "RFB_ENDERECO_EXTERIOR",
        "RFB_ESTABELECIMENTO", "RFB_ESTABELECIMENTO_EXTERIOR", "RFB_ESTRANGEIRO",
        "RFB_FAIXA_ETARIA", "RFB_NATUREZA_LEGAL", "RFB_OPCAO_MEI", "RFB_OPCAO_SIMPLES",
        "RFB_PESSOA", "RFB_QUALIFICACAO", "RFB_RAZAO_SITUACAO_CADASTRAL",
        "RFB_SITUACAO_CADASTRAL", "RFB_SITUACAO_ESPECIAL", "RFB_SOCIEDADE_COM_ESTRANGEIRO",
        "RFB_SOCIEDADE_COM_HOLDING", "RFB_SOCIEDADE_COM_PESSOA_FISICA",
        "RFB_TEM_ESTABELECIMENTO",
        "RFB_TEM_SITUACAO_ESPECIAL", "RFB_TEM_SOCIEDADE_ESTRANGEIRA",
        "RFB_TEM_SOCIEDADE_FISICA", "RFB_TEM_SOCIEDADE_JURIDICA"
    ]
    
    for table in tables:
        spark.sql(f"TRUNCATE TABLE {table}")
        print(f'Tabela {table} truncada com sucesso!')

def insertAllTables():
    insertRFB_EMPRESA()
    insertRFB_EMPRESA_HOLDING()
    insertRFB_ENDERECO()
    insertRFB_ENDERECO_EXTERIOR()
    insertRFB_ESTABELECIMENTO()
    insertRFB_ESTABELECIMENTO_EXTERIOR()
    insertRFB_ESTRANGEIRO()
    insertRFB_FAIXA_ETARIA()
    insertRFB_NATUREZA_LEGAL()
    insertRFB_OPCAO_MEI()
    insertRFB_OPCAO_SIMPLES()
    insertRFB_PESSOA()
    insertRFB_QUALIFICACAO()
    insertRFB_RAZAO_SITUACAO_CADASTRAL()
    insertRFB_SITUACAO_CADASTRAL()
    insertRFB_SITUACAO_ESPECIAL()
    insertRFB_SOCIEDADE_COM_ESTRANGEIRO()
    insertRFB_SOCIEDADE_COM_HOLDING()
    insertRFB_SOCIEDADE_COM_PESSOA_FISICA()
    insertRFB_TEM_ATIV_ECONOMICA_SECUNDARIA()
    insertRFB_TEM_ESTABELECIMENTO()
    insertRFB_TEM_SITUACAO_ESPECIAL()
    insertRFB_TEM_SOCIEDADE_ESTRANGEIRA()
    insertRFB_TEM_SOCIEDADE_FISICA()
    insertRFB_TEM_SOCIEDADE_JURIDICA()

def createAllTables():
    createEmpresaHolding()
    createEmpresa()
    createEndereco()
    createEnderecoExterior()
    createEstabelecimento()
    createEstabelecimentoExterior()
    createEstrangeiro()
    createFaixaEtaria()
    createNaturezaLegal()
    createOpcaoMei()
    createOpcaoSimples()
    createPessoa()
    createQualificacaoSocio()
    createRazaoSituacaoCadastral()
    createSituacaoCadastral()
    createSituacaoEspecial()
    createSociedadeEstrangeiro()
    createSociedadeHolding()
    createSociedadePessoaFisica()
    createTemAtividadeEconomicaSecundaria()
    createTemEstabelecimento()
    createTemSituacaoEspecial()
    createTemSociedadeEstrangeiro()
    createTemSociedadeJuridica()
    createTemSociedadeFisica()

def startSilver():
    if table_exists("RFB_EMPRESA"):
        cleanAllTables()
        insertAllTables()
    else:
        createAllTables()

startSilver()

def store_table_info_as_json(table_names, json_path):
    table_infos = []

    for table_name in table_names:
        table_path = f"/tmp/case-rfb/{table_name}"
        df = spark.read.format("delta").load(table_path)

        # Obtenha o tamanho da tabela em bytes
        table_size = sum(f.size for f in spark._jsparkSession.sessionState().catalog().getTableMetadata(spark._jsparkSession.sessionState().catalog().resolveTableIdentifier(table_name)).location().getStorage().listFilesRecursive())
        
        # Obtenha a quantidade de registros
        record_count = df.count()

        table_info = {
            "table_name": table_name,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "size_in_bytes": table_size,
            "record_count": record_count
        }

        table_infos.append(table_info)
    
    with open("silver_tables_info.json", "w") as json_file:
        json.dump(table_infos, json_file, indent=4)

    print(f"Informa��es das tabelas armazenadas em {json_path} com sucesso!")

# Caminho do arquivo JSON
json_path = "silver_tables_info.json"

# Chame o m�todo para armazenar informa��es das tabelas
store_table_info_as_json(table_names, json_path)

#createRazaoSituacaoCadastral()
#createQualificacaoSocio()
#createEndereco()
#createEnderecoExterior()
#createEstabelecimento()
#createEstabelecimentoExterior() 
#createEstrangeiro()
#spark.sql("DROP TABLE RFB_FAIXA_ETARIA")
#createFaixaEtaria()
#createNaturezaLegal()
#createOpcaoMei()
#createOpcaoSimples()
#createPessoa()
#createQualificacaoSocio()
#createRazaoSituacaoCadastral()
#createSituacaoCadastral()
#createSituacaoEspecial()
#createSociedadeEstrangeiro()
#createSociedadeHolding()
#createSociedadePessoaFisica()
#createTemAtividadeEconomicaSecundaria()
#createTemEstabelecimento()
#createTemSituacaoEspecial()
#createTemSociedadeEstrangeiro()
#createTemSociedadeJuridica()
#createTemSociedadeFisica()

# Encerrar a sess�o Spark
spark.stop()