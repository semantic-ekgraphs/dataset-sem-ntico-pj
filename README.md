# Dataset Semântico de Pessoas Jurídicas do Cadastro Nacional (DSPJ) - RFB
DSPJ é uma iniciativa para para construção de um dataset semântico (DS) de Pessoas Jurídicas baseado em uma arquitetura de Data Lakehouses.

Para construçao do dataset, a metodologia proposta é organizada da seguinte forma conforme (Figura 2): i) Aquisição dos dados; ii) Geração das Tabelas Bronze; iii) Geração das Tabelas Silver; iv) Geração das Tabelas Gold; v) Geração do Dataset.

![metodologia__](https://github.com/user-attachments/assets/27ed331a-52c9-4393-baa6-7624f52e0636)

Arquitetura geral adotada para construção do dataset do Cadastro Nacional de Pessoas Jurídicas com base em um Data Lakehouse Semântico é apresentada na Figura 1. Essa arquitetura serve para nortear a metodologia para desenvolvimento do dataset deste trabalho. Na arquitetura apresentada, o Data Lakehouse é estruturado através do uso do Delta Lake. Ainda, a arquitetura utilizada como base é flexível e segue esquemas semanticamente coerentes, isto é, possibilitando que desenvolvedores, ainda não familiarizados com tecnologias semˆanticas, possam alternativamente realizar consultas em SQL ou desenvolver aplicações com os dados do CNPJ diretamente através das tabelas delta.

![plat__ (1)](https://github.com/user-attachments/assets/aadfd177-7b13-4d7c-a87f-55a1c24d6075)

# Script obtenção dos dados (Passo 0):
A primeira etapa do script consiste no download de arquivos .zip. Em seguida, o conteúdo de cada arquivo é extraído no diretório data/<concept>, onde <concept> corresponde ao nome do conceito relacionado ao arquivo, e.g Paises.zip é extraído em data/paises/. Durante o processo, os arquivos são renomeados com a extensão .csv, pois, originalmente, eles são disponibilizados com o texto ”CSV” ao final do nome, sem a extensão adequada.

[Link](scripts/step0.py).

# Script Geração das Tabelas Bronze (Passo 1):

#[Link](scripts/step1.py).

# Script Geração das Tabelas Silver (Passo 2):

#[Link](scripts/step2.py).

# Arquivo de Mapeamento

#[Link](scripts/mapeamentos.ttl).

# Script de Geração das triplas

#[Link](scripts/triplify.py).

