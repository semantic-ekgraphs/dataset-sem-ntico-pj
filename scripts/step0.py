import requests
import re
import os
import zipfile
import shutil
import logging
from datetime import datetime, timedelta
from urllib.parse import urljoin
import json

# Configuração do log
logging.basicConfig(filename='download_log.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# URL da página com os arquivos
base_url = 'https://dadosabertos.rfb.gov.br/CNPJ/'

# Diretório base para salvar os arquivos
base_dir = 'data/'
old_base_dir = 'data_old/'

# Mapeamento dos arquivos para os diretórios correspondentes
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

# Função para criar diretórios se não existirem
def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

# Criação dos diretórios base se não existirem
create_directory(base_dir)
create_directory(old_base_dir)

# Criação dos diretórios para cada tipo de arquivo
for dir_name in dir_mapping.values():
    create_directory(os.path.join(base_dir, dir_name))

def download_file(file_url, save_dir):
    local_filename = file_url.split('/')[-1]
    zip_path = os.path.join(save_dir, local_filename)
    
    try:
        with requests.get(file_url, stream=True) as r:
            r.raise_for_status()
            with open(zip_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        mensagem = f'Baixado: {local_filename}'
        print(mensagem)
        logging.info(mensagem)
        return zip_path
    except requests.exceptions.RequestException as e:
        mensagem = f'Erro ao baixar {local_filename}: {e}'
        print(mensagem)
        logging.error(mensagem)
        return None

def extract_zip(zip_path, save_dir):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for zip_info in zip_ref.infolist():
                extracted_path = os.path.join(save_dir, zip_info.filename)
                
                # Verifica se o arquivo extraído já existe e se o tamanho é o mesmo
                if os.path.exists(extracted_path) and os.path.getsize(extracted_path) == zip_info.file_size:
                    mensagem = f'Arquivo {zip_info.filename} já existe com o mesmo tamanho. Extração não necessária.'
                    print(mensagem)
                    logging.info(mensagem)
                else:
                    zip_ref.extract(zip_info, save_dir)
                    # Renomeia o arquivo para adicionar ".csv" ao final
                    if zip_info.filename.endswith('CSV'):
                        base_name = zip_info.filename[:-3]  # Remove 'CSV'
                        new_file_name = base_name + '.csv'
                        new_file_path = os.path.join(save_dir, new_file_name)
                        
                        # Verifica se o novo nome do arquivo já existe
                        if os.path.exists(new_file_path):
                            mensagem = f'Arquivo {new_file_name} já existe. Renomeação não necessária.'
                            print(mensagem)
                            logging.info(mensagem)
                        else:
                            os.rename(os.path.join(save_dir, zip_info.filename), new_file_path)

        os.remove(zip_path)
        
        mensagem = f'Extraído: {os.path.basename(zip_path)}'
        print(mensagem)
        logging.info(mensagem)
    except zipfile.BadZipFile as e:
        mensagem = f'Erro ao extrair {zip_path}: {e}'
        print(mensagem)
        logging.error(mensagem)

def archive_and_clear_directory(file_name_prefix, save_dir):
    date_str = datetime.now().strftime('%Y-%m-%d')
    old_save_dir = os.path.join(old_base_dir, file_name_prefix, date_str)
    
    create_directory(old_save_dir)
    
    # Move arquivos antigos para o diretório de arquivos antigos
    for file in os.listdir(save_dir):
        shutil.move(os.path.join(save_dir, file), old_save_dir)

def is_new_version(file_url, save_dir):
    local_filename = file_url.split('/')[-1]
    zip_path = os.path.join(save_dir, local_filename)

    # Verifica se o arquivo já existe
    if os.path.exists(zip_path):
        try:
            response = requests.head(file_url)
            response.raise_for_status()
            remote_file_size = int(response.headers.get('Content-Length', 0))
            local_file_size = os.path.getsize(zip_path)
            
            if remote_file_size != local_file_size:
                return True
            else:
                mensagem = f'Arquivo {local_filename} já existe e tem o mesmo tamanho. Download não necessário.'
                print(mensagem)
                logging.info(mensagem)
                return False
        except requests.exceptions.RequestException as e:
            mensagem = f'Erro ao verificar versão de {local_filename}: {e}'
            print(mensagem)
            logging.error(mensagem)
            return False
    else:
        return True

def get_zip_links():
    try:
        response = requests.get(base_url)
        response.raise_for_status()
        zip_links = re.findall(r'href=[\'"]?([^\'" >]+\.zip)', response.text)
        full_links = [urljoin(base_url, link) for link in zip_links]
        return full_links
    except requests.exceptions.RequestException as e:
        mensagem = f'Erro ao buscar links zip: {e}'
        print(mensagem)
        logging.error(mensagem)
        return []

def get_save_dir(file_name):
    for key, value in dir_mapping.items():
        if key in file_name.lower():
            return os.path.join(base_dir, value)
    return None

def process_zip_file(link):
    file_name = link.split('/')[-1].split('.')[0]
    save_dir = get_save_dir(file_name)

    if save_dir is None:
        mensagem = f'Arquivo {file_name} não corresponde a nenhum diretório mapeado. Download ignorado.'
        print(mensagem)
        logging.info(mensagem)
        return

    if os.path.exists(save_dir):
        archive_and_clear_directory(file_name, save_dir)
    else:
        create_directory(save_dir)

    if is_new_version(link, save_dir):
        zip_path = download_file(link, save_dir)
        if zip_path:
            extract_zip(zip_path, save_dir)

def main():
    zip_links = get_zip_links()
    file_sizes = {}

    for link in zip_links:
        file_name = link.split('/')[-1].split('.')[0]
        save_dir = get_save_dir(file_name)
        if save_dir and is_new_version(link, save_dir):
            zip_path = download_file(link, save_dir)
            if zip_path:
                # Adiciona informações do arquivo baixado ao dicionário
                file_sizes[os.path.basename(zip_path)] = os.path.getsize(zip_path)
                extract_zip(zip_path, save_dir)
                


    # Escreve as informações do download em um arquivo JSON
    download_data = {
        'data': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'arquivos': file_sizes
    }
    with open('download_data.json', 'w') as json_file:
        json.dump(download_data, json_file, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    # while True:
    #     # Verifica se a data no arquivo download_data.json é superior a 30 dias em relação à data atual
    #     if os.path.exists('download_data.json'):
    #         with open('download_data.json', 'r') as json_file:
    #             download_data = json.load(json_file)
    #             last_download_date = datetime.strptime(download_data['data'], '%Y-%m-%d %H:%M:%S')
    #             if datetime.now() - last_download_date > timedelta(days=30):
    #                 main()
    #     else:
    #         main()
    #     logging.info('Processo de download e extração concluído.')
        
    #     # Aguarda 24 horas para a próxima execução
    #     time.sleep(86400)
    
    main()
    logging.info('Processo de download e extração concluído.')

# Encerrar a sess�o Spark
spark.stop()
