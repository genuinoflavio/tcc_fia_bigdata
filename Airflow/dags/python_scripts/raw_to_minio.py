# TODO: Tirar prints e incluir LOGS com tempo de inicio e fim de tarefas
# TODO Refatorar funções para seguir principios do SOLID
# TODO Renomeiar arquivos da pasta raw/broze/I (e.g I_1.json para 1.json)

import requests
import json
import os
import re
import time
from minio import Minio
from minio.error import S3Error
import io
import sys

#MINIO CONFIGS
minio_endpoint = os.environ.get('MINIO_ENDPOINT') # Mudar para o IP quando rodar no Docker;
minio_access_key = os.environ.get('MINIO_ROOT_USER')
minio_secret_key = os.environ.get('MINIO_ROOT_PASSWORD')
minio_bucket_name  = "raw"

riot_api_token = os.environ.get('API_TOKEN') 

print(f'VARIABLES: {riot_api_token} - {minio_secret_key}')

def get_minio_client():
    minio_client = Minio(minio_endpoint,
                            access_key=minio_access_key,
                            secret_key=minio_secret_key,
                            secure=False)

    return minio_client

def check_folder_exists(minio_bucket_name, folder_path):
    
    minio_client = get_minio_client()
    try:
        # Lista todos objetos na pasata
        objects = minio_client.list_objects(minio_bucket_name, prefix=folder_path, recursive=False)
        
        folders = {obj.object_name.split("/")[0] for obj in objects if obj.is_dir}
        
        # Retorno true se encontrar as pastas
        return folder_path.rstrip("/") in folders

    except S3Error as err:
        return False  # Retorna False se não existir

def create_folders(minio_bucket_name, folder_path):
    minio_client = get_minio_client()
    try:
        minio_client.put_object(minio_bucket_name, f"{folder_path}/", io.BytesIO(b''), 0)
        print(f"O caminho no BUCKET {minio_bucket_name}/'{folder_path}' foi criado.")

    except S3Error as err:
        print(f"Error: {err}")

def get_last_page(folder_path:str):
    print(folder_path + 'atualizado 11:17')
    minio_client = get_minio_client()

    # Lista todos os arquivos na pasta
    folder_objects = minio_client.list_objects(minio_bucket_name, prefix=folder_path, recursive=True)
    # Se houver arquivos arquivos, pego a ultima pagina gravada
    files_in_folder = [obj.object_name for obj in folder_objects][1:]
    if files_in_folder:
        pages_array = [int(re.sub(r'\D','',arquivo)) for arquivo in files_in_folder]
        last_page = max(pages_array)
        return last_page + 1
    else:
        return 1
        
        
def get_player_by_name(player_name:str):
    base_url = 'https://br1.api.riotgames.com'
    endpoint = f"/lol/summoner/v4/summoners/by-name/{player_name}"
    token = riot_api_token
    request_url = f"{base_url}{endpoint}"
    headers = {
        "X-Riot-Token": f"{token}"
        }
    
    response = requests.get(request_url, headers=headers)

    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print("Error API Status-Code: ", response.status_code)

def upload_json_to_minio(bucket_name, folder_path, filename, data):
    minio_client = get_minio_client()
    try:
        # CONVERTE JSON PARA BYTES 
        json_bytes = json.dumps(data, ensure_ascii=False).encode('utf-8')

        # FAZ UPLOAD TO MINIO 
        object_name = os.path.join(folder_path, filename)
        minio_client.put_object(bucket_name, object_name, io.BytesIO(json_bytes), len(json_bytes))

        print(f"UPLOAD JSON  '{filename}' COM SUCESSO.")

    except S3Error as err:
        print(f"Error: {err}")

##TODO: Aplicar principios do SOLID(S)
def get_player_by_rank(tier:str, division:str):
    base_url = 'https://br1.api.riotgames.com'
    endpoint = f"/lol/league-exp/v4/entries/RANKED_SOLO_5x5/{tier}/{division}"
    token = riot_api_token
    request_url = f"{base_url}{endpoint}"
    headers = {
        "X-Riot-Token": f"{token}"
        }
    
    #folder_path = f"src/raw/{tier.lower()}/{division}"
    folder_path = f"{tier.lower()}_{division}"
    if not check_folder_exists(minio_bucket_name,folder_path):
        create_folders(minio_bucket_name, folder_path)
        page = 1
    else:
        page = get_last_page(folder_path+'/')

    # i = 1#controle para não estourar os limite da api
    
    print(f'[EXCUTION] Iniciando {tier}-{division}: Pagina {page}')
    while True: # Loop para passar por todas as paginas

        resquest_url_param = f"{request_url}?page={page}"
        response = requests.get(resquest_url_param, headers=headers)


        if (response.status_code == 200): # and i <= 200 Retorno 200 indica sucesso e apendo o resultado no meu array de players
            data = json.loads(response.text) 
            if data:
                upload_json_to_minio(minio_bucket_name, folder_path+'/', f"{page}.json", data)

                page += 1 #Incrementa para pegar próxima pagina
                # i+=1 #Contador devido a limitaçao da API 
            else:
                print(f'[{tier}-{division}] Pagina {page} retornando vazias')
                break
        
        else: #Retorno diferente de 200 indica ERRO ou fim de paginas, então paro o LOOP
            print("Error API Status-Code: ", response.status_code)
            #print("Erro na requisição da API")
            break
            
        time.sleep(0.9)

    
RANKS = {
        "BRONZE":['I','II','III','IV']
        ,"SILVER": ['I','II','III','IV']
        ,"GOLD" :['I','II','III','IV']
        ,"PLATINUM":['I','II','III','IV']
        # ,"EMERALD":['I','II','III','IV']
        # ,"DIAMOND":['I','II','III','IV']
        # ,"MASTER":['I']
        # ,"GRANDMASTER":['I']
        # ,"CHALLENGER":['I']
}

tier = sys.argv[1]
print(f"Execução do TIER: {tier}")
for division in RANKS[tier]:
    get_player_by_rank(tier=tier, division=division)