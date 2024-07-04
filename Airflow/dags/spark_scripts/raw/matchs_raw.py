import requests
import json
import os
import re
import time
from minio import Minio
from minio.error import S3Error
import sys
import io
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.window import Window
import pyspark.sql.functions as F

def minio_session_spark():
    spark = (
        SparkSession.builder
            .master("local[*]")
            .appName("appMinIO")
            ### Config Fields
            .config('spark.sql.debug.maxToStringFields', 5000)
            .config('spark.debug.maxToStringFields', 5000)
            ### Optimize
            .config("delta.autoOptimize.optimizeWrite", "true")
            .config("delta.autoOptimize.autoCompact", "true")
            ### Delta Table
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            ## MinIO
             .config("spark.hadoop.fs.s3a.endpoint", "minio:9000")

            .config("spark.hadoop.fs.s3a.access.key", os.environ.get('MINIO_ACCESS_KEY_USER'))
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get('MINIO_ACCESS_KEY_SECRET'))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            ## Jars
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            .getOrCreate()
    )
    return spark

spark = minio_session_spark()
tier = sys.argv[1]

def get_top_players():


    df = (
    spark
        .read
        .format('delta')
        .load(f"s3a://gold/tb_lol_{tier.lower()}_players")
    )

    #define a janela para partição
    window_spec = (
        Window
        .partitionBy("rank")
        .orderBy(F.col("leaguePoints").desc(),
                F.col("win_percentage").desc())
    )

    #Cria lista dos top 2 players
    list_top_players = (
        df
        .filter(( F.col("wins") + F.col("losses") >= 20))
        .withColumn("win_percentage", F.col("wins") / ( F.col("wins") + F.col("losses" )))
        .withColumn("rn", F.row_number().over(window_spec))
        .select("summonerId")
        .filter(F.col("rn") <= 2)
        .rdd
        .flatMap(lambda x: x)
        .collect()
    )

    return list_top_players


#MINIO CONFIGS
minio_endpoint = os.environ.get('MINIO_ENDPOINT')
minio_access_key = os.environ.get('MINIO_ROOT_USER')
minio_secret_key = os.environ.get('MINIO_ROOT_PASSWORD')
minio_bucket_name  = "raw"

riot_api_token = os.environ.get('API_TOKEN') 


def get_minio_client():
    minio_client = Minio(minio_endpoint,
                            access_key=minio_access_key,
                            secret_key=minio_secret_key,
                            secure=False)

    return minio_client

def get_puuid_by_name(player_name:str):
    base_url = 'https://americas.api.riotgames.com'
    endpoint = f"/riot/account/v1/accounts/by-riot-id/{player_name}/br1"
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

def get_puuid_summonerid(summonerid:str):
    base_url = 'https://br1.api.riotgames.com'
    endpoint = f"/lol/summoner/v4/summoners/{summonerid}"
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


def get_match_by_puuid(puuid:str):
    base_url = 'https://americas.api.riotgames.com'
    endpoint = f"/lol/match/v5/matches/by-puuid/{puuid}/ids?startTime=1717200000&endTime=1719791999&type=ranked&start=0&count=20"
    token = riot_api_token
    request_url = f"{base_url}{endpoint}"
    headers = {
        "X-Riot-Token": f"{token}",
        }
    
    response = requests.get(request_url, headers=headers)

    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print("Error API Status-Code: ", response.status_code)

def get_matchs_timeline(match_id:str):
    
    base_url = 'https://americas.api.riotgames.com'
    endpoint = f"/lol/match/v5/matches/{match_id}/timeline"
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

def get_matchs(match_id:str):
    
    base_url = 'https://americas.api.riotgames.com'
    endpoint = f"/lol/match/v5/matches/{match_id}"
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

def upload_json_to_minio(bucket_name, folder_path, filename, data):
    minio_client = get_minio_client()
    if not check_folder_exists(minio_bucket_name,folder_path):
        create_folders(minio_bucket_name, folder_path)
        print(f'Folder {bucket_name}/{folder_path} criado com sucesso')
    try:
        # CONVERTE JSON PARA BYTES 
        json_bytes = json.dumps(data, ensure_ascii=False).encode('utf-8')

        # FAZ UPLOAD TO MINIO 
        object_name = os.path.join(folder_path, filename)
        minio_client.put_object(bucket_name, object_name, io.BytesIO(json_bytes), len(json_bytes))

        print(f"UPLOAD JSON  '{filename}' COM SUCESSO.")

    except S3Error as err:
        print(f"Error: {err}")


list_top_players = get_top_players()

for player in list_top_players:

    player_puuid = get_puuid_summonerid(player)['puuid']
    match_list = get_match_by_puuid(player_puuid)

    for match in match_list:
        match_detail = get_matchs(match)
        upload_json_to_minio(minio_bucket_name
                            ,'matchs'
                            , f"{tier.lower()}_{match}.json" 
                            ,match_detail)