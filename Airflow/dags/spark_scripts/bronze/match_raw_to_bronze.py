from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from minio import Minio
from delta.tables import *
import os
import sys
import re
import pyspark.sql.functions as F
from pyspark.sql.window import Window

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
            #.config("spark.hadoop.fs.s3a.endpoint", "http://172.20.0.2:9000")
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


#MINIO CONFIGS
minio_endpoint = os.environ.get('MINIO_ENDPOINT')
minio_access_key = os.environ.get('MINIO_ROOT_USER')
minio_secret_key = os.environ.get('MINIO_ROOT_PASSWORD')
secure = False  
minio_client = Minio(endpoint=minio_endpoint, access_key=minio_access_key, secret_key=minio_secret_key, secure=secure)

#
minio_bucket = 'raw'
folder = 'matchs/'
tier = sys.argv[1]
print(f'TIER PASSADO NO ARGUMENTO: {tier}')
objects = minio_client.list_objects(minio_bucket, prefix=folder)

json_files = [f"s3a://{minio_bucket}/{obj.object_name}" for obj in objects]
pattern = re.compile(f'{tier.lower()}')
filtered_json = [path for path in json_files if pattern.search(path)]
print(f'LISTA FILTRADA PARA O TIER: {filtered_json}')
df = spark.read.json(filtered_json[0:])

df_final = (
    df
    .withColumn('players_exploded', F.explode(F.col("info.participants")))
    .selectExpr("metadata.matchId"
                ,"players_exploded.puuid"
                ,"players_exploded.summonerId"
                ,"players_exploded.championName"
                ,"players_exploded.role"
                ,"players_exploded.assists"
                ,"players_exploded.damageDealtToTurrets"
                ,"players_exploded.damageDealtToObjectives"
                ,"players_exploded.detectorWardsPlaced"
                ,"players_exploded.visionScore"
                ,"players_exploded.visionWardsBoughtInGame"
                ,"players_exploded.wardsKilled"
                ,"players_exploded.wardsPlaced"
                ,"players_exploded.enemyMissingPings"
                ,"players_exploded.enemyVisionPings"
                ,"players_exploded.getBackPings"
                ,"players_exploded.goldEarned"
                ,"players_exploded.goldSpent"
                ,"players_exploded.longestTimeSpentLiving"
                ,"players_exploded.magicDamageDealt"
                ,"players_exploded.magicDamageDealtToChampions"
                ,"players_exploded.magicDamageTaken"
                ,"players_exploded.physicalDamageDealt"
                ,"players_exploded.physicalDamageDealtToChampions"
                ,"players_exploded.physicalDamageTaken"
                ,"players_exploded.totalDamageDealt"
                ,"players_exploded.totalDamageDealtToChampions"
                ,"players_exploded.totalDamageTaken"
                ,"players_exploded.totalTimeSpentDead"  
                ,"players_exploded.spell1Casts"
                ,"players_exploded.spell2Casts"
                ,"players_exploded.spell3Casts"
                ,"players_exploded.spell4Casts"
                ,"players_exploded.teamId"
                ,"players_exploded.win"
               )
)

(
    df_final
    .write
    .format("delta")
    .mode("overwrite") 
    .option("overwriteSchema", "True")
    .save(f"s3a://bronze/" + f'tb_lol_{tier.lower()}_matchs')
)