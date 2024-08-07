from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from minio import Minio
from delta.tables import *
import os

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


minio_bucket = 'raw'
minio_path_ouro_players = ['gold_I/', 'gold_II/','gold_III/','gold_IV/']


#GOLD PLAYERS
for rank in minio_path_ouro_players:
    objects = minio_client.list_objects(minio_bucket, prefix=rank)

   
    json_files = [f"s3a://{minio_bucket}/{obj.object_name}" for obj in objects]
    df = spark.read.json(json_files[1:])
    

    (
        df
        .write
        .format("delta")
        .mode("overwrite") 
        .option("overwriteSchema", "True")
        .save(f"s3a://bronze/" + f'{rank}')
    )
