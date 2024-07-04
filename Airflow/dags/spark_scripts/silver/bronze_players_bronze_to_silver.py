from pyspark.sql.types import StructType
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter
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
            #.config("spark.hadoop.fs.s3a.endpoint", "http://172.20.0.2:9000")
             .config("spark.hadoop.fs.s3a.endpoint", "minio:9000")

            .config("spark.hadoop.fs.s3a.access.key", os.environ.get('MINIO_ACCESS_KEY_USER'))
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get('MINIO_ACCESS_KEY_SECRET'))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            ## Jars
            .config("spark.jars", "/home/jovyan/work/jars/hadoop-common-3.3.2.jar,\
                                    /home/jovyan/work/jars/hadoop-aws-3.3.2.jar, \
                                    /home/jovyan/work/jars/aws-java-sdk-bundle-1.11.874.jar")
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            .getOrCreate()
    )
    return spark

spark = minio_session_spark()

# spark
print(f"Spark version = {spark.version}")

# hadoop
print(f"Hadoop version = {spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")


#MINIO CONFIGS
minio_endpoint = os.environ.get('MINIO_ENDPOINT')
minio_access_key = os.environ.get('MINIO_ROOT_USER')
minio_secret_key = os.environ.get('MINIO_ROOT_PASSWORD')
secure = False  
minio_client = Minio(endpoint=minio_endpoint, access_key=minio_access_key, secret_key=minio_secret_key, secure=secure)


minio_bucket = 'bronze'
# minio_path_bronze_players = ['bronze_I/', 'bronze_II/','bronze_III/','bronze_IV/']
minio_path_bronze_players = ['bronze_II/','bronze_III/','bronze_IV/']

#BRONZE PLAYERS
final_df = None
for i in range(len(minio_path_bronze_players)):
    df = (
    spark
    .read
    .format('delta')
    .load(f"s3a://{minio_bucket}/{minio_path_bronze_players[i]}")
    )


    if final_df is None:
        final_df = df
    else:
        final_df = final_df.union(df)
        
#Salvando delta table
(
    final_df
    .write
    .format("delta")
    .mode("overwrite") 
    .option("overwriteSchema", "True")
    .save(f"s3a://silver/" + 'bronze_players')
)