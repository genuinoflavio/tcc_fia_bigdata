import os
from airflow import DAG
from random import randint
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'Flavio', 
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id="silver_players_ingestion"
         ,default_args=default_args
         ,description='DAG responsável por fazer a ingestão dos players SILVER.'
         , schedule_interval=None
      #    , schedule_interval=timedelta(days=1)
         ,) as dag:
    


    start_dag = DummyOperator(
                    task_id='start_dag',
                    dag=dag
                    )
    
    
    dag_finish = DummyOperator(
                     task_id='dag_finish',
                     dag=dag
                     )
    with TaskGroup(
       group_id='RAW_LAYER'
       ,ui_color='green'
       ,ui_fgcolor='black'
       ,tooltip='Essa task executa a extração dos jogadores de LOL da API para a camanda RAW'
      ) as process_data:
    
      raw_ingestion = SparkSubmitOperator(
            task_id='all_players_json_to_minio',
            conn_id='spark_local',
            application_args=['SILVER'],
            application='/usr/local/airflow/dags/python_scripts/raw_to_minio.py',
            dag=dag,
      )

    with TaskGroup(
       group_id="BRONZE_LAYER"
       ,ui_color="yellow" 
       ,ui_fgcolor="black"
       ,tooltip="Essa task executa a ingestão da camada RAW -> BRONZE em tabelas DELTA por rank e subdivisão"
      ) as process_data:

      bronze_ingestion = DummyOperator(
                  task_id='bronze_layer_ingestion',
                  dag=dag
                  )

      silver_players_b = SparkSubmitOperator(
                                    task_id=f'SILVER_PLAYERS_to_BRONZE_LAYER',
                                    conn_id='spark_local',
                                    jars='/usr/local/airflow/jars/hadoop-azure-3.2.1.jar,\
                                          /usr/local/airflow/jars/hadoop-common-3.3.2.jar,\
                                          /usr/local/airflow/jars/hadoop-aws-3.3.2.jar,\
                                          /usr/local/airflow/jars/aws-java-sdk-bundle-1.11.874.jar,\
                                          /usr/local/airflow/jars/azure-storage-8.6.4.jar,\
                                          /usr/local/airflow/jars/jetty-util-ajax-11.0.7.jar,\
                                          /usr/local/airflow/jars/jetty-util-9.3.25.v20180904.jar,\
                                          /usr/local/airflow/jars/postgresql-42.3.3.jar,\
                                          /usr/local/airflow/jars/ojdbc6-11.2.0.3.jar,\
                                          /usr/local/airflow/jars/delta-core_2.12-2.0.0.jar,\
                                          /usr/local/airflow/jars/delta-storage-2.0.0.jar'.replace(' ', ''),
                                    conf={'spark.driver.host' : 'localhost',
                                          'spark.ui.port' : randint(4040, 5050),
                                          'spark.executor.extraJavaOptions': '-Djava.security.egd=file:///dev/urandom -Duser.timezone=UTC',
                                          'spark.driver.extraJavaOptions': '-Djava.security.egd=file:///dev/urandom -Duser.timezone=UTC'},
                                    driver_memory='500m',
                                    num_executors=1,
                                    executor_memory='250m',
                                    name=f'task_id',
                                    application='/usr/local/airflow/dags/spark_scripts/bronze/silver_players_raw_to_bronze.py',
                                    execution_timeout=timedelta(minutes=180),
                                    dag=dag
                              ) 

      
    with TaskGroup(
       group_id="SILVER_LAYER"
       ,ui_color="blue" 
       ,ui_fgcolor="black"
       ,tooltip="Essa task executa a ingestão da camada BRONZE -> SILVER em tabelas DELTA"
      ) as process_data:

      silver_ingestion = DummyOperator(
            task_id='silver_layer_ingestion',
            dag=dag
            )


      silver_players_s = SparkSubmitOperator(
                                    task_id=f'SILVER_PLAYERS_to_SILVER_LAYER',
                                    conn_id='spark_local',
                                    jars='/usr/local/airflow/jars/hadoop-azure-3.2.1.jar,\
                                          /usr/local/airflow/jars/hadoop-common-3.3.2.jar,\
                                          /usr/local/airflow/jars/hadoop-aws-3.3.2.jar,\
                                          /usr/local/airflow/jars/aws-java-sdk-bundle-1.11.874.jar,\
                                          /usr/local/airflow/jars/azure-storage-8.6.4.jar,\
                                          /usr/local/airflow/jars/jetty-util-ajax-11.0.7.jar,\
                                          /usr/local/airflow/jars/jetty-util-9.3.25.v20180904.jar,\
                                          /usr/local/airflow/jars/postgresql-42.3.3.jar,\
                                          /usr/local/airflow/jars/ojdbc6-11.2.0.3.jar,\
                                          /usr/local/airflow/jars/delta-core_2.12-2.0.0.jar,\
                                          /usr/local/airflow/jars/delta-storage-2.0.0.jar'.replace(' ', ''),
                                    conf={'spark.driver.host' : 'localhost',
                                          'spark.ui.port' : randint(4040, 5050),
                                          'spark.executor.extraJavaOptions': '-Djava.security.egd=file:///dev/urandom -Duser.timezone=UTC',
                                          'spark.driver.extraJavaOptions': '-Djava.security.egd=file:///dev/urandom -Duser.timezone=UTC'},
                                    driver_memory='500m',
                                    num_executors=1,
                                    executor_memory='250m',
                                    name=f'task_id',
                                    application='/usr/local/airflow/dags/spark_scripts/silver/silver_players_bronze_to_silver.py',
                                    execution_timeout=timedelta(minutes=180),
                                    dag=dag
                              ) 


    with TaskGroup(
       group_id="GOLD_LAYER"
       ,ui_color="grey" 
       ,ui_fgcolor="black"
       ,tooltip="Essa task executa a ingestão na camada SILVER -> GOLD em tabelas DELTA"
      ) as process_data:

      gold_ingestion = DummyOperator(
            task_id='gold_layer_ingestion',
            dag=dag
            )
      
      silver_players_g = SparkSubmitOperator(
                              task_id=f'SILVER_PLAYERS_to_GOLD_LAYER',
                              conn_id='spark_local',
                              jars='/usr/local/airflow/jars/hadoop-azure-3.2.1.jar,\
                                    /usr/local/airflow/jars/hadoop-common-3.3.2.jar,\
                                    /usr/local/airflow/jars/hadoop-aws-3.3.2.jar,\
                                    /usr/local/airflow/jars/aws-java-sdk-bundle-1.11.874.jar,\
                                    /usr/local/airflow/jars/azure-storage-8.6.4.jar,\
                                    /usr/local/airflow/jars/jetty-util-ajax-11.0.7.jar,\
                                    /usr/local/airflow/jars/jetty-util-9.3.25.v20180904.jar,\
                                    /usr/local/airflow/jars/postgresql-42.3.3.jar,\
                                    /usr/local/airflow/jars/ojdbc6-11.2.0.3.jar,\
                                    /usr/local/airflow/jars/delta-core_2.12-2.0.0.jar,\
                                    /usr/local/airflow/jars/delta-storage-2.0.0.jar'.replace(' ', ''),
                              conf={'spark.driver.host' : 'localhost',
                                    'spark.ui.port' : randint(4040, 5050),
                                    'spark.executor.extraJavaOptions': '-Djava.security.egd=file:///dev/urandom -Duser.timezone=UTC',
                                    'spark.driver.extraJavaOptions': '-Djava.security.egd=file:///dev/urandom -Duser.timezone=UTC'},
                              driver_memory='500m',
                              num_executors=1,
                              executor_memory='250m',
                              name=f'task_id',
                              application='/usr/local/airflow/dags/spark_scripts/gold/silver_players_silver_to_gold.py',
                              execution_timeout=timedelta(minutes=180),
                              dag=dag
                        )
      

start_dag >> raw_ingestion >> \
      bronze_ingestion  >> silver_players_b >> \
            silver_ingestion >> [ silver_players_s] >> \
                  gold_ingestion >> silver_players_g >> dag_finish