# airflow/dags/data_processing_dag.py

import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

# === Всегда эти константы ===
YC_DP_AZ = 'ru-central1-b'
YC_DP_SSH_PUBLIC_KEY = (
    'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHjD1pVAPedn1Kaq8xO7ixYfLF7nyukGT37SYIbKDgpW '
    'f02volodin@yandex.ru'
)
YC_DP_SUBNET_ID = 'e2l493987tqlbhl1o6bq'
YC_DP_SA_ID = 'aje4h1l7o6d0pu28o45h'
YC_DP_METASTORE_URI = '10.129.0.22'
YC_BUCKET = 'etlexam'

with DAG(
    dag_id='data_processing_dag',
    schedule_interval='@daily',
    start_date=datetime.datetime(2025, 6, 18),
    catchup=False,
    max_active_runs=1,
    tags=['data-processing-and-airflow'],
) as dag:

    # 1. Создать кластер (нет HDFS-узлов — datanode_count=0)
    create_dp_cluster = DataprocCreateClusterOperator(
        task_id='create_dp_cluster',
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        cluster_description='Spark cluster for CSV ingestion',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_BUCKET,
        zone=YC_DP_AZ,
        cluster_image_version='2.1',
        # мастер-нода
        masternode_resource_preset='s2.micro',
        masternode_disk_type='network-ssd',
        masternode_disk_size=30,
        # вычислительные ноды
        computenode_resource_preset='s2.micro',
        computenode_disk_type='network-ssd',
        computenode_disk_size=30,
        computenode_count=1,
        # HDFS-узлы отключены
        datanode_count=0,
        # только Spark + YARN, без HDFS/OOZIE
        services=['YARN', 'SPARK'],
        properties={
            'spark:spark.hive.metastore.uris': f'thrift://{YC_DP_METASTORE_URI}:9083',
        },
    )

    # 2. Запустить PySpark-job
    run_pyspark_job = DataprocCreatePysparkJobOperator(
        task_id='run_pyspark_job',
        main_python_file_uri=f's3a://{YC_BUCKET}/scripts/process_csv.py',
        args=[
            '--input_path',  f's3a://{YC_BUCKET}/TwizzlerData.csv',
            '--output_path', f's3a://{YC_BUCKET}/output/TwizzlerData_clean.parquet',
        ],
        cluster_id='{{ ti.xcom_pull(task_ids="create_dp_cluster", key="cluster_id") }}',
    )

    # 3. Удалить кластер (всегда, даже при ошибках)
    delete_dp_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dp_cluster',
        cluster_id='{{ ti.xcom_pull(task_ids="create_dp_cluster", key="cluster_id") }}',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_dp_cluster >> run_pyspark_job >> delete_dp_cluster
