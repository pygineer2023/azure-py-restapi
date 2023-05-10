import os
import logging
import zipfile
import io
import time
from flask import Flask, request, jsonify
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from azure.storage.datalake import DataLakeServiceClient
from azure.synapse.spark import SparkBatchJob, SparkClient

app = Flask(__name__)

# Azureリソース名を環境変数から取得
key_vault_name = os.environ['KEY_VAULT_NAME']
synapse_workspace_name = os.environ['SYNAPSE_WORKSPACE_NAME']
storage_account_name = os.environ['STORAGE_ACCOUNT_NAME']
file_system_name = os.environ['FILE_SYSTEM_NAME']
directory_name = os.environ['DIRECTORY_NAME']
model_id_secret_name = os.environ['MODEL_ID_SECRET_NAME']
api_key_secret_name = os.environ['API_KEY_SECRET_NAME']

# Azure Key Vault
credential = DefaultAzureCredential()
key_vault_client = SecretClient(vault_url=f"https://{key_vault_name}.vault.azure.net/", credential=credential)

# Azure Storage
storage_connection_string_secret_name = os.environ['STORAGE_CONNECTION_STRING_SECRET_NAME']
storage_connection_string = key_vault_client.get_secret(storage_connection_string_secret_name).value
blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)

# Azure Data Lake Storage
datalake_connection_string_secret_name = os.environ['DATALAKE_CONNECTION_STRING_SECRET_NAME']
datalake_connection_string = key_vault_client.get_secret(datalake_connection_string_secret_name).value
data_lake_service_client = DataLakeServiceClient.from_connection_string(datalake_connection_string)

# Azure Synapse Spark
synapse_workspace_endpoint = f"https://{synapse_workspace_name}.dev.azuresynapse.net"
spark_client = SparkClient(synapse_workspace_endpoint, credential)

logging.basicConfig(level=logging.DEBUG)

@app.route('/api', methods=['POST'])
def api():
    try:
        # ZIPファイルのチェックと展開
        if 'file' not in request.files:
            return jsonify({'message': 'No file part', 'status': 'error'})
        file = request.files['file']
        if file.filename == '':
            return jsonify({'message': 'No selected file', 'status': 'error'})
        if file and zipfile.is_zipfile(file):
            zip = zipfile.ZipFile(io.BytesIO(file.read()))

            # Data Lake Storageにファイルをアップロード
            upload_files_to_datalake(zip, file_system_name, directory_name)

        # Key Vaultからシークレットを取得
        model_id_secret = key_vault_client.get_secret(model_id_secret_name).value
        api_key_secret = key_vault_client.get_secret(api_key_secret_name).value

        # Sparkジョブを実行して予測モデルを作成
        create_spark_batch_job(synapse_workspace_name, model_id_secret, api_key_secret)

        logging.info('Prediction results...')

        return jsonify({
            'message': 'Prediction results...',
            'status': 'success'
        })
    except Exception as e:
        # エラーハンドリング
        logging.error('An error occurred: %s', str(e))

        return jsonify({
            'message': str(e),
            'status': 'error'
        })

def upload_files_to_datalake(zip_file, file_system_name, directory_name):
    # Data Lake Storageのファイルシステムとディレクトリを取得
    file_system_client = data_lake_service_client.get_file_system_client(file_system_name)
    directory_client = file_system_client.get_directory_client(directory_name)

    # ZIPファイル内のファイルを展開してData Lake Storageにアップロード
    for name in zip_file.namelist():
        with zip_file.open(name) as file:
            file_client = directory_client.create_file(name)
            file_client.append_data(data=file.read(), offset=0, length=len(file.read()))
            file_client.flush_data(len(file.read()))

    logging.info('Files uploaded to Data Lake Storage.')

def create_spark_batch_job(workspace_name, model_id_secret, api_key_secret):
    # Sparkジョブの設定
    job_config = SparkBatchJob(
        name="model-training-job",
        file=f"/mnt/data/code/{workspace_name}-model-training-job.py",
        driver_memory="28g",
        executor_memory="28g",
        num_executors=3,
        conf={
            "spark.jars.packages": "com.microsoft.ml.spark:com.microsoft.azure:1.0.0-rc3",
            "spark.databricks.delta.preview.enabled": "true"
        },
        arguments=[model_id_secret, api_key_secret]
    )

    # Sparkジョブの作成と実行
    spark_job = spark_client.spark_batch.create_spark_batch_job(job_config)
    spark_job.wait_until_finished()
    if spark_job.state != "success":
        raise Exception("Spark job failed.")
    
    logging.info('Spark job succeeded.')
