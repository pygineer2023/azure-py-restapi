import os

from flask import Flask, request, jsonify
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.filedatalake import DataLakeServiceClient
from azure.synapse.spark import SparkBatchJob, SparkClient
import logging
import zipfile
import io

app = Flask(__name__)

# Azureリソースへの認証
credential = DefaultAzureCredential()

# Azure Key Vault
# Key Vaultへのアクセス
key_vault_url = os.environ["KEY_VAULT_URL"]
key_vault_client = SecretClient(vault_url=key_vault_url, credential=credential)

# Azure Synapse Spark
# Synapse WorkspaceのSparkクライアントを作成
synapse_workspace_endpoint = os.environ["SYNAPSE_WORKSPACE_ENDPOINT"]
spark_client = SparkClient(synapse_workspace_endpoint, credential=credential)

# ログ設定
logging.basicConfig(level=logging.DEBUG)

@app.route('/api', methods=['POST'])
def api():
    try:
        # 受信したZIPファイルのチェック
        zip_file = get_zip_file_from_request()
        check_zip_file_contents(zip_file)

        # Key Vaultからシークレットを取得
        storage_account_name, storage_account_key = get_storage_account_secrets()

        # ユーザー割り当てマネージドIDを使用して、受信したCSVファイルをAzure Data Lake Storageのコンテナに配置
        upload_files_to_datalake(zip_file, storage_account_name, storage_account_key)

        # ユーザー割り当てマネージドIDを使用して、Synapse WorkspaceのSparkプールに配置済みのdotDataを使用して予測モデルを作成
        create_and_run_spark_job()

        logging.info('予測結果...')  # 実際のログメッセージに置き換えてください

        return jsonify({
            'message': '予測結果...',  # 実際の予測結果に置き換えてください
            'status': 'success'
        })
    except Exception as e:
        # エラーハンドリング
        logging.error('エラーが発生しました: %s', str(e))

        return jsonify({
            'message': str(e),
            'status': 'error'
        })


def get_zip_file_from_request():
    if 'file' not in request.files:
        raise ValueError('ファイルが見つかりません')
    file = request.files['file']
    if file.filename == '':
        raise ValueError('ファイルが選択されていません')
    if not zipfile.is_zipfile(file):
        raise ValueError('ファイルがZIPファイルではありません')
    return zipfile.ZipFile(io.BytesIO(file.read()))


def check_zip_file_contents(zip_file):
    # ZIPファイルのチェックを実行してください
    pass


def get_storage_account_secrets():
    # Key Vaultからストレージアカウントのシークレットを取得して、アカウント名とアカウントキー
    storage_account_name_secret = key_vault_client.get_secret(os.environ["STORAGE_ACCOUNT_NAME_SECRET_NAME"])
    storage_account_name = storage_account_name_secret.value

    storage_account_key_secret = key_vault_client.get_secret(os.environ["STORAGE_ACCOUNT_KEY_SECRET_NAME"])
    storage_account_key = storage_account_key_secret.value

    return storage_account_name, storage_account_key


def upload_files_to_datalake(zip_file, storage_account_name, storage_account_key):
    # Data Lake Storageのファイルシステムクライアントを作成
    data_lake_service_client = DataLakeServiceClient(account_url=f"https://{storage_account_name}.dfs.core.windows.net",
                                                     credential=DefaultAzureCredential())

    # ファイルをData Lake Storageにアップロード
    file_system_client = data_lake_service_client.get_file_system_client(os.environ["FILE_SYSTEM_NAME"])
    for file_info in zip_file.infolist():
        if not file_info.is_file():
            continue

        with zip_file.open(file_info.filename) as file:
            data = file.read()

            directory_client = file_system_client.get_directory_client(os.environ["DIRECTORY_NAME"])
            file_client = directory_client.get_file_client(file_info.filename)
            file_client.upload_data(data, overwrite=True)


def create_and_run_spark_job():
    # Synapse WorkspaceのSparkプールでSparkバッチジョブを作成・実行する
    job_config = SparkBatchJob()  # ここでSparkジョブを設定してください
    spark_job = spark_client.spark_batch.create_spark_batch_job(job_config)


if __name__ == "__main__":
    app.run(debug=True)
