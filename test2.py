from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Azure Key Vaultの情報
key_vault_url = 'https://your-key-vault-name.vault.azure.net/'
secret_name = 'your-secret-name'

# システム割り当てマネージドIDを使用してキーボルトからシークレットを取得
def get_secret_from_keyvault():
    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
    secret = secret_client.get_secret(secret_name)
    return secret.value

# Flaskアプリケーションにシークレット取得のエンドポイントを追加
@app.route('/get_secret', methods=['GET'])
def get_secret():
    try:
        secret = get_secret_from_keyvault()
        return secret
    except Exception as e:
        logging.error(str(e))
        return 'Error occurred', 500
