from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Azure Key Vault URL — found in the Azure Portal under Key Vault > Overview
VAULT_URL = "https://your-key-vault-name.vault.azure.net/"

def get_db_credentials() -> dict:
    """Fetch SQL Server credentials from Azure Key Vault."""
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=VAULT_URL, credential=credential)

    return {
        "server":   client.get_secret("sql-server").value,
        "database": client.get_secret("sql-database").value,
        "username": client.get_secret("sql-username").value,
        "password": client.get_secret("sql-password").value,
    }