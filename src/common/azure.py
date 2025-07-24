# src/common/azure.py

import os
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobClient, BlobServiceClient
from azure.storage.queue import QueueClient

# These are initialized once and reused.
cred = ManagedIdentityCredential()
account = os.getenv("AZURE_STORAGE_ACCOUNT")
account_url = f"https://{account}.blob.core.windows.net"

def get_queue_client(queue_name: str) -> QueueClient:
    """Creates and returns an Azure Storage Queue client."""
    return QueueClient(
        account_url=f"https://{account}.queue.core.windows.net",
        queue_name=queue_name,
        credential=cred
    )

def get_blob_service_client() -> BlobServiceClient:
    """Creates and returns a Blob Service Client to manage containers and blobs."""
    return BlobServiceClient(account_url=account_url, credential=cred)


def get_blob_client(container_name: str, blob_name: str) -> BlobClient:
    """Creates and returns a Blob client for a specific container and blob."""
    return BlobClient(
        account_url=account_url,
        container_name=container_name,
        blob_name=blob_name,
        credential=cred
    )