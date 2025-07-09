import os, io, json, base64, pandas as pd
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobClient
from azure.storage.queue import QueueClient

account = os.getenv("AZURE_STORAGE_ACCOUNT")
queue   = os.getenv("QUEUE_NAME")
cred    = ManagedIdentityCredential()

# Pull one message
q   = QueueClient(f"https://{account}.queue.core.windows.net", queue, credential=cred)
msg = next(q.receive_messages())

raw = msg.content or ""

# Try plain JSON first, else Base-64-decode
try:
    event = json.loads(raw)
except json.JSONDecodeError:
    event = json.loads(base64.b64decode(raw).decode())

q.delete_message(msg)

# Download & convert
blob_url = event["url"]
in_blob  = BlobClient.from_blob_url(blob_url, credential=cred)
data     = in_blob.download_blob().readall()
df       = pd.read_excel(io.BytesIO(data))

# Upload CSV
out_name = os.path.basename(blob_url).replace(".xlsx", ".csv")
out_blob = BlobClient(
    f"https://{account}.blob.core.windows.net",
    container_name="cleansed",
    blob_name=out_name,
    credential=cred)
out_blob.upload_blob(df.to_csv(index=False).encode(), overwrite=True)
print(f"Converted → {out_name}")
