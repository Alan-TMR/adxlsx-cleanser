import os, io, json, base64, pandas as pd
from urllib.parse import unquote, urlparse
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobClient
from azure.storage.queue import QueueClient

account = os.getenv("AZURE_STORAGE_ACCOUNT")
queue   = os.getenv("QUEUE_NAME")
cred    = ManagedIdentityCredential()

# ── 1. pull one message ────────────────────────────────────────────────────────
q   = QueueClient(f"https://{account}.queue.core.windows.net", queue, credential=cred)
msg = next(q.receive_messages())
raw = msg.content or ""

try:
    event = json.loads(raw)                       # plain JSON?
except json.JSONDecodeError:                      # …or EG base-64 payload
    event = json.loads(base64.b64decode(raw).decode())
q.delete_message(msg)

# ── 2. download the Excel file ────────────────────────────────────────────────
blob_url  = unquote(event["url"])                 # decode %2F → /
path_part = urlparse(blob_url).path               # /stg/2248719/202505/File.xlsx
relative  = "/".join(path_part.split("/")[2:])    # 2248719/202505/File.xlsx
in_blob   = BlobClient.from_blob_url(blob_url, credential=cred)
data      = in_blob.download_blob().readall()

# ── 3. convert to CSV ─────────────────────────────────────────────────────────
df        = pd.read_excel(io.BytesIO(data), engine="openpyxl")

# ── 4. write to cleansed/… preserving the sub-folders ─────────────────────────
out_name  = relative.replace(".xlsx", ".csv")
out_blob  = BlobClient(
    account_url   = f"https://{account}.blob.core.windows.net",
    container_name= "cleansed",
    blob_name     = out_name,                     # cleansed/2248719/202505/File.csv
    credential    = cred)
out_blob.upload_blob(df.to_csv(index=False).encode(), overwrite=True)
print(f"Converted → {out_name}", flush=True)

### Dummy Line