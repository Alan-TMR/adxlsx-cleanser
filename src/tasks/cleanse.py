# src/tasks/cleanse.py

import os
import io
import json
import base64
import pandas as pd
from urllib.parse import unquote, urlparse
from common import azure

QUEUE_NAME = os.getenv("QUEUE_NAME", "eventqueue")
INTERMEDIATE_CONTAINER = "in-progress"

def run(run_id: str) -> tuple[str, str]:
    """
    Pulls a message, converts the Excel file to CSV, and saves it to a temporary location.

    Args:
        run_id: The unique identifier for this workflow run.

    Returns:
        A tuple containing the intermediate blob name and the original relative file path.
    """
    print(f"[{run_id}] Running 'cleanse' task...")

    q_client = azure.get_queue_client(QUEUE_NAME)
    msg = next(q_client.receive_messages(), None)
    if not msg:
        raise RuntimeError("No messages in the queue to process.")

    try:
        event = json.loads(msg.content)
    except json.JSONDecodeError:
        event = json.loads(base64.b64decode(msg.content).decode())
    
    blob_url = unquote(event.get("data", {}).get("url", event.get("url")))
    
    parsed_path = urlparse(blob_url).path.lstrip('/')
    source_container = parsed_path.split('/')[0]
    original_relative_path = '/'.join(parsed_path.split('/')[1:])
    
    in_blob = azure.get_blob_client(source_container, original_relative_path)
    data = in_blob.download_blob().readall()
    print(f"[{run_id}] Downloaded blob: {in_blob.blob_name}")
    
    q_client.delete_message(msg)

    df = pd.read_excel(io.BytesIO(data), engine="openpyxl")
    
    output_blob_name = f"{run_id}/01-cleansed.csv"
    out_blob = azure.get_blob_client(INTERMEDIATE_CONTAINER, output_blob_name)
    out_blob.upload_blob(df.to_csv(index=False).encode('utf-8'), overwrite=True)
    
    print(f"[{run_id}] Cleansed file saved to: {INTERMEDIATE_CONTAINER}/{output_blob_name}")
    
    return output_blob_name, original_relative_path