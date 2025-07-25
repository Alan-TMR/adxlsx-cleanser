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

def run(run_id: str) -> tuple[list[str], str]:
    """
    Pulls a message, reads all sheets from the Excel file, converts each to a CSV,
    and uploads them to the intermediate container.

    Args:
        run_id: The unique identifier for this workflow run.

    Returns:
        A tuple containing a list of the intermediate blob names and the original path.
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

    # Read all sheets into a dictionary of DataFrames
    excel_data = pd.read_excel(io.BytesIO(data), engine="openpyxl", sheet_name=None)
    
    created_blob_names = []
    print(f"[{run_id}] Found {len(excel_data)} sheets to process.")

    # Loop through each sheet and save it as a separate blob
    for sheet_name, df in excel_data.items():
        # Sanitize sheet name for use in blob path
        safe_sheet_name = "".join(c for c in sheet_name if c.isalnum() or c in (' ', '_', '-')).rstrip()
        output_blob_name = f"{run_id}/01-cleansed-{safe_sheet_name}.csv"
        
        out_blob = azure.get_blob_client(INTERMEDIATE_CONTAINER, output_blob_name)
        out_blob.upload_blob(df.to_csv(index=False).encode('utf-8'), overwrite=True)
        
        created_blob_names.append(output_blob_name)
        print(f"[{run_id}] Cleansed file for sheet '{sheet_name}' saved to: {INTERMEDIATE_CONTAINER}/{output_blob_name}")
    
    return created_blob_names, original_relative_path