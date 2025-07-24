# src/tasks/reformat.py

import io, json, base64, pandas as pd
from urllib.parse import unquote, urlparse
from common import azure

# Constants for container and queue names, can be loaded from env vars
QUEUE_NAME = os.getenv("QUEUE_NAME", "xlsx-queue")
INTERMEDIATE_CONTAINER = "in-progress"

def run(run_id: str) -> str:
    """
    Task 1: Pulls a message, downloads the referenced Excel file,
    converts it to CSV, and uploads it to the intermediate container.

    Args:
        run_id: The unique identifier for this workflow run.

    Returns:
        The name of the generated blob in the intermediate container.
    """
    print(f"[{run_id}] Running 'reformat' task...")

    # 1. Pull one message from the queue
    q_client = azure.get_queue_client(QUEUE_NAME)
    msg = next(q_client.receive_messages(), None)
    if not msg:
        raise RuntimeError("No messages in the queue to process.")
    
    # Process the message content
    try:
        event = json.loads(msg.content)
    except json.JSONDecodeError:
        event = json.loads(base64.b64decode(msg.content).decode())
    
    # 2. Download the Excel file
    blob_url = unquote(event.get("data", {}).get("url", event.get("url"))) # Handles EventGrid and plain JSON
    in_blob = azure.get_blob_client(
        urlparse(blob_url).netloc.split('.')[0], # container name from URL
        urlparse(blob_url).path.lstrip('/')      # blob name from URL
    )
    data = in_blob.download_blob().readall()
    print(f"[{run_id}] Downloaded blob: {in_blob.blob_name}")
    
    # Delete the queue message only after successfully starting the download
    q_client.delete_message(msg)

    # 3. Convert to CSV using pandas
    df = pd.read_excel(io.BytesIO(data), engine="openpyxl")
    
    # 4. Write to intermediate storage
    output_blob_name = f"{run_id}/01-reformated.csv"
    out_blob = azure.get_blob_client(INTERMEDIATE_CONTAINER, output_blob_name)
    out_blob.upload_blob(df.to_csv(index=False).encode('utf-8'), overwrite=True)
    
    print(f"[{run_id}] reformated file saved to: {INTERMEDIATE_CONTAINER}/{output_blob_name}")
    return output_blob_name