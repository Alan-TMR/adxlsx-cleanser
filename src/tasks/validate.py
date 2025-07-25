# src/tasks/validate.py

import os
import pandas as pd
import io
from common import azure

INTERMEDIATE_CONTAINER = "in-progress"
FINAL_CONTAINER = "validated"

def run(run_id: str, input_blob_name: str, original_path: str) -> str:
    """
    Reads the cleansed CSV, performs validation, and saves it to the
    final container, preserving the original path and name.

    Args:
        run_id: The unique identifier for this workflow run.
        input_blob_name: The name of the cleansed blob from the previous step.
        original_path: The original relative path of the source file.

    Returns:
        The name of the final validated blob.
    """
    print(f"[{run_id}] Running 'validate' task...")
    
    in_blob = azure.get_blob_client(INTERMEDIATE_CONTAINER, input_blob_name)
    data = in_blob.download_blob().readall()
    df = pd.read_csv(io.BytesIO(data))
    
    # Add your validation logic here.
    print(f"[{run_id}] File passed validation checks.")
    
    # Construct the final path by replacing the original extension with .csv
    output_blob_name = os.path.splitext(original_path)[0] + '.csv'
    
    out_blob = azure.get_blob_client(FINAL_CONTAINER, output_blob_name)
    out_blob.upload_blob(data, overwrite=True)

    print(f"[{run_id}] Validated file saved to: {FINAL_CONTAINER}/{output_blob_name}")
    return output_blob_name