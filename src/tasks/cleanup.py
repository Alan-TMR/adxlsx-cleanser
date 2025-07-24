# src/tasks/cleanup.py

from common import azure

INTERMEDIATE_CONTAINER = "in-progress"

def run(run_id: str):
    """
    Final Task: Deletes all intermediate files for a given run ID.

    Args:
        run_id: The unique identifier for the workflow run to clean up.
    """
    print(f"[{run_id}] Running 'cleanup' task...")
    blob_service_client = azure.get_blob_service_client()
    container_client = blob_service_client.get_container_client(INTERMEDIATE_CONTAINER)
    
    # List all blobs that start with the run_id prefix
    blob_list = container_client.list_blobs(name_starts_with=run_id)
    blobs_to_delete = [blob.name for blob in blob_list]

    if not blobs_to_delete:
        print(f"[{run_id}] No intermediate files found to delete.")
        return

    # Delete the blobs
    container_client.delete_blobs(*blobs_to_delete)
    print(f"[{run_id}] Successfully deleted {len(blobs_to_delete)} intermediate files.")