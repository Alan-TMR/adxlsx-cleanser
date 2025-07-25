# src/main.py

import uuid
import sys
from tasks import cleanse, validate, cleanup

def main():
    """Orchestrates the file processing pipeline by running tasks sequentially."""
    run_id = str(uuid.uuid4())
    print(f"🚀 Starting workflow for Run ID: {run_id}")

    try:
        # Task 1: Cleanse all sheets and get a list of intermediate files
        cleansed_blob_list, original_path = cleanse.run(run_id)

        # Task 2: Loop through each cleansed file and validate it
        for cleansed_blob in cleansed_blob_list:
            validate.run(run_id, cleansed_blob, original_path)

    except Exception as e:
        print(f"❌ Workflow failed for Run ID: {run_id}. Error: {e}", file=sys.stderr)
        print("Intermediate files are left in 'in-progress' container for debugging.", file=sys.stderr)
        sys.exit(1)

    # Final Step: Clean up all intermediate files for the run
    try:
        cleanup.run(run_id)
    except Exception as e:
        print(f"⚠️ Cleanup failed for Run ID: {run_id}. Error: {e}", file=sys.stderr)

    print(f"\n✅ Workflow completed successfully for Run ID: {run_id}")

if __name__ == "__main__":
    main()