# src/main.py

import uuid
import sys
from tasks import cleanse, validate, cleanup

def main():
    """
    Orchestrates the file processing pipeline by running tasks sequentially.
    Generates a unique Run ID to isolate files for this specific job.
    """
    run_id = str(uuid.uuid4())
    print(f"🚀 Starting workflow for Run ID: {run_id}")

    try:
        # Task 1: Cleanse the file and get the original path
        cleansed_blob, original_path = cleanse.run(run_id)

        # Task 2: Validate the cleansed file, passing the original path for naming
        validated_blob = validate.run(run_id, cleansed_blob, original_path)

    except Exception as e:
        print(f"❌ Workflow failed for Run ID: {run_id}. Error: {e}", file=sys.stderr)
        print("Intermediate files are left in the 'in-progress' container for debugging.", file=sys.stderr)
        sys.exit(1)

    # Final Step: Clean up intermediate files only on a successful run
    try:
        cleanup.run(run_id)
    except Exception as e:
        print(f"⚠️ Cleanup failed for Run ID: {run_id}. Error: {e}", file=sys.stderr)

    print(f"\n✅ Workflow completed successfully for Run ID: {run_id}")

if __name__ == "__main__":
    main()