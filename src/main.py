# src/main.py

import uuid
import sys
from tasks import cleanse, validate, cleanup

def main():
    """
    Orchestrates the file processing pipeline by running tasks sequentially.
    Generates a unique Run ID to isolate files for this specific job.
    """
    # Generate a unique ID for this entire run
    run_id = str(uuid.uuid4())
    print(f"🚀 Starting workflow for Run ID: {run_id}")

    try:
        # Task 1: Cleanse the file from the queue
        cleansed_blob = cleanse.run(run_id)

        # Task 2: Validate the cleansed file
        validated_blob = validate.run(run_id, cleansed_blob)

        # --- Add more sequential tasks here as needed ---
        # enriched_blob = enrich.run(run_id, validated_blob)

    except Exception as e:
        print(f"❌ Workflow failed for Run ID: {run_id}. Error: {e}", file=sys.stderr)
        print("Intermediate files are left in the 'in-progress' container for debugging.", file=sys.stderr)
        sys.exit(1) # Exit with a non-zero status code to indicate failure

    # Final Step: Clean up intermediate files only on a successful run
    try:
        cleanup.run(run_id)
    except Exception as e:
        print(f"⚠️ Cleanup failed for Run ID: {run_id}. Error: {e}", file=sys.stderr)

    print(f"\n✅ Workflow completed successfully for Run ID: {run_id}")

if __name__ == "__main__":
    main()