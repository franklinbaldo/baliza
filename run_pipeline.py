import os
import dlt

# Change the working directory to the root of the project
os.chdir('/app')

from baliza.pipeline import run_sync_pipeline

if __name__ == "__main__":
    run_sync_pipeline()
