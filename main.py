import subprocess
import os
import logging

# Configure logging
logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def check_file_exists(filepath):
    """Check if a file exists and log a message if it does not."""
    if not os.path.exists(filepath):
        logging.error(f"File not found: {filepath}")
        return False
    return True

def validate_environment():
    """Ensure necessary environment variables and files are available."""
    required_files = ["extract.py", "transform.py", "load.py", ".env"]
    for file in required_files:
        if not check_file_exists(file):
            raise FileNotFoundError(f"Required file missing: {file}")
    logging.info("Environment validation successful.")

def run_stage(script_name):
    """Run a specific pipeline stage."""
    try:
        logging.info(f"Running {script_name}...")
        subprocess.run(["python", script_name], check=True)
        logging.info(f"{script_name} completed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"{script_name} failed with error: {e}")
        raise

def run_pipeline():
    """Execute the ETL pipeline stages in sequence."""
    try:
        logging.info("Starting the pipeline...")

        # Validate environment and dependencies
        validate_environment()

        # Run ETL stages
        run_stage("extract.py")
        run_stage("transform.py")
        run_stage("load.py")

        logging.info("Pipeline completed successfully.")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")

if __name__ == "__main__":
    run_pipeline()
