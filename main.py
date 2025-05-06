import os
import json
import base64
import pandas as pd
import logging
from datetime import datetime
from flask import Flask, request, jsonify
from google.cloud import storage, firestore

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()

# Configuration variables
BUCKET_NAME = os.getenv("BUCKET_NAME", "gcs-csv-reporter")
RAW_DATA_FOLDER = os.getenv("RAW_DATA_FOLDER", "raw-data")
REPORTS_FOLDER = os.getenv("REPORTS_FOLDER", "reports")
PROCESSED_COLLECTION = os.getenv("PROCESSED_COLLECTION", "processed_files")
# SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/royaldsouza/Downloads/my_gcp_project.json") # for local dev
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_FILE # for local dev

# Initialize clients
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)
firestore_client = firestore.Client()

app = Flask(__name__)

def parse_pubsub_message(envelope):
    """Parse the Pub/Sub message from the request envelope."""
    if not envelope or 'message' not in envelope:
        logger.warning("Bad request: No message found in envelope")
        print("Bad request: No message found in envelope")
        raise ValueError("Invalid Pub/Sub message format")
    
    pubsub_message = base64.b64decode(envelope['message']['data']).decode('utf-8')
    return json.loads(pubsub_message)

def validate_file_info(bucket_name, file_path):
    """Validate the bucket name and file path."""
    if bucket_name != BUCKET_NAME:
        logger.warning(f"Bucket mismatch: expected {BUCKET_NAME}, got {bucket_name}")
        print(f"Bucket mismatch: expected {BUCKET_NAME}, got {bucket_name}")
        raise ValueError(f"Invalid bucket: expected {BUCKET_NAME}, got {bucket_name}")
        
    if not file_path.startswith(f"{RAW_DATA_FOLDER}/") or not file_path.endswith(".csv"):
        logger.warning(f"Invalid file path: {file_path}. Only CSVs in {RAW_DATA_FOLDER}/ folder are allowed")
        print(f"Invalid file path: {file_path}. Only CSVs in {RAW_DATA_FOLDER}/ folder are allowed")
        raise ValueError(f"Invalid file path: {file_path}. Only CSVs in {RAW_DATA_FOLDER}/ folder are allowed")
    
def check_already_processed(file_name, output_blob_name):
    """Check if the file has already been processed."""
    # Check if output already exists in GCS
    blob = bucket.blob(output_blob_name)
    if blob.exists():
        logger.info(f"{file_name} already exists in GCS at {output_blob_name}")
        print(f"{file_name} already exists in GCS at {output_blob_name}")
        return True
    
    # Check if processing record exists in Firestore
    doc_ref = firestore_client.collection(PROCESSED_COLLECTION).document(file_name)
    if doc_ref.get().exists:
        logger.info(f"{file_name} already processed (found in Firestore)")
        print(f"{file_name} already processed (found in Firestore)")
        return True
    
    return False

def read_csv_from_gcs(file_path):
    """Read a CSV file from Google Cloud Storage."""
    gs_file_path = f'gs://{BUCKET_NAME}/{file_path}'
    return pd.read_csv(gs_file_path)

def generate_metrics(df):
    """Generate metrics from a DataFrame."""
    return {
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "null_counts": df.isnull().sum().to_dict(),
        "datatype_summary": {col: str(dtype) for col, dtype in df.dtypes.items()}
    }

def save_metrics_to_gcs(blob_name, metrics):
    """Save metrics to Google Cloud Storage."""
    blob = bucket.blob(blob_name)
    blob.upload_from_string(
        data=json.dumps(metrics, indent=2),
        content_type='application/json'
    )
    logger.info(f"Uploaded metrics JSON to GCS: {blob_name}")
    print(f"Uploaded metrics JSON to GCS: {blob_name}.")

def save_metadata_to_firestore(file_name, file_path, metrics):
    """Save processing metadata to Firestore."""
    doc_ref = firestore_client.collection(PROCESSED_COLLECTION).document(file_name)
    doc_ref.set({
        'file_path': file_path,
        'processed_at': datetime.now(),
        'metrics_summary': {
            'row_count': metrics['row_count'],
            'column_count': metrics['column_count']
        }
    })
    logger.info(f"Logged processing metadata to Firestore for file: {file_name}")
    print(f"Logged processing metadata to Firestore for file: {file_name}")

def process_csv(file_path, file_name, output_blob_name):
    """Process a CSV file and generate metrics."""
    # Read CSV
    df = read_csv_from_gcs(file_path)
    
    # Generate metrics
    metrics = generate_metrics(df)
    
    # Save metrics to GCS
    save_metrics_to_gcs(output_blob_name, metrics)
    
    # Save metadata to Firestore
    save_metadata_to_firestore(file_name, file_path, metrics)
    
    return metrics

@app.route("/", methods=["POST"])
def main():
    """Main handler for Pub/Sub triggered CSV processing."""
    try:
        # Parse the Pub/Sub message
        envelope = request.get_json()
        event_data = parse_pubsub_message(envelope)
        
        # Extract and validate file information
        bucket_name = event_data.get('bucket')
        file_path = event_data.get('name')
        
        if not bucket_name or not file_path:
            return jsonify({"status": "error", "message": "Missing bucket or file path"}), 400
        
        validate_file_info(bucket_name, file_path)
        
        # Extract file name information
        file_name = os.path.basename(file_path)
        file_name_without_ext = os.path.splitext(file_name)[0]
        output_blob_name = f"{REPORTS_FOLDER}/{file_name_without_ext}_metrics.json"
        
        # Check if already processed
        if check_already_processed(file_name, output_blob_name):
            return jsonify({
                "status": "skipped",
                "message": f"File {file_name} already processed"
            }), 200
        
        # Process the CSV file
        metrics = process_csv(file_path, file_name, output_blob_name)
        
        # Return success response
        return jsonify({
            "status": "success", 
            "input_file": file_path,
            "output_file": output_blob_name,
            'metrics_summary': {
                'row_count': metrics['row_count'],
                'column_count': metrics['column_count']
            }
        }), 200
        
    except ValueError as e:
        logger.warning(f"Validation error: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        return jsonify({"status": "error", "message": f"Internal error: {str(e)}"}), 500

if __name__ == "__main__":
    PORT = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=PORT)