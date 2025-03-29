import os
import urllib.request
import gzip
import shutil
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import time

# 🔹 Change this to your GCS bucket name
BUCKET_NAME = "de-zoomcamp-wk4_fhv"   

# 🔹 If authenticated via GCP SDK, you can comment out this
CREDENTIALS_FILE = "/workspaces/de_zoomcamp/terraform_demo/Keys/tf_keys.json"  
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

# 🔹 NYC FHV dataset (2019 Jan - Dec)
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-"
MONTHS = [f"{i:02d}" for i in range(1, 13)]  # 01 to 12
DOWNLOAD_DIR = "data"

# 🔹 Set GCS upload chunk size (8MB)
CHUNK_SIZE = 8 * 1024 * 1024  

# 🔹 Ensure download directory exists
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


def download_and_extract(month):
    """Downloads a .csv.gz file, extracts it, and saves it as .csv"""
    gz_file_path = os.path.join(DOWNLOAD_DIR, f"fhv_tripdata_2019-{month}.csv.gz")
    csv_file_path = os.path.join(DOWNLOAD_DIR, f"fhv_tripdata_2019-{month}.csv")
    url = f"{BASE_URL}{month}.csv.gz"

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, gz_file_path)
        print(f"Downloaded: {gz_file_path}")

        # Extract .csv.gz to .csv
        with gzip.open(gz_file_path, 'rb') as gz_file, open(csv_file_path, 'wb') as csv_file:
            shutil.copyfileobj(gz_file, csv_file)

        print(f"Extracted: {csv_file_path}")

        # Remove the .gz file to save space
        os.remove(gz_file_path)

        return csv_file_path
    except Exception as e:
        print(f"❌ Failed to process {url}: {e}")
        return None


def verify_gcs_upload(blob_name):
    """Verifies if a file was successfully uploaded to GCS"""
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    """Uploads the CSV file to GCS and verifies the upload"""
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE  

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to GCS (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"✅ Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            # Verify upload
            if verify_gcs_upload(blob_name):
                print(f"✅ Verification successful for {blob_name}")
                return
            else:
                print(f"⚠️ Verification failed for {blob_name}, retrying...")

        except Exception as e:
            print(f"❌ Upload failed for {file_path}: {e}")

        time.sleep(5)  

    print(f"❌ Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    # 🔹 Download & Extract (Multithreading for efficiency)
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_and_extract, MONTHS))

    # 🔹 Upload to GCS (Skipping None values)
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))

    print("✅ All files processed and uploaded successfully.")
