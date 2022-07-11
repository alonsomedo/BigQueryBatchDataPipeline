from google.cloud import storage
import json

# SETTING GLOBAL CONFIGURATIONS
f = open("./user_configs.json")
SERVICE_ACCOUNT_JSON = "./protean-tome-355920-301854203aac.json"
USER_CONFIGS = json.load(f)

# LOCAL PATH
CUSTOMER_PATH = USER_CONFIGS["CUSTOMER_PATH"]
LOANAPPLICATIONS_PATH = USER_CONFIGS["LOANAPPLICATIONS_PATH"]

# DESTINATION PATH
CUSTOMER_DESTINATION_PATH = USER_CONFIGS["CUSTOMER_DESTINATION_PATH"]
LOANAPPLICATIONS_DESTINATION_PATH = USER_CONFIGS["LOANAPPLICATIONS_DESTINATION_PATH"]

# GCP CLIENTS
storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)


# CREATING THE BUCKET IN GCP
def create_bucket_class_location(bucket_name):
    """
    Create a new bucket in the US region with the STANDARD storage
    class
    """
    try:
        bucket_name = bucket_name 

        bucket = storage_client.bucket(bucket_name)
        bucket.storage_class = "STANDARD"
        new_bucket = storage_client.create_bucket(bucket, location="us")

        print(
            "Created bucket {} in {} with storage class {}".format(
                new_bucket.name, new_bucket.location, new_bucket.storage_class
            )
        )
        return new_bucket
    except:
        print("Bucket {} already exists".format(bucket_name))


# CREATE FOLDER IN BUCKET AND UPLOAD FILES
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )



if __name__ == '__main__':
    print("Starting process")
    create_bucket_class_location(USER_CONFIGS["BUCKET_NAME"])
    create_bucket_class_location(USER_CONFIGS["TMP_BUCKET_NAME"])
    upload_blob(USER_CONFIGS["BUCKET_NAME"], LOANAPPLICATIONS_PATH, LOANAPPLICATIONS_DESTINATION_PATH)