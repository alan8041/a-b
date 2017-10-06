import os
import time

#https://cloud.google.com/storage/docs/object-basics
from google.cloud import storage

BUCKETNAME = "unileverecommseaa-warehouse-data-sources-backups_dev"

# private methods
def __print_duration(start_time, end_time):
    """Prints duration of a job.
    Params:
        start_time (time): Start time of the job.
        end_time (time): End time of the job.
    Returns:
        nothing
    """
    total_seconds = end_time - start_time
    total_minutes = float(total_seconds) / 60
    print("Time taken: {:.2f} minutes.".format(total_minutes))
    return

# end private methods

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """ Uploads a file to the bucket.
        Params:
            bucket_name (str): The destination bucket
            source_file_name: the absolute path and the source file name
            destination_blob_name: folder and destination file name
        Returns:
            nothing
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    start_time = time.time()

    blob.upload_from_filename(source_file_name)

    end_time = time.time()

    __print_duration(start_time, end_time)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))

def delete_blob(bucket_name, blob_name):
    """ Deletes a blob from the bucket.
        Params:
            bucket_name (str): The target bucket
            blob_name: folder and target file name
        Returns:
            nothing
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.delete()

    print('Blob {} deleted.'.format(blob_name))

def copy_blob(bucket_name, blob_name, new_bucket_name, new_blob_name):
    """Copies a blob from one bucket to another with a new name."""
    storage_client = storage.Client()
    source_bucket = storage_client.get_bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.get_bucket(new_bucket_name)

    new_blob = source_bucket.copy_blob(
        source_blob, destination_bucket, new_blob_name)

    print('Blob {} in bucket {} copied to blob {} in bucket {}.'.format(
        source_blob.name, source_bucket.name, new_blob.name,
        destination_bucket.name))

def rename_blob(bucket_name, blob_name, new_name):
    """Renames a blob."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    new_blob = bucket.rename_blob(blob, new_name)

    print('Blob {} has been renamed to {}'.format(
        blob.name, new_blob.name))

#
#path = os.path.abspath("Brand_Partnership_Report_Unilever-SEA_2017-04-02.xlsx")
#sourceFileName = path
#destinationBlobName = "b/Copy of Brand_Partnership_Report_Unilever-SEA_2017-04-02.xlsx"
#upload_blob(BUCKETNAME,sourceFileName, destinationBlobName)
