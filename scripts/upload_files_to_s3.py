import logging
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
import os

AWS_REGION = 'eu-west-2'
BUCKET_NAME = 'dublin-trips-data-lake'
LOCAL_DATA_DIR = '../data/raw'

s3_client = boto3.client('s3', region_name = AWS_REGION)

def upload_files_to_s3(local_data_dir, bucket_name):
    """Upload files in a directory to an S3 bucket

    :param local_data_dir: Local directory path of files to upload
    :param bucket_name: Bucket to upload to
    :return: None
    """

    # Loop through files and directory
    try:
        for (root, dirs, files) in os.walk(LOCAL_DATA_DIR):
            for file in files:
                local_file_path = os.path.join(root, file)
				# Upload each file to S3
                s3_client.upload_file(local_file_path, bucket_name, file)
                print(f'Uploaded {file} to S3 bucket {bucket_name}')
    except FileNotFoundError as e:
        print(f"Error: The file {file} does not exist in the local directory.")
    except ClientError as e:
        print(e)
    except NoCredentialsError:
        print("Error: AWS credentials not found.")
    except PartialCredentialsError:
        print("Error: Incomplete AWS credentials.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    upload_files_to_s3(LOCAL_DATA_DIR, BUCKET_NAME)