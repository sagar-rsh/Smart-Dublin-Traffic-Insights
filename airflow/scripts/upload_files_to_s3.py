import pandas as pd
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
import os

AWS_REGION = 'eu-west-2'
BUCKET_NAME = 'dublin-trips-data-lake'
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
LOCAL_RAW_DIR = os.path.join(AIRFLOW_HOME, "data", "raw")
LOCAL_CLEAN_DIR = os.path.join(AIRFLOW_HOME, "data", "clean")
# LOCAL_RAW_DIR = '../data/raw'
# LOCAL_CLEAN_DIR = '../data/clean'

s3_client = boto3.client('s3', region_name = AWS_REGION)

def clean_csv(file_path):
    """Clean the CSV by validating rows and stripping whitespace."""
    try:
        os.makedirs(LOCAL_CLEAN_DIR, exist_ok=True)
        cleaned_lines = []
        with open(file_path, 'r') as f:
            lines = f.readlines()

        # Clean header
        header = lines[0].strip().split(',')
        header = [col.strip() for col in header]
        cleaned_lines.append(','.join(header))

        # Process data lines
        for line in lines[1:]:
            fields = [field.strip() for field in line.strip().split(',')]
            if len(fields) == len(header):
                cleaned_lines.append(','.join(fields))
            else:
                # print bad lines
                print(f"Skipping malformed line: {line.strip()}")

        # Save cleaned file
        cleaned_filename = os.path.basename(file_path).replace(".csv", "_cleaned.csv")
        cleaned_path = os.path.join(LOCAL_CLEAN_DIR, cleaned_filename)
        with open(cleaned_path, 'w') as f:
            for row in cleaned_lines:
                f.write(row + '\n')

        return cleaned_path

    except Exception as e:
        print(f"Failed to clean {file_path}: {e}")
        return None


def upload_files_to_s3(local_data_dir, bucket_name):
    """Upload files in a directory to an S3 bucket

    :param local_data_dir: Local directory path of files to upload
    :param bucket_name: Bucket to upload to
    :return: None
    """

    # Loop through files and directory
    try:
        for (root, dirs, files) in os.walk(local_data_dir):
            for file in files:
                local_file_path = os.path.join(root, file)

                cleaned_path = clean_csv(local_file_path)
                if cleaned_path:
                    filename = os.path.basename(cleaned_path)
                    # Upload each file to S3
                    s3_client.upload_file(cleaned_path, bucket_name, filename)
                    print(f'Uploaded {filename} to S3 bucket {bucket_name}')
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

def main():
    upload_files_to_s3(LOCAL_RAW_DIR, BUCKET_NAME)

if __name__ == "__main__":
    main()