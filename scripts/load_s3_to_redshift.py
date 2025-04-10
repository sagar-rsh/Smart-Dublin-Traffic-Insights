import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def copy_file_to_redshift(s3_file, table_name):
    host = os.getenv('REDSHIFT_HOST')
    port = os.getenv('REDSHIFT_PORT', 5439)
    db = os.getenv('REDSHIFT_DB')
    user = os.getenv('REDSHIFT_USER')
    password = os.getenv('REDSHIFT_PASSWORD')
    s3_bucket = os.getenv('S3_BUCKET')
    iam_role = os.getenv('IAM_ROLE_ARN')

    s3_path = f's3://{s3_bucket}/{s3_file}'

    copy_command = f"""
        COPY {table_name}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS CSV
        IGNOREHEADER 1
        DELIMITER ',';
    """

    print(f"[INFO] Copying {s3_file} to Redshift table '{table_name}'")

    conn = psycopg2.connect(
        dbname=db,
        user=user,
        password=password,
        host=host,
        port=port
    )
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute(f'TRUNCATE TABLE {table_name};')
        cur.execute(copy_command)
        print(f"[SUCCESS] {s3_file} loaded into {table_name}")
    except Exception as e:
        print(f"[ERROR] Failed to load {s3_file}: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    file_table_map = {
        "trips_1_day_cleaned.csv": "trips_raw",
        "routes_cleaned.csv": "routes"
    }

    for s3_file, table in file_table_map.items():
        copy_file_to_redshift(s3_file, table)
