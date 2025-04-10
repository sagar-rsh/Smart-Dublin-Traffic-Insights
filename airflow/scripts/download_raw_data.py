import os
import requests

# Define download directory
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
DOWNLOAD_DIR = os.path.join(AIRFLOW_HOME, "data", "raw")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Files to download
files = {
    "trips_1_day": "https://data.smartdublin.ie/dataset/d083b9a8-bed7-444c-a387-d58318f31c5d/resource/3bf193dc-6029-42e7-987f-31ea5ae3c32f/download/trips-1-day.csv",
    "routes": "https://data.smartdublin.ie/dataset/d083b9a8-bed7-444c-a387-d58318f31c5d/resource/275fa529-22e1-4afa-8106-d82c075068f4/download/routes.csv",
    "junctions": "https://data.smartdublin.ie/dataset/d083b9a8-bed7-444c-a387-d58318f31c5d/resource/80ab75fc-3e81-420e-8987-31140ccc24d8/download/junctions.csv"
}

def download_files():
    # Download and save files
    for name, url in files.items():
        file_path = os.path.join(DOWNLOAD_DIR, f"{name}.csv")
        response = requests.get(url)
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                f.write(response.content)
            print(f"Successfully downloaded and saved: {file_path}")
        else:
            print(f"Failed to download {name} from {url}")

if __name__ == "__main__":
    download_files()