import os, sys, requests, time
from zipfile import ZipFile
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging, format_time

def download_zip_file(url, output_dir, logger):
    response = requests.get(url, stream=True)
    os.makedirs(output_dir, exist_ok=True)
    
    if response.status_code == 200:
        filename = os.path.join(output_dir, "downloaded.zip")
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logger.info(f"Downloaded zip file: {filename}")
        return filename
    else:
        logger.error(f"Failed to download file. Status code: {response.status_code}")
        raise Exception(f"Failed to download file. Status code: {response.status_code}")
    
def extract_zip_file(zip_filename, output_dir, logger):
    with ZipFile(zip_filename, 'r') as zip_file:
        zip_file.extractall(output_dir)
    logger.info(f"Extracted zip file to: {output_dir}")
    logger.info("Removing the zip file")
    os.remove(zip_filename)

def fix_json_dict(output_dir, logger):
    import json
    file_path = os.path.join(output_dir, "dict_artists.json")
    
    with open(file_path, "r") as f:
        data = json.load(f)

    fixed_path = os.path.join(output_dir, "fixed_da.json")
    with open(fixed_path, "w", encoding="utf-8") as f_out:
        for key, value in data.items():
            record = {"id": key, "related_ids": value}
            json.dump(record, f_out, ensure_ascii=False)
            f_out.write("\n")

    logger.info(f"File {file_path} has been fixed and written to {fixed_path}")
    logger.info("Removing the original file")
    os.remove(file_path)

if __name__ == "__main__":

    logger = setup_logging("extract.log")
    if len(sys.argv) < 2:
        logger.debug("Extraction path is required")
        logger.debug("Example usage:")
        logger.debug("python3 execute.py /Users/najibthapa1/Data/Extraction")
    else:
        try:
            logger.debug("Starting Extraction Engine...")
            start = time.time()
            EXTRACT_PATH = sys.argv[1]
            # KAGGLE_URL = "https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks?resource=download"
            # zip_file = download_zip_file(KAGGLE_URL, EXTRACT_PATH, logger)
            zip_file = os.path.join(EXTRACT_PATH, "archive.zip")
            extract_zip_file(zip_file, EXTRACT_PATH, logger)
            fix_json_dict(EXTRACT_PATH, logger)
            end = time.time()
            logger.info("Extraction Successfully Completed!")

        except Exception as e:
            logger.error(f"Error: {e}")