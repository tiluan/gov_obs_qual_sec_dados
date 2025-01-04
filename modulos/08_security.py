import logging
import os
import re
import pandas as pd
import boto3

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_latest_file(folder: str) -> str:
    """Gets the latest CSV file in the specified directory."""
    try:
        files = [f for f in os.listdir(folder) if f.endswith('.csv') and os.path.isfile(os.path.join(folder, f))]
        if not files:
            raise FileNotFoundError(f"No CSV files found in the folder: {folder}")
        return max(files, key=lambda f: os.path.getmtime(os.path.join(folder, f)))
    except OSError as e:
        logger.error(f"Error accessing the directory: {str(e)}")
        raise

def load_and_prepare_data(file_path: str) -> pd.DataFrame:
    """Loads and prepares data from the CSV file."""
    try:
        """Loads and prepares data from the CSV file."""
        df = pd.read_csv(file_path)
        
        # Mascarar dados sensíveis (por exemplo, nome)
        df['nome_mascarado'] = df['nome'].apply(lambda x: x[0] + '*' * (len(x) - 1) if isinstance(x, str) else '')

        # Remover a coluna original
        df = df.drop('nome', axis=1)
    except Exception as e:
        logger.error(f"Error loading and preparing data: {str(e)}")
        raise
    
    return df

def save_to_csv(df: pd.DataFrame, file_path: str) -> None:
    """Saves the DataFrame to a CSV file."""
    df.to_csv(file_path, index=False)
    logger.info(f"Data masking saved to: {file_path}")

def upload_to_s3(file_path: str, bucket_name: str, s3_key: str) -> None:
    """Uploads the file to S3."""
    s3 = boto3.client('s3')
    s3.upload_file(file_path, bucket_name, s3_key)
    logger.info(f"File uploaded to S3: s3://{bucket_name}/{s3_key}")

def main():
    try:
        # Configuração
        bucket_name = 'data-lake-p6-890447484968'
        folder = 'arquivos'
        dir_path = os.path.dirname(os.path.abspath(__file__))
        full_path = os.path.join(dir_path, folder)
        
        # Obter o arquivo mais recente
        last_file = get_latest_file(full_path)
        file_path = os.path.join(full_path, last_file)
        
        df = load_and_prepare_data(file_path)
        
        # Extract base filename without extension
        base_filename = os.path.splitext(os.path.basename(file_path))[0]
        end_name = re.findall(r'[0-9_]+', base_filename)
        
        clean_data_path = os.path.join(full_path, f"final_data{end_name[1]}.csv")
        
        save_to_csv(df, clean_data_path)
        
        s3_key = f'governed-data/final_data{end_name[1]}.csv'
        upload_to_s3(clean_data_path, bucket_name, s3_key)
        
        logger.info("Data masking completed and file uploaded to the Data Lake.")
    
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")

if __name__ == "__main__":
    main()