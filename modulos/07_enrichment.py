import logging
import os
import re
from typing import List

import pandas as pd
import boto3
from botocore.exceptions import BotoCoreError, ClientError

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

def categorize_salary(salary: float) -> str:
    """Categorizes the salary into ranges."""
    if pd.isna(salary) or salary < 0:
        return 'Desconhecido'
    elif salary < 70000:
        return 'Baixa'
    elif 70000 <= salary < 80000:
        return 'Média'
    else:
        return 'Alta'

def enrich_data(df: pd.DataFrame) -> pd.DataFrame:
    """Enriches the DataFrame with salary range."""
    df['faixa_salarial'] = df['salario'].apply(categorize_salary)
    return df

def save_to_csv(df: pd.DataFrame, file_path: str) -> None:
    """Saves the DataFrame to a CSV file."""
    try:
        df.to_csv(file_path, index=False)
        logger.info(f"Data saved to: {file_path}")
    except IOError as e:
        logger.error(f"Error saving CSV file: {str(e)}")
        raise

def upload_to_s3(file_path: str, bucket_name: str, s3_key: str) -> None:
    """Uploads the file to S3."""
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        logger.info(f"File uploaded to S3: s3://{bucket_name}/{s3_key}")
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        raise

def process_data(file_path: str, bucket_name: str) -> pd.DataFrame:
    """Processes the data, enriches it, and uploads to S3."""
    try:
        df = pd.read_csv(file_path)
        df_enriched = enrich_data(df)
        
        logger.info("Data enrichment completed and data sent to the Data Lake.")
        
        return df_enriched
        
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise
        raise

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
        
        # Processar dados
        df = process_data(file_path, bucket_name)
        
        # Extract base filename without extension
        base_filename = os.path.splitext(os.path.basename(file_path))[0]
        end_name = re.findall(r'[0-9_]+', base_filename)
        
        clean_data_path = os.path.join(full_path, f"enriched_data{end_name[1]}.csv")
        
        save_to_csv(df, clean_data_path)
        
        s3_key = f'enriched-data/enriched_data{end_name[1]}.csv'
        upload_to_s3(clean_data_path, bucket_name, s3_key)
        
        logger.info("Data enrichement completed and uploaded to the Data Lake.")
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to process observability metrics: {str(e)}")

if __name__ == "__main__":
    main()