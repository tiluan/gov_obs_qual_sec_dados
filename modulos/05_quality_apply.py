import pandas as pd
import logging
import os
import re
import boto3
import great_expectations as ge

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_latest_file(folder: str) -> str:
    """Gets the most recent CSV file in the specified directory."""
    files = [f for f in os.listdir(folder) if f.endswith('.csv') and os.path.isfile(os.path.join(folder, f))]
    if not files:
        raise FileNotFoundError(f"No CSV files found in the folder: {folder}")
    return max(files, key=lambda f: os.path.getmtime(os.path.join(folder, f)))

def load_and_prepare_data(file_path: str) -> pd.DataFrame:
    """Loads and prepares data from the CSV file."""
    df = pd.read_csv(file_path)
    
    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df = df.dropna(subset=['id'])
    df['id'] = df['id'].astype(int)
    
    df['idade'] = pd.to_numeric(df['idade'], errors='coerce')
    df['idade'] = df['idade'].clip(0, 120)
    df['idade'] = df['idade'].fillna(df['idade'].mean()).round(1)
    
    df['salario'] = pd.to_numeric(df['salario'], errors='coerce')
    df['salario'] = df['salario'].clip(lower=0)
    df['salario'] = df['salario'].fillna(df['salario'].mean())
    
    df['nome'] = df['nome'].fillna('Unknown')
    
    return df

def save_to_csv(df: pd.DataFrame, file_path: str) -> None:
    """Saves the DataFrame to a CSV file."""
    df.to_csv(file_path, index=False)
    logger.info(f"Cleaned data saved to: {file_path}")

def upload_to_s3(file_path: str, bucket_name: str, s3_key: str) -> None:
    """Uploads the file to S3."""
    s3 = boto3.client('s3')
    s3.upload_file(file_path, bucket_name, s3_key)
    logger.info(f"File uploaded to S3: s3://{bucket_name}/{s3_key}")

def validate_data(df: pd.DataFrame) -> None:
    """Validates the data using Great Expectations."""
    context = ge.data_context.DataContext()
    suite = context.get_expectation_suite("my_suite")
    validator = ge.dataset.PandasDataset(df, expectation_suite=suite)
    results = validator.validate()
    if not results["success"]:
        logger.warning("Data validation failed. Check the results for more details.")
    else:
        logger.info("Data validation completed successfully.")

def main():
    try:
        bucket_name = 'data-lake-p6-890447484968'
        folder = 'arquivos'
        dir_path = os.path.dirname(os.path.abspath(__file__))
        full_path = os.path.join(dir_path, folder)
        
        last_file = get_latest_file(full_path)
        file_path = os.path.join(full_path, last_file)
        
        df = load_and_prepare_data(file_path)
        
        # Extract base filename without extension
        base_filename = os.path.splitext(os.path.basename(file_path))[0]
        end_name = re.findall(r'[0-9_]+', base_filename)
        
        clean_data_path = os.path.join(full_path, f"cleaned_data{end_name[1]}.csv")
        
        save_to_csv(df, clean_data_path)
        
        s3_key = 'processed-data/cleaned_data{end_name[1]}.csv'
        upload_to_s3(clean_data_path, bucket_name, s3_key)
        
        validate_data(df)
        
        logger.info("Quality validation completed and cleaned data uploaded to the Data Lake.")
    
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")

if __name__ == "__main__":
    main()