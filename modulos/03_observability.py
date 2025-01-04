import logging
import json
import pandas as pd
import os
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_latest_file(folder: str) -> str:
    """Get the latest modified CSV file in the specified directory."""
    files = [os.path.join(folder, f) for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f)) and f.endswith('.csv')]
    if not files:
        raise FileNotFoundError(f"No CSV files found in the folder: {folder}")
    latest_file = max(files, key=os.path.getmtime)
    return os.path.basename(latest_file)

def validate_dataframe(df):
    """Validate if DataFrame is not empty and has expected structure"""
    if df is None or df.empty:
        raise ValueError("DataFrame cannot be empty")
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")
    return True

def calculate_observability_metrics(df):
    """Calculate observability metrics with validation"""
    validate_dataframe(df)
    
    try:
        # Identify numeric and categorical columns
        variaveis_quantitativas = df.select_dtypes(include=['int64', 'float64']).columns
        variaveis_categoricas = df.select_dtypes(include=['object', 'category']).columns

        estatisticas_quantitativas = {
            coluna: df[coluna].describe().map(lambda x: float(x) if pd.notnull(x) else None).to_dict()
            for coluna in variaveis_quantitativas
        }

        estatisticas_categoricas = {
            coluna: {
                'total': int(df[coluna].count()),
                'valores_unicos': int(df[coluna].nunique()),
                'valor_mais_frequente': df[coluna].mode()[0] if not df[coluna].mode().empty else None,
                'categorias_unicas': int(df[coluna].nunique()),
                'frequencia_do_valor_mais_frequente': int(df[coluna].value_counts().iloc[0]) if not df[coluna].value_counts().empty else None
            }
            for coluna in variaveis_categoricas
        }

        observabilidade = {
            'total_linhas': int(len(df)),
            'colunas': df.columns.tolist(),
            'colunas_nulas': df.isnull().sum().astype(int).to_dict(),
            'tipos_dados': df.dtypes.astype(str).to_dict(),
            'estatisticas_quantitativas': estatisticas_quantitativas,
            'estatisticas_categoricas': estatisticas_categoricas
        }

        return observabilidade
    
    except Exception as e:
        logger.error(f"Error calculating observability metrics: {str(e)}")
        raise

def save_observability_metrics(metrics, file_path, bucket_name):
    """Save metrics locally and to S3 with error handling"""
    
    # Create output directory if it doesn't exist
    output_dir = 'arquivos'
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract base filename without extension
    base_filename = os.path.splitext(os.path.basename(file_path))[0]
    
    # Construct output paths
    local_path = os.path.join(output_dir, f'observabilidade_{base_filename}.json')
    s3_path = f'observabilidade/{base_filename}.json'
    
    # Save locally
    try:
        with open(local_path, 'w') as f:
            json.dump(metrics, f, indent=4)
        logger.info(f"Observability metrics saved locally to {local_path}")
    except IOError as e:
        logger.error(f"Error saving local file: {str(e)}")
        raise

    # Upload to S3
    try:
        s3 = boto3.client('s3')
        s3.upload_file(local_path, bucket_name, s3_path)
        logger.info(f"Observability metrics uploaded to S3 as {s3_path}")
    except ClientError as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        # Configuration
        bucket_name = 'data-lake-p6-890447484968'
        region = 'us-east-2'
        folder = 'arquivos'
        
        # Create folder if it doesn't exist
        os.makedirs(folder, exist_ok=True)
               
        try:
            dir = os.path.dirname(os.path.abspath(__file__))
            full_path = os.path.join(dir, folder)
            last_file = get_latest_file(full_path)
            file_path = os.path.join(full_path, last_file)
            
            df = pd.read_csv(file_path)
            metrics = calculate_observability_metrics(df)
            print(metrics)
            save_observability_metrics(metrics, file_path, bucket_name)
            
        except FileNotFoundError as e:
            logger.error(f"File not found: {str(e)}")
            raise
            
    except Exception as e:
        logger.error(f"Failed to process observability metrics: {str(e)}")
        raise