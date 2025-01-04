import logging
import json
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_latest_file(folder: str) -> str:
    """
    Get the latest modified file in the specified directory.
    
    Args:
        directory (str): The directory to search for files.
        
    Returns:
        str: The path to the latest modified file.
    """
    files = [os.path.join(folder, f) for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo encontrado na pasta: {folder}")
    latest_file = max(files, key=os.path.getmtime)
    return os.path.basename(latest_file)

# Configuration
bucket_name = 'data-lake-p6-890447484968'
region = 'us-east-2'
folder = 'arquivos'
try:
    last_file = get_latest_file(folder)
    file_path = os.path.join(folder, last_file)
except FileNotFoundError as e:
    print(e)

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

def save_observability_metrics(metrics, local_path='arquivos/observabilidade.json', 
                             bucket_name=bucket_name,
                             s3_path='observabilidade/observabilidade_inicial.json'):
    """Save metrics locally and to S3 with error handling"""
    # Save locally
    try:
        with open(local_path, 'w') as f:
            json.dump(metrics, f, indent=4)
        logger.info("Observability metrics saved locally")
    except IOError as e:
        logger.error(f"Error saving local file: {str(e)}")
        raise

    # Upload to S3
    try:
        s3 = boto3.client('s3')
        s3.upload_file(local_path, bucket_name, s3_path)
        logger.info("Observability metrics uploaded to S3 successfully")
    except ClientError as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        raise

# Main execution
try:
    df = pd.read_csv(file_path)
    metrics = calculate_observability_metrics(df)
    save_observability_metrics(metrics, bucket_name)
except Exception as e:
    logger.error(f"Failed to process observability metrics: {str(e)}")
    raise