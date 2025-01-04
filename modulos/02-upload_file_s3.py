import boto3
import logging
from botocore.exceptions import ClientError
import os

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

raw_bucket = 'raw-data'
object_name = os.path.join(raw_bucket, last_file)
s3 = boto3.client('s3')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Função para verificar se o bucket existe
def check_create_bucket(bucket_name: str, region: str):
    """
    Verify if an S3 bucket exists and create it if it doesn't.
    
    Args:
        bucket_name (str): Name of the bucket to verify/create
        region (str): AWS region where the bucket should be created
        
    Returns:
        Optional[bool]: True if operation successful, None if validation fails
        
    Raises:
        ClientError: If there's an error interacting with AWS
    """
    try:
        # Check if bucket exists
        s3.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' already exists.")
        return True
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
                try:
                    # Create bucket if it doesn't exist
                    logger.info(f"Bucket '{bucket_name}' not found. Creating...")
                    s3.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                    logger.info(f"Bucket '{bucket_name}' created successfully.")
                    return True
                except ClientError as create_error:
                    logger.error(f"Failed to create bucket: {str(create_error)}")
                    raise
        else:
            logger.error(f"Error accessing bucket: {str(e)}")
            raise


if __name__ == "__main__":
    try:
        result = check_create_bucket(bucket_name, region)
        if result is None:
            logger.error("Failed to verify/create bucket due to validation errors")
        
        s3.upload_file(file_path, bucket_name, object_name)
        logger.info(f"File '{last_file}' send to '{bucket_name}/{object_name}' successfully.")
    except Exception as e:
        logger.error(f"Operation failed: {str(e)}")