import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List
from datetime import datetime
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_sample_data() -> Dict[str, List]:
    """Create sample data with potential issues."""
    return {
        'id': [1, 2, 3, 4, 5, 'seis', 7],
        'nome': ['Mariana', 'Gabriel', 'Carlos', None, 'Ana', 'Francisco', 'Helena'],
        'idade': [26, None, 35, 28, -6, 40, 'unknown'],
        'salario': [50000, 60000, None, 70000, 80000, 90000, 100000]
    }


def save_dataframe(df: pd.DataFrame, output_path: str) -> None:
    """Save dataframe to CSV with error handling."""
    try:
        # Create directory if it doesn't exist
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Save the file
        df.to_csv(output_path, index=False)
        logger.info(f"Data successfully saved to {output_path}")
    except Exception as e:
        logger.error(f"Error saving data: {str(e)}")
        raise

def main():
    try:
        # Create data
        logger.info("Creating sample data...")
        data = create_sample_data()
        df_raw = pd.DataFrame(data)
 
        # Save data
        dir = os.path.dirname(os.path.abspath(__file__))
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(dir, f"arquivos/processed_data_{timestamp}.csv")
        save_dataframe(df_raw, output_path)
        
    except Exception as e:
        logger.error(f"Error in main processing: {str(e)}")
        raise

if __name__ == "__main__":
    main()