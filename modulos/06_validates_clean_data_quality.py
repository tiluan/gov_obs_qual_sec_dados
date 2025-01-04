import logging
import os
import re
import pandas as pd
import great_expectations as ge
from datetime import datetime
from typing import Dict, Any

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
    """Loads and prepares data from the CSV."""
    df = pd.read_csv(file_path)
    df['idade'] = pd.to_numeric(df['idade'], errors='coerce')
    df['salario'] = pd.to_numeric(df['salario'], errors='coerce')
    return df

def validate_data(df: pd.DataFrame) -> Dict[str, Any]:
    """Validates the data using Great Expectations."""
    print(df)
    gdf = ge.from_pandas(df)
    
    # Create an expectation suite
    suite = ge.core.ExpectationSuite(expectation_suite_name="my_suite")
    
    # Add expectations to the suite
    suite.add_expectation(ge.core.ExpectationConfiguration(expectation_type="expect_column_to_exist", kwargs={"column": "id"}))
    suite.add_expectation(ge.core.ExpectationConfiguration(expectation_type="expect_column_values_to_be_of_type", kwargs={"column": "id", "type_": "int"}))
    suite.add_expectation(ge.core.ExpectationConfiguration(expectation_type="expect_column_values_to_not_be_null", kwargs={"column": "id"}))
    suite.add_expectation(ge.core.ExpectationConfiguration(expectation_type="expect_column_to_exist", kwargs={"column": "idade"}))
    suite.add_expectation(ge.core.ExpectationConfiguration(expectation_type="expect_column_values_to_be_between", kwargs={"column": "idade", "min_value": 0, "max_value": 120}))
    suite.add_expectation(ge.core.ExpectationConfiguration(expectation_type="expect_column_to_exist", kwargs={"column": "salario"}))
    suite.add_expectation(ge.core.ExpectationConfiguration(expectation_type="expect_column_values_to_be_between", kwargs={"column": "salario", "min_value": 0, "max_value": None}))
    suite.add_expectation(ge.core.ExpectationConfiguration(expectation_type="expect_column_to_exist", kwargs={"column": "nome"}))
    suite.add_expectation(ge.core.ExpectationConfiguration(expectation_type="expect_column_values_to_not_be_null", kwargs={"column": "nome"}))
    
    # Validate the data using the expectation suite
    results = gdf.validate(expectation_suite=suite, result_format="SUMMARY")
    
    
    return results

def generate_html_report(results: Dict[str, Any], output_path: str):
    """Generate an enhanced corporate layout HTML report."""
    html_content = f"""
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Relatório de Validação de Qualidade</title>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
                padding: 0;
                background-color: #f0f2f5;
                color: #333;
                line-height: 1.6;
            }}
            .container {{
                max-width: 900px;
                margin: 40px auto;
                background: #fff;
                padding: 30px;
                border-radius: 10px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }}
            h1, h2, h3 {{
                color: #2c3e50;
                margin-top: 0;
            }}
            h1 {{
                border-bottom: 2px solid #3498db;
                padding-bottom: 10px;
                margin-bottom: 20px;
            }}
            .status {{
                font-size: 1.2em;
                padding: 10px;
                border-radius: 5px;
                margin-bottom: 20px;
            }}
            .success {{
                background-color: #d4edda;
                color: #155724;
                border: 1px solid #c3e6cb;
            }}
            .failure {{
                background-color: #f8d7da;
                color: #721c24;
                border: 1px solid #f5c6cb;
            }}
            .validation-item {{
                background-color: #e9ecef;
                border-left: 4px solid #3498db;
                padding: 15px;
                margin-bottom: 15px;
                border-radius: 0 5px 5px 0;
            }}
            .validation-item h3 {{
                margin-top: 0;
                color: #3498db;
            }}
            ul {{
                list-style-type: none;
                padding-left: 0;
            }}
            li {{
                margin: 5px 0;
            }}
            .footer {{
                text-align: center;
                margin-top: 30px;
                padding-top: 20px;
                border-top: 1px solid #e9ecef;
                font-size: 0.9em;
                color: #7f8c8d;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Relatório de Validação de Qualidade</h1>
            <div class="status {'success' if results['success'] else 'failure'}">
                {'✅ Todas as validações foram concluídas com sucesso!' if results['success'] else '❌ Foram encontrados problemas na validação dos dados.'}
            </div>
            <h2>Detalhes das Validações:</h2>
            {''.join(f'''
            <div class="validation-item">
                <h3>{expectation['expectation_config']['expectation_type']}</h3>
                <ul>
                    <li><strong>Coluna:</strong> {expectation['expectation_config']['kwargs'].get('column', 'N/A')}</li>
                    <li><strong>Sucesso:</strong> {'✅ Sim' if expectation['success'] else '❌ Não'}</li>
                    <li><strong>Observado:</strong> {expectation['result'].get('observed_value', 'N/A')}</li>
                </ul>
            </div>
            ''' for expectation in results['results'])}
            <div class="footer">
                <p>Relatório gerado automaticamente em {datetime.now().strftime('%d/%m/%Y às %H:%M:%S')}</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    logger.info(f"Enhanced corporate HTML report successfully generated: {output_path}")

def main():
    try:
        # Configuration
        folder = 'arquivos'
        dir_path = os.path.dirname(os.path.abspath(__file__))
        full_path = os.path.join(dir_path, folder)
        
        # Get the latest file
        last_file = get_latest_file(full_path)
        file_path = os.path.join(full_path, last_file)
        
        # Load and prepare the data
        df = load_and_prepare_data(file_path)
        
        # Validate the data
        results = validate_data(df)

        # Generate HTML report
        base_filename = os.path.splitext(os.path.basename(file_path))[0]
        end_name = re.findall(r'[0-9_]+', base_filename)
        report_path = os.path.join(full_path, f"clean_data_validation_report{end_name[-1]}.html")
        generate_html_report(results, report_path)
        
        if not results["success"]:
            logger.warning("Issues found in data quality validation. Report generated.")
        else:
            logger.info("No quality issues detected.")
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to process observability metrics: {str(e)}")

if __name__ == "__main__":
    main()
