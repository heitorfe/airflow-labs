"""
DAG 01: Uso de Variáveis e Connections
Demonstra como usar Variáveis do Airflow e Connections com BaseHook
Fluxo: HTTP to S3 > Extract Details > Save Details in S3
"""

# Imports
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow import Dataset
import pendulum
import json
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook

import logging

logger = logging.getLogger(__name__)

# Connections & variables
API_BASE_URL = Variable.get("camara_api_base_url", "https://dadosabertos.camara.leg.br/api/v2")
BUCKET_NAME = "airflow-my-lab"

# Datasets
deputies_dataset = Dataset("s3://camara/raw/deputies/")
deputies_details_dataset = Dataset("s3://camara/raw/deputies_details/")

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# DAG definition
@dag(
    dag_id='01_deputados_para_s3_blob',
    default_args=default_args,
    description='Carrega dados de deputados da API da Câmara e salva no S3 com detalhes',
    schedule='@monthly',  # Executa mensalmente
    catchup=False,
    tags=['api', 's3', 'deputies', 'full-load'],
)
def deputados_etl():
    
    # Task 1: Load basic deputies data to S3
    load_deputies_to_s3 = HttpToS3Operator(
        task_id="transfer_deputados_to_s3",
        http_conn_id="http_dadosabertos_camara",
        endpoint="deputados",
        method="GET",
        aws_conn_id="aws_s3_demo",
        log_response=True,
        replace=True,
        s3_key=f"raw/deputies/deputies_{pendulum.now().strftime('%Y%m%d')}.json",
        s3_bucket=BUCKET_NAME,
        outlets=[deputies_dataset]
    )
    
    # Task 2: Extract details from API
    @task
    def extract_deputy_details():
        """Extract detailed information for each deputy from the API"""

        
        # Get S3 hook to read the deputies file
        s3_hook = S3Hook(aws_conn_id="aws_s3_demo")
        http_hook = HttpHook(http_conn_id="http_dadosabertos_camara", method="GET")
        
        # Read deputies data from S3
        s3_key = f"raw/deputies/deputies_{pendulum.now().strftime('%Y%m%d')}.json"
        deputies_data = s3_hook.read_key(key=s3_key, bucket_name=BUCKET_NAME)
        deputies_json = json.loads(deputies_data)
        
        details_list = []
        failures = 0
        max_failures = 5
        
        # Extract details for each deputy
        for deputy in deputies_json.get('dados', []):
            deputy_id = deputy.get('id')
            if deputy_id:
                try:
                    # Get detailed information
                    detail_response = http_hook.run(
                        endpoint=f"deputados/{deputy_id}",
                        headers={"Accept": "application/json"}
                    )
                    
                    if detail_response.status_code == 200:
                        detail_data = detail_response.json()
                        details_list.append(detail_data)
                        logger.info(f"Successfully fetched details for deputy {deputy_id}")
                    else:
                        failures += 1
                        logger.warning(f"Failed to fetch details for deputy {deputy_id}: HTTP {detail_response.status_code}")
                        
                        if failures >= max_failures:
                            raise Exception(f"DAG failed: {failures} deputies failed to fetch details (limit: {max_failures})")
                    
                except Exception as e:
                    if "DAG failed" in str(e):
                        raise e  # Re-raise DAG failure exception
                    
                    failures += 1
                    logger.error(f"Error fetching details for deputy {deputy_id}: {e}")
                    
                    if failures >= max_failures:
                        raise Exception(f"DAG failed: {failures} deputies failed to fetch details (limit: {max_failures})")
                    
                    continue
        
        logger.info(f"Successfully fetched details for {len(details_list)} deputies with {failures} failures")
        return details_list
    
    # Task 3: Save details to S3
    @task
    def save_details_to_s3(details_data, **context):
        """Save detailed deputy information to S3"""
        
        s3_hook = S3Hook(aws_conn_id="aws_s3_demo")
        
        # Convert details to JSON
        details_json = json.dumps(details_data, ensure_ascii=False, indent=2)
        
        # Save to S3
        s3_key = f"raw/deputies_details/deputies_details_{pendulum.now().strftime('%Y%m%d')}.json"
        
        s3_hook.load_string(
            string_data=details_json,
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        
        return s3_key
    
    # Task dependencies
    details = extract_deputy_details()
    saved_details = save_details_to_s3(details)
    
    load_deputies_to_s3 >> details >> saved_details

# DAG Instantiation
dag_instance = deputados_etl()