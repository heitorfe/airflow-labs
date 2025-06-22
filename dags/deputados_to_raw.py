"""
DAG 01: Uso de Variáveis e Connections
Demonstra como usar Variáveis do Airflow e Connections com BaseHook
"""

# Imports
from airflow.decorators import dag
from airflow.models import Variable
from airflow import Dataset
import pendulum
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator

# Connections & variables
API_BASE_URL = Variable.get("camara_api_base_url", "https://dadosabertos.camara.leg.br/api/v2")
BUCKET_NAME = "airflow-my-lab"

# Datasets
deputies_dataset = Dataset("s3://camara/raw/deputies/")

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
    description='Carrega dados de deputados da API da Câmara e salva no S3',
    schedule='@monthly',  # Executa mensalmente
    catchup=False,
    tags=['api', 's3', 'deputies', 'full-load'],
)
def deputados_etl():
    
    load_to_s3 = HttpToS3Operator(
        task_id="transfer_deputados_to_s3",
        http_conn_id="http_dadosabertos_camara",
        endpoint="deputados",  # Endpoint relativo
        method="GET",
        aws_conn_id="aws_s3_demo",
        log_response=True,
        replace=True,
        s3_key=f"raw/deputies/deputies_{pendulum.now().strftime('%Y%m%d')}.json",
        s3_bucket=BUCKET_NAME,
    )
    
    # Task dependencies
    load_to_s3  # Adicione o operador ao DAG

# DAG Instantiation
dag_instance = deputados_etl()