"""
DAG 01: Uso de Variables e Connections
Demonstra como usar Variables do Airflow e Connections com BaseHook
"""

# Imports
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable, Connection
from airflow import Dataset
import pendulum
import requests
import json
import boto3
from typing import Dict, Any
import logging

# Connections & variables
API_BASE_URL = Variable.get("camara_api_base_url", "https://dadosabertos.camara.leg.br/api/v2")

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
    
    @task
    def extract_deputies_data() -> Dict[str, Any]:
        """
        Extrai dados dos deputados da API da Câmara
        """
        url = f"{API_BASE_URL}/deputados"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            print(f"Extraídos {len(data.get('dados', []))} deputados")
            return data
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Erro ao buscar dados da API: {str(e)}")
    
    @task
    def transform_deputies_data(raw_data: Dict[str, Any]) -> list:
        """
        Transforma e limpa os dados dos deputados
        """
        deputies = raw_data.get('dados', [])
        
        transformed_deputies = []
        for deputy in deputies:
            transformed_deputy = {
                'id': deputy.get('id'),
                'nome': deputy.get('nome'),
                'partido': deputy.get('siglaPartido'),
                'uf': deputy.get('siglaUf'),
                'email': deputy.get('email'),
                'uri': deputy.get('uri'),
                'uriPartido': deputy.get('uriPartido'),
                'urlFoto': deputy.get('urlFoto'),
                'extracted_at': pendulum.now().isoformat()
            }
            transformed_deputies.append(transformed_deputy)
        
        print(f"Transformados {len(transformed_deputies)} registros")
        return transformed_deputies

    @task(outlets=[deputies_dataset])
    def load_to_s3(deputies_data: list) -> str:
        """
        Carrega os dados transformados para o S3
        """
        # Obtém credenciais da conexão AWS
        aws_conn = BaseHook.get_connection('aws_s3_demo')
        
        # Configura cliente S3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name=aws_conn.extra_dejson.get('region_name', 'us-east-1')
        )
        
        # Define bucket e chave
        bucket_name = 'airflow-my-lab'
        file_key = f"raw/deputies/deputies_{pendulum.now().strftime('%Y%m%d')}.json"
        
        try:
            # Converte dados para JSON
            json_data = json.dumps(deputies_data, ensure_ascii=False, indent=2)
            
            # Upload para S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=file_key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json'
            )
            
            s3_path = f"s3://{bucket_name}/{file_key}"
            print(f"Dados salvos em: {s3_path}")
            return s3_path
            
        except Exception as e:
            raise Exception(f"Erro ao carregar dados para o S3: {str(e)}")
        
    
    
    # Task dependencies
    raw_data = extract_deputies_data()
    transformed_data = transform_deputies_data(raw_data)
    s3_path = load_to_s3(transformed_data)

# DAG Instantiation
dag_instance = deputados_etl()