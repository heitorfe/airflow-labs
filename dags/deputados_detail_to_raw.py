# Imports
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow import Dataset
import pendulum
import boto3
import requests
import json
from typing import List, Dict
import logging

# Variáveis
API_BASE_URL = Variable.get("camara_api_base_url", "https://dadosabertos.camara.leg.br/api/v2")
INPUT_DATASET = Dataset("s3://camara/raw/deputies/")

# Default args
default_args = {
    'owner': 'data-team',
    'start_date': pendulum.datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
}

@dag(
    dag_id="02_detalhes_deputados",
    default_args=default_args,
    schedule=[INPUT_DATASET],
    catchup=False,
    tags=["api", "s3", "deputies", "details"],
    description="Obtém detalhes dos deputados da Câmara a partir de dataset raw",
)
def detalhes_deputados_etl():
    
    @task
    def get_deputy_ids_from_s3() -> List[int]:
        """
        Lê o último arquivo de deputados no S3 e extrai os IDs
        """
        aws_conn = BaseHook.get_connection("aws_s3_demo")
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name=aws_conn.extra_dejson.get("region_name", "us-east-1")
        )
        
        # OBS: este exemplo lê o último arquivo com prefixo, sem usar paginator
        bucket_name = "airflow-my-lab"
        prefix = "raw/deputies/"
        
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        objects = response.get("Contents", [])
        json_files = [obj['Key'] for obj in objects if "deputies_" in obj['Key']]
        
        if not json_files:
            raise ValueError("Nenhum arquivo encontrado no S3.")
        
        # Pega o mais recente por ordenação (assumindo nome com timestamp)
        latest_file_key = sorted(json_files)[-1]
        response = s3_client.get_object(Bucket=bucket_name, Key=latest_file_key)
        data = json.loads(response['Body'].read().decode("utf-8"))
        
        deputy_ids = [d["id"] for d in data if "id" in d]
        print(f"{len(deputy_ids)} IDs lidos.")
        return deputy_ids
    
    @task
    def fetch_details(ids: List[int]) -> List[Dict]:
        """
        Faz requests dos detalhes dos deputados
        """
        detalhes = []
        for _id in ids:
            url = f"{API_BASE_URL}/deputados/{_id}"
            try:
                resp = requests.get(url)
                resp.raise_for_status()
                detalhes.append(resp.json().get("dados", {}))
            except Exception as e:
                logging.warning(f"Erro ao buscar detalhes de ID {_id}: {str(e)}")
        return detalhes
    
    @task
    def save_details_to_s3(deputy_details: List[Dict]) -> str:
        """
        Salva os detalhes no S3 com timestamp
        """
        aws_conn = BaseHook.get_connection("aws_s3_demo")
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name=aws_conn.extra_dejson.get("region_name", "us-east-1")
        )

        bucket_name = "airflow-my-lab"
        file_key = f"raw/deputies/deputies_details_{pendulum.now().format('YYYYMMDD')}.json"
        json_data = json.dumps(deputy_details, ensure_ascii=False, indent=2)

        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=json_data.encode('utf-8'),
            ContentType='application/json'
        )
        s3_path = f"s3://{bucket_name}/{file_key}"
        print(f"Detalhes salvos em: {s3_path}")
        return s3_path
    
    ids = get_deputy_ids_from_s3()
    detalhes = fetch_details(ids)
    save_details_to_s3(detalhes)

dag_instance = detalhes_deputados_etl()
