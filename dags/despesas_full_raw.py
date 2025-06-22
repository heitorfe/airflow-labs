"""
DAG 03: Carga Full das Despesas dos Deputados
Executa carga completa mensal das despesas
"""

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow import Dataset
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
import pendulum
import boto3
import requests
import json
from typing import List, Dict, Any
import logging

# Variáveis
API_BASE_URL = Variable.get("camara_api_base_url", "https://dadosabertos.camara.leg.br/api/v2")
BUKET_NAME = "airflow-my-lab"

# Datasets
expenses_dataset_full = Dataset("s3://camara/raw/expenses/full/")
deputy_ids_dataset = Dataset("s3://camara/raw/deputies/")

default_args = {
    'owner': 'data-team',
    'start_date': pendulum.datetime(2025, 6, 22),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

@dag(
    dag_id='03_despesas_deputados_full',
    default_args=default_args,
    description='Carga full mensal das despesas dos deputados',
    schedule='0 2 1 * *',  # Todo dia 1 às 02:00
    catchup=False,
    tags=['api', 's3', 'expenses', 'full-load'],
    params={
        'ano': pendulum.now().year,
        'mes': pendulum.now().month
    }
)
def despesas_full_etl():
    
    @task
    def get_deputy_list() -> List[Dict]:
        """Obtém lista atual de deputados"""
        http_hook = HttpHook(
            http_conn_id='http_dadosabertos_camara',
            method='GET')
        
        endpoint = "/deputados"

        response = http_hook.run(
            endpoint=endpoint,
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code != 200:
            raise Exception(f"Erro ao buscar lista de deputados: {response.text}")
        
        data = response.json()
        deputies = data.get('dados', [])
        logging.info(f"Total de deputados encontrados: {len(deputies)}")

        return [deputy['id'] for deputy in deputies]

    @task
    def extract_expenses_batch(deputies: List[str], **context) -> Dict[str, Any]:
        """Extrai despesas em lotes para otimizar performance"""
        params = context['params']
        ano = params.get('ano', pendulum.now().year)
        mes = params.get('mes', pendulum.now().month)
        
        all_expenses = []
        failed_deputies = []
        
        logging.info(f"Processando despesas para {ano}/{mes:02d}")
        
        for i, deputy_id in enumerate(deputies):
            try:
                url = f"{API_BASE_URL}/deputados/{deputy_id}/despesas"
                params = {
                    'ano': ano,
                    'mes': mes,
                    'itens': 100  # Máximo por página
                }
                
                page = 1
                deputy_expenses = []
                
                while True:
                    params['pagina'] = page
                    response = requests.get(url, params=params, timeout=30)
                    response.raise_for_status()
                    
                    data = response.json()
                    expenses = data.get('dados', [])
                    
                    if not expenses:
                        break
                    
                    # Adiciona metadados
                    for expense in expenses:
                        expense['deputy_id'] = deputy_id
                        expense['extracted_at'] = pendulum.now('America/Sao_Paulo').isoformat()
                    
                    deputy_expenses.extend(expenses)
                    
                    # Verificar se há mais páginas
                    links = data.get('links', [])
                    if not any(link.get('rel') == 'next' for link in links):
                        break
                    
                    page += 1
                    
                all_expenses.extend(deputy_expenses)
                
                if (i + 1) % 50 == 0:  # Log a cada 50 deputados
                    logging.info(f"Processados {i + 1}/{len(deputies)} deputados")
                    
            except Exception as e:
                logging.error(f"Erro ao processar deputado {deputy_id}: {str(e)}")
                failed_deputies.append({'id': deputy_id, 'error': str(e)})
                continue

            finally:
                if len(failed_deputies) >= 10:  # Limite de 10.000 despesas por execução
                    raise Exception("Too much errors occurred during the extraction process. ")
        
        result = {
            'expenses': all_expenses,
            'total_expenses': len(all_expenses),
            'failed_deputies': failed_deputies,
            'total_deputies_processed': len(deputies),
            'ano': ano,
            'mes': mes,
            'extraction_timestamp': pendulum.now().isoformat()
        }
        
        logging.info(f"Extração concluída: {len(all_expenses)} despesas de {len(deputies)} deputados")
        return result

    @task(outlets=[expenses_dataset_full])
    def load_expenses_to_s3(extraction_result: Dict[str, Any]) -> str:
        """Salva despesas no S3 com particionamento por ano/mês"""
        s3_hook = S3Hook(aws_conn_id='aws_s3_demo')
        
        bucket_name = 'airflow-my-lab'
        ano = extraction_result['ano']
        mes = extraction_result['mes']
        # timestamp = pendulum.now().strftime('%Y%m%d')
        
        # Particionamento por ano/mês
        file_key = f"raw/expenses/full/year={ano}/month={mes:02d}/expenses.json"
        
        # Dados para salvar
        data_to_save = {
            'expenses': extraction_result['expenses'],
            'failed_deputies': extraction_result['failed_deputies']
        }
        
        json_data = json.dumps(data_to_save, ensure_ascii=False, indent=2)
        
        s3_hook.load_string(
            string_data=json_data,
            key=file_key,
            bucket_name=bucket_name,
            replace=True
        )

        
        s3_path = f"s3://{bucket_name}/{file_key}"
        logging.info(f"Despesas salvas em: {s3_path}")
        return s3_path


    # Fluxo da DAG
    deputies = get_deputy_list()
    extraction_result = extract_expenses_batch(deputies)
    s3_path = load_expenses_to_s3(extraction_result)

dag_instance = despesas_full_etl()
