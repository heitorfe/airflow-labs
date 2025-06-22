"""
DAG 03: Carga Full das Despesas dos Deputados
Executa carga completa mensal das despesas
"""

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow import Dataset
import pendulum
import boto3
import requests
import json
from typing import List, Dict, Any
import logging

# Variáveis
API_BASE_URL = Variable.get("camara_api_base_url", "https://dadosabertos.camara.leg.br/api/v2")

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
        aws_conn = BaseHook.get_connection('aws_s3_demo')
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name=aws_conn.extra_dejson.get('region_name', 'us-east-1')
        )
        
        # Pega o arquivo mais recente de deputados
        bucket_name = 'airflow-my-lab'
        response = s3_client.list_objects_v2(
            Bucket=bucket_name, 
            Prefix='raw/deputies/deputies_'
        )
        
        if not response.get('Contents'):
            # Fallback para API se não houver arquivo
            url = f"{API_BASE_URL}/deputados"
            resp = requests.get(url)
            resp.raise_for_status()
            return resp.json().get('dados', [])
        
        # Pega o arquivo mais recente
        latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'])[-1]
        obj = s3_client.get_object(Bucket=bucket_name, Key=latest_file['Key'])
        return json.loads(obj['Body'].read().decode('utf-8'))

    @task
    def extract_expenses_batch(deputies: List[Dict], **context) -> Dict[str, Any]:
        """Extrai despesas em lotes para otimizar performance"""
        params = context['params']
        ano = params.get('ano', pendulum.now().year)
        mes = params.get('mes', pendulum.now().month)
        
        all_expenses = []
        failed_deputies = []
        
        logging.info(f"Processando despesas para {ano}/{mes:02d}")
        
        for i, deputy in enumerate(deputies):
            deputy_id = deputy['id']
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
                        expense['deputy_name'] = deputy.get('nome')
                        expense['extracted_at'] = pendulum.now().isoformat()
                        expense['ano_referencia'] = ano
                        expense['mes_referencia'] = mes
                    
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
        aws_conn = BaseHook.get_connection('aws_s3_demo')
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name=aws_conn.extra_dejson.get('region_name', 'us-east-1')
        )
        
        bucket_name = 'airflow-my-lab'
        ano = extraction_result['ano']
        mes = extraction_result['mes']
        timestamp = pendulum.now().strftime('%Y%m%d')
        
        # Particionamento por ano/mês
        file_key = f"raw/expenses/full/year={ano}/month={mes:02d}/expenses_full_{timestamp}.json"
        
        # Dados para salvar
        data_to_save = {
            'metadata': {
                'ano': ano,
                'mes': mes,
                'total_expenses': extraction_result['total_expenses'],
                'total_deputies_processed': extraction_result['total_deputies_processed'],
                'failed_deputies_count': len(extraction_result['failed_deputies']),
                'extraction_timestamp': extraction_result['extraction_timestamp']
            },
            'expenses': extraction_result['expenses'],
            'failed_deputies': extraction_result['failed_deputies']
        }
        
        json_data = json.dumps(data_to_save, ensure_ascii=False, indent=2)
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=json_data.encode('utf-8'),
            ContentType='application/json',
            Metadata={
                'ano': str(ano),
                'mes': str(mes),
                'total_expenses': str(extraction_result['total_expenses']),
                'extraction_type': 'full'
            }
        )
        
        s3_path = f"s3://{bucket_name}/{file_key}"
        logging.info(f"Despesas salvas em: {s3_path}")
        return s3_path


    # Fluxo da DAG
    deputies = get_deputy_list()
    extraction_result = extract_expenses_batch(deputies)
    s3_path = load_expenses_to_s3(extraction_result)

dag_instance = despesas_full_etl()