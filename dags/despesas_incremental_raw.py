"""
DAG 04: Carga Incremental das Despesas dos Deputados
Executa carga diária das despesas mais recentes
"""

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow import Dataset
import pendulum
import boto3
import requests
import json
from typing import List, Dict, Any, Optional
import logging

# Variáveis
API_BASE_URL = Variable.get("camara_api_base_url", "https://dadosabertos.camara.leg.br/api/v2")
BUCKET_NAME = 'airflow-my-lab'

# Datasets
expenses_dataset_incremental = Dataset("s3://camara/raw/expenses/incremental/")

default_args = {
    'owner': 'data-team',
    'start_date': pendulum.datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': pendulum.duration(minutes=2),
}

@dag(
    dag_id='04_despesas_deputados_incremental',
    default_args=default_args,
    description='Carga incremental diária das despesas dos deputados',
    schedule='0 6 * * *',  # Todo dia às 06:00
    catchup=False,
    tags=['api', 's3', 'expenses', 'incremental'],
    params={
        'days_back': 7,  # Quantos dias para trás verificar
        'force_current_month': True
    }
)
def despesas_incremental_etl():
    
    @task
    def get_last_extraction_info() -> Dict[str, Any]:
        """Verifica última extração para determinar período incremental"""
        aws_conn = BaseHook.get_connection('aws_s3_demo')
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name=aws_conn.extra_dejson.get('region_name', 'us-east-1')
        )
                
        # Verifica último arquivo de controle incremental
        try:
            response = s3_client.list_objects_v2(
                Bucket=BUCKET_NAME,
                Prefix='control/expenses/incremental_'
            )
            
            if response.get('Contents'):
                latest_control = sorted(response['Contents'], key=lambda x: x['LastModified'])[-1]
                obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=latest_control['Key'])
                last_extraction = json.loads(obj['Body'].read().decode('utf-8'))
                
                return {
                    'last_extraction_date': last_extraction.get('extraction_date'),
                    'last_year': last_extraction.get('year'),
                    'last_month': last_extraction.get('month'),
                    'has_previous': True
                }
        except Exception as e:
            logging.warning(f"Erro ao ler último controle: {e}")
        
        # Se não há extração anterior, usar mês atual
        now = pendulum.now()
        return {
            'last_extraction_date': None,
            'last_year': now.year,
            'last_month': now.month,
            'has_previous': False
        }

    @task
    def determine_extraction_period(last_info: Dict[str, Any], **context) -> Dict[str, Any]:
        """Determina período para extração incremental"""
        params = context['params']
        days_back = params.get('days_back', 7)
        force_current_month = params.get('force_current_month', True)
        
        now = pendulum.now()
        
        if force_current_month or not last_info['has_previous']:
            # Sempre pega o mês atual + anterior para garantir completude
            periods = [
                {'year': now.year, 'month': now.month},
            ]
            
            # Adiciona mês anterior se estamos no início do mês
            if now.day <= 5:
                prev_month = now.subtract(months=1)
                periods.insert(0, {'year': prev_month.year, 'month': prev_month.month})
        else:
            # Usa informação da última extração
            periods = [
                {'year': last_info['last_year'], 'month': last_info['last_month']}
            ]
        
        return {
            'periods': periods,
            'extraction_date': now.strftime('%Y-%m-%d'),
            'is_initial_load': not last_info['has_previous']
        }

    @task
    def get_active_deputies() -> List[Dict]:
        """Obtém lista de deputados ativos"""
        url = f"{API_BASE_URL}/deputados"
        params = {'ordem': 'ASC', 'ordenarPor': 'nome'}
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json().get('dados', [])
        except Exception as e:
            logging.error(f"Erro ao obter deputados: {e}")
            raise

    @task
    def extract_incremental_expenses(
        deputies: List[Dict], 
        period_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Extrai despesas para os períodos determinados"""
        
        all_expenses = []
        failed_deputies = []
        extraction_summary = {
            'periods_processed': [],
            'total_expenses': 0,
            'deputies_processed': 0,
            'extraction_timestamp': pendulum.now().isoformat()
        }
        
        for period in period_info['periods']:
            ano = period['year']
            mes = period['month']
            
            logging.info(f"Processando período: {ano}/{mes:02d}")
            period_expenses = []
            
            for i, deputy in enumerate(deputies):
                deputy_id = deputy['id']
                try:
                    url = f"{API_BASE_URL}/deputados/{deputy_id}/despesas"
                    params = {
                        'ano': ano,
                        'mes': mes,
                        'itens': 100,
                        'ordem': 'DESC',
                        'ordenarPor': 'dataDocumento'
                    }
                    
                    response = requests.get(url, params=params, timeout=20)
                    response.raise_for_status()
                    
                    data = response.json()
                    expenses = data.get('dados', [])
                    
                    # Adiciona metadados
                    for expense in expenses:
                        expense['deputy_id'] = deputy_id
                        expense['deputy_name'] = deputy.get('nome')
                        expense['extraction_type'] = 'incremental'
                        expense['extracted_at'] = pendulum.now().isoformat()
                        expense['periodo_referencia'] = f"{ano}-{mes:02d}"
                    
                    period_expenses.extend(expenses)
                    
                except Exception as e:
                    logging.warning(f"Erro ao processar deputado {deputy_id} em {ano}/{mes}: {e}")
                    failed_deputies.append({
                        'deputy_id': deputy_id,
                        'period': f"{ano}-{mes:02d}",
                        'error': str(e)
                    })
                    continue
            
            all_expenses.extend(period_expenses)
            extraction_summary['periods_processed'].append({
                'year': ano,
                'month': mes,
                'expenses_count': len(period_expenses)
            })
            
            logging.info(f"Período {ano}/{mes:02d}: {len(period_expenses)} despesas extraídas")
        
        extraction_summary.update({
            'total_expenses': len(all_expenses),
            'deputies_processed': len(deputies),
            'failed_deputies_count': len(failed_deputies),
            'extraction_date': period_info['extraction_date'],
            'is_initial_load': period_info['is_initial_load']
        })
        
        return {
            'expenses': all_expenses,
            'failed_deputies': failed_deputies,
            'summary': extraction_summary
        }

    @task(outlets=[expenses_dataset_incremental])
    def save_incremental_data(extraction_data: Dict[str, Any]) -> str:
        """Salva dados incrementais no S3"""
        aws_conn = BaseHook.get_connection('aws_s3_demo')
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name=aws_conn.extra_dejson.get('region_name', 'us-east-1')
        )
        
        timestamp = pendulum.now().strftime('%Y%m%d')
        
        # Arquivo de dados
        data_key = f"raw/expenses/incremental/incremental_expenses_{timestamp}.json"
        
        data_to_save = {
            'metadata': extraction_data['summary'],
            'expenses': extraction_data['expenses'],
            'failed_deputies': extraction_data['failed_deputies']
        }
        
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=data_key,
            Body=json.dumps(data_to_save, ensure_ascii=False, indent=2).encode('utf-8'),
            ContentType='application/json',
            Metadata={
                'extraction_type': 'incremental',
                'total_expenses': str(extraction_data['summary']['total_expenses']),
                'extraction_date': extraction_data['summary']['extraction_date']
            }
        )
        
        # Arquivo de controle
        control_key = f"control/expenses/incremental_{timestamp}.json"
        control_data = {
            'load_type': 'incremental',
            'data_file': f"s3://{BUCKET_NAME}/{data_key}",
            'extraction_date': extraction_data['summary']['extraction_date'],
            'periods_processed': extraction_data['summary']['periods_processed'],
            'total_expenses': extraction_data['summary']['total_expenses'],
            'load_timestamp': pendulum.now().isoformat(),
            'status': 'completed'
        }
        
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=control_key,
            Body=json.dumps(control_data, ensure_ascii=False, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
        
        s3_path = f"s3://{BUCKET_NAME}/{data_key}"
        logging.info(f"Dados incrementais salvos em: {s3_path}")
        return s3_path

    # Fluxo da DAG
    last_info = get_last_extraction_info()
    period_info = determine_extraction_period(last_info)
    deputies = get_active_deputies()
    extraction_data = extract_incremental_expenses(deputies, period_info)
    s3_path = save_incremental_data(extraction_data)

dag_instance = despesas_incremental_etl()