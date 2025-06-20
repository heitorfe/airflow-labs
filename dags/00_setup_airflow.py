"""
DAG 00: Setup do Airflow Labs
=============================

Esta DAG configura automaticamente todas as Variables e Connections
necessárias para executar as outras DAGs do laboratório.

Execute esta DAG PRIMEIRO antes de usar as outras DAGs.

Funcionalidades:
- Criação de Variables do Airflow
- Configuração de Connections
- Criação de diretórios necessários
- Validação da configuração

Conceitos:
- Configuração programática do Airflow
- Gerenciamento de Variables e Connections
- Setup automatizado de ambiente
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable, Connection
from airflow.utils.session import provide_session
from airflow.hooks.base import BaseHook
import os
import json


@dag(
    dag_id='00_setup_airflow',
    description='Setup automático de Variables e Connections para o Airflow Labs',
    tags=['setup', 'configuration', 'variables', 'connections'],
    schedule=None,  # Execução manual apenas
    start_date=datetime(2024, 1, 1),
    catchup=False,
    doc_md=__doc__,
    default_args={
        'owner': 'airflow-lab',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
)
def setup_airflow_dag():
    
    @task
    def create_directories():
        """Cria diretórios necessários para as DAGs"""
        
        directories = [
            '/tmp/airflow_data',
            '/tmp/airflow_data/raw',
            '/tmp/airflow_data/processed',
            '/tmp/airflow_data/output',
            '/tmp/etl_output'  # Para a DAG 15 (ETL Pipeline)
        ]
        
        print("📁 Criando diretórios necessários...")
        created_dirs = []
        errors = []
        
        for directory in directories:
            try:
                os.makedirs(directory, exist_ok=True)
                created_dirs.append(directory)
                print(f"✅ Diretório criado/verificado: {directory}")
            except Exception as e:
                error_msg = f"❌ Erro ao criar diretório '{directory}': {e}"
                print(error_msg)
                errors.append(error_msg)
        
        return {
            'created_directories': created_dirs,
            'errors': errors,
            'total_created': len(created_dirs)
        }
    
    @task
    def setup_variables():
        """Configura as variáveis do Airflow necessárias para as DAGs"""
        
        variables_config = {
            'api_base_url': {
                'value': 'https://jsonplaceholder.typicode.com',
                'description': 'Base URL for JSONPlaceholder API'
            },
            'data_path': {
                'value': '/tmp/airflow_data',
                'description': 'Base path for data storage'
            },
            'retry_count': {
                'value': '3',
                'description': 'Default retry count for tasks'
            },
            'email_on_failure': {
                'value': 'admin@example.com',
                'description': 'Email for failure notifications'
            },
            'max_records': {
                'value': '1000',
                'description': 'Maximum records to process'
            },
            'environment': {
                'value': 'development',
                'description': 'Current environment'
            },
            'db_schema': {
                'value': 'public',
                'description': 'Default database schema'
            },
            'file_format': {
                'value': 'csv',
                'description': 'Default file format for outputs'
            },
            'timeout_seconds': {
                'value': '300',
                'description': 'Default timeout for operations'
            },
            'coindesk_url': {
                'value': 'https://api.coindesk.com/v1/bpi/currentprice.json',
                'description': 'CoinDesk API URL for cryptocurrency data'
            }
        }
        
        print("🔧 Configurando Variables do Airflow...")
        configured_vars = []
        errors = []
        
        for key, config in variables_config.items():
            try:
                # Verifica se a variável já existe
                try:
                    existing_value = Variable.get(key)
                    print(f"⚠️  Variable '{key}' já existe com valor: {existing_value}")
                    configured_vars.append(f"{key} (já existia)")
                except:
                    # Variável não existe, criar nova
                    Variable.set(
                        key=key, 
                        value=config['value'],
                        description=config['description']
                    )
                    configured_vars.append(key)
                    print(f"✅ Variable '{key}' configurada: {config['value']}")
                    
            except Exception as e:
                error_msg = f"❌ Erro ao configurar variable '{key}': {e}"
                print(error_msg)
                errors.append(error_msg)
        
        return {
            'configured_variables': configured_vars,
            'errors': errors,
            'total_configured': len(configured_vars)
        }
    
    @task
    def setup_connections():
        """Configura as conexões do Airflow necessárias para as DAGs"""
        
        connections_config = [
            {
                'conn_id': 'http_default',
                'conn_type': 'http',
                'host': 'jsonplaceholder.typicode.com',
                'port': 443,
                'schema': 'https',
                'description': 'HTTP connection for JSONPlaceholder API'
            },
            {
                'conn_id': 'coindesk_api',
                'conn_type': 'http',
                'host': 'api.coindesk.com',
                'port': 443,
                'schema': 'https',
                'description': 'CoinDesk API connection for cryptocurrency data'
            },
            {
                'conn_id': 'httpbin_api',
                'conn_type': 'http',
                'host': 'httpbin.org',
                'port': 443,
                'schema': 'https',
                'description': 'HTTPBin API for HTTP testing'
            },
            {
                'conn_id': 'postgres_local',
                'conn_type': 'postgres',
                'host': 'localhost',
                'port': 5432,
                'schema': 'airflow_demo',
                'login': 'airflow',
                'password': 'airflow',
                'description': 'Local PostgreSQL connection (configure conforme necessário)'
            },
            {
                'conn_id': 'aws_s3_demo',
                'conn_type': 'aws',
                'description': 'AWS S3 demo connection (configure com suas credenciais)',
                'extra': json.dumps({
                    'region_name': 'us-east-1',
                    'aws_access_key_id': 'DEMO_KEY',
                    'aws_secret_access_key': 'DEMO_SECRET'
                })
            }
        ]
        
        print("🔗 Configurando Connections do Airflow...")
        configured_conns = []
        errors = []
        
        print("⚠️  AVISO: Airflow 3.0 não permite acesso direto ao ORM/Database")
        print("🔧 As seguintes conexões devem ser criadas MANUALMENTE:")
        print("   Admin > Connections > + (Adicionar)")
        print()
        
        for conn_data in connections_config:
            try:
                # Tentar verificar se a conexão já existe usando BaseHook
                try:
                    existing_conn = BaseHook.get_connection(conn_data['conn_id'])
                    print(f"✅ Connection '{conn_data['conn_id']}' já existe: {existing_conn.host}")
                    configured_conns.append(f"{conn_data['conn_id']} (já existe)")
                except Exception:
                    # Connection não existe, mostrar instruções para criação manual
                    print(f"❗ Connection '{conn_data['conn_id']}' NÃO EXISTE")
                    print(f"   Conn ID: {conn_data['conn_id']}")
                    print(f"   Conn Type: {conn_data['conn_type']}")
                    if 'host' in conn_data:
                        print(f"   Host: {conn_data['host']}")
                    if 'port' in conn_data:
                        print(f"   Port: {conn_data['port']}")
                    if 'schema' in conn_data:
                        print(f"   Schema: {conn_data['schema']}")
                    if 'login' in conn_data:
                        print(f"   Login: {conn_data['login']}")
                    if 'password' in conn_data:
                        print(f"   Password: {conn_data['password']}")
                    if 'extra' in conn_data:
                        print(f"   Extra: {conn_data['extra']}")
                    print(f"   Description: {conn_data['description']}")
                    print()
                    configured_conns.append(f"{conn_data['conn_id']} (criar manualmente)")
                    
            except Exception as e:
                error_msg = f"❌ Erro ao verificar connection '{conn_data['conn_id']}': {e}"
                print(error_msg)
                errors.append(error_msg)
        
        print("📝 INSTRUÇÕES PARA CRIAR CONNECTIONS MANUALMENTE:")
        print("1. Acesse a UI do Airflow")
        print("2. Vá em Admin > Connections")
        print("3. Clique no botão '+ Adicionar'")
        print("4. Use os dados mostrados acima para cada conexão")
        print("5. Execute novamente esta DAG para verificar")
        
        return {
            'configured_connections': configured_conns,
            'errors': errors,
            'total_configured': len(configured_conns)
        }
    
    @task
    def validate_setup(dirs_result: dict, vars_result: dict, conns_result: dict):
        """Valida se a configuração foi realizada com sucesso"""
        
        print("🔍 Validando configuração...")
        
        # Validar diretórios
        print("\n📁 Validação de Diretórios:")
        for directory in dirs_result['created_directories']:
            if os.path.exists(directory):
                print(f"✅ {directory} - OK")
            else:
                print(f"❌ {directory} - FALHA")
        
        # Validar algumas variáveis importantes
        print("\n🔧 Validação de Variables:")
        important_vars = ['api_base_url', 'data_path', 'environment']
        for var_name in important_vars:
            try:
                value = Variable.get(var_name)
                print(f"✅ {var_name} = {value}")
            except Exception as e:
                print(f"❌ {var_name} - Erro: {e}")
        
        # Validar algumas conexões importantes
        print("\n🔗 Validação de Connections:")
        important_conns = ['http_default', 'coindesk_api']
        for conn_id in important_conns:
            try:
                conn = BaseHook.get_connection(conn_id)
                print(f"✅ {conn_id} - {conn.host}")
            except Exception as e:
                print(f"❌ {conn_id} - Erro: {e}")
        
        # Resumo geral
        total_errors = len(dirs_result.get('errors', [])) + \
                      len(vars_result.get('errors', [])) + \
                      len(conns_result.get('errors', []))
        
        setup_status = "SUCCESS" if total_errors == 0 else "PARTIAL_SUCCESS"
        
        summary = {
            'status': setup_status,
            'directories_created': dirs_result['total_created'],
            'variables_configured': vars_result['total_configured'],
            'connections_configured': conns_result['total_configured'],
            'total_errors': total_errors,
            'setup_timestamp': datetime.now().isoformat()
        }
        
        print("\n" + "=" * 50)
        print(f"🎉 SETUP {'CONCLUÍDO' if setup_status == 'SUCCESS' else 'PARCIALMENTE CONCLUÍDO'}!")
        print("=" * 50)
        print(f"📁 Diretórios criados: {summary['directories_created']}")
        print(f"🔧 Variables configuradas: {summary['variables_configured']}")
        print(f"🔗 Connections configuradas: {summary['connections_configured']}")
        print(f"❌ Total de erros: {summary['total_errors']}")
        
        if setup_status == "SUCCESS":
            print("\n✅ Ambiente configurado com sucesso!")
            print("📝 Próximos passos:")
            print("1. Acesse Admin > Variables para verificar as variáveis")
            print("2. Acesse Admin > Connections para verificar as conexões")
            print("3. Ative as DAGs numeradas (01_variables_connections, etc.)")
            print("4. Execute as DAGs e monitore os logs para aprender!")
        else:
            print("\n⚠️  Setup concluído com alguns erros.")
            print("Verifique os logs acima para detalhes.")
        
        return summary
    
    @task
    def cleanup_test_files():
        """Remove arquivos de teste antigos se existirem"""
        
        test_files = [
            '/tmp/sensor_test_file.txt',
            '/tmp/airflow_data/test_data.csv',
            '/tmp/test_transfer_file.txt'
        ]
        
        print("🧹 Limpando arquivos de teste antigos...")
        cleaned_files = []
        
        for file_path in test_files:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    cleaned_files.append(file_path)
                    print(f"🗑️  Removido: {file_path}")
            except Exception as e:
                print(f"⚠️  Erro ao remover {file_path}: {e}")
        
        print(f"✅ Limpeza concluída: {len(cleaned_files)} arquivos removidos")
        return {'cleaned_files': cleaned_files, 'count': len(cleaned_files)}
    
    # === DEFINIÇÃO DO FLUXO ===
    
    # Limpeza inicial
    cleanup_result = cleanup_test_files()
    
    # Setup paralelo
    dirs_result = create_directories()
    vars_result = setup_variables()
    conns_result = setup_connections()
    
    # Validação final
    validation_result = validate_setup(dirs_result, vars_result, conns_result)
    
    # === DEPENDÊNCIAS ===
    
    # Limpeza primeiro
    cleanup_result >> [dirs_result, vars_result, conns_result]
    
    # Validação após setup completo
    [dirs_result, vars_result, conns_result] >> validation_result


# Instanciar a DAG
setup_dag = setup_airflow_dag()


if __name__ == "__main__":
    # Teste local
    print("DAG 00: Setup do Airflow Labs")
    print("=" * 50)
    print("Esta DAG configura automaticamente:")
    print("📁 Diretórios necessários")
    print("🔧 Variables do Airflow")
    print("🔗 Connections para APIs")
    print("🔍 Validação da configuração")
    print("=" * 50)
    print("Execute esta DAG PRIMEIRO antes das outras!")
