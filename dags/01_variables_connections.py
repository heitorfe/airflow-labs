"""
DAG 01: Uso de Variables e Connections
Demonstra como usar Variables do Airflow e Connections com BaseHook
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import logging


default_args = {
    'owner': 'airflow-labs',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


@dag(
    dag_id='01_variables_connections',
    default_args=default_args,
    description='Demonstra uso de Variables e Connections do Airflow',
    schedule='@daily',
    catchup=False,
    tags=['tutorial', 'variables', 'connections', 'basics']
)
def variables_connections_dag():
    """
    Esta DAG demonstra:
    1. Como ler Variables do Airflow
    2. Como usar Connections com BaseHook
    3. Como passar dados entre tarefas
    4. Boas práticas de configuração
    """

    @task
    def read_airflow_variables():
        """
        Lê variáveis configuradas no Airflow e demonstra diferentes formas de acesso
        """
        logging.info("📖 Lendo Variables do Airflow...")
        
        try:
            # Método 1: Variable.get() com valor padrão
            api_base_url = Variable.get("api_base_url", default_var="https://httpbin.org")
            data_path = Variable.get("data_path", default_var="/tmp")
            retry_count = Variable.get("retry_count", default_var="3")
            environment = Variable.get("environment", default_var="development")
            
            # Método 2: Variable.get() com tratamento de exceção
            try:
                max_records = Variable.get("max_records")
            except KeyError:
                max_records = "100"  # valor padrão
                logging.warning("Variable 'max_records' não encontrada, usando valor padrão")
            
            # Compilar informações das variáveis
            variables_info = {
                'api_base_url': api_base_url,
                'data_path': data_path,
                'retry_count': int(retry_count),
                'environment': environment,
                'max_records': int(max_records),
                'read_timestamp': datetime.now().isoformat()
            }
            
            logging.info("✅ Variables lidas com sucesso:")
            for key, value in variables_info.items():
                logging.info(f"  {key}: {value}")
            
            return variables_info
            
        except Exception as e:
            logging.error(f"❌ Erro ao ler variables: {e}")
            raise

    @task
    def get_connection_info():
        """
        Obtém informações de conexões configuradas usando BaseHook
        """
        logging.info("🔗 Obtendo informações de Connections...")
        
        connections_info = {}
        
        # Lista de conexões para verificar
        connection_ids = ['http_default', 'coindesk_api', 'postgres_local']
        
        for conn_id in connection_ids:
            try:
                # Usar BaseHook para obter conexão
                connection = BaseHook.get_connection(conn_id)
                
                # Extrair informações da conexão (sem expor senhas)
                conn_info = {
                    'conn_id': connection.conn_id,
                    'conn_type': connection.conn_type,
                    'host': connection.host,
                    'port': connection.port,
                    'schema': connection.schema,
                    'login': connection.login,
                    'has_password': bool(connection.password),
                    'description': getattr(connection, 'description', None)
                }
                
                connections_info[conn_id] = conn_info
                logging.info(f"✅ Connection '{conn_id}' encontrada: {connection.host}")
                
            except Exception as e:
                logging.warning(f"⚠️ Connection '{conn_id}' não encontrada ou erro: {e}")
                connections_info[conn_id] = {'status': 'not_found', 'error': str(e)}
        
        return connections_info

    @task
    def validate_configuration(variables_data: dict, connections_data: dict):
        """
        Valida se a configuração está adequada para execução das DAGs
        """
        logging.info("🔍 Validando configuração do ambiente...")
        
        validation_results = {
            'variables': {},
            'connections': {},
            'overall_status': 'unknown'
        }
        
        # Validar variáveis críticas
        required_variables = ['api_base_url', 'data_path', 'retry_count']
        variables_ok = True
        
        for var_name in required_variables:
            if var_name in variables_data and variables_data[var_name]:
                validation_results['variables'][var_name] = 'ok'
                logging.info(f"✅ Variable '{var_name}': OK")
            else:
                validation_results['variables'][var_name] = 'missing'
                variables_ok = False
                logging.error(f"❌ Variable '{var_name}': MISSING")
        
        # Validar conexões importantes
        important_connections = ['http_default']
        connections_ok = True
        
        for conn_id in important_connections:
            if conn_id in connections_data and 'host' in connections_data[conn_id]:
                validation_results['connections'][conn_id] = 'ok'
                logging.info(f"✅ Connection '{conn_id}': OK")
            else:
                validation_results['connections'][conn_id] = 'missing'
                connections_ok = False
                logging.error(f"❌ Connection '{conn_id}': MISSING")
        
        # Status geral
        if variables_ok and connections_ok:
            validation_results['overall_status'] = 'healthy'
            logging.info("🎉 Configuração do ambiente: SAUDÁVEL")
        else:
            validation_results['overall_status'] = 'issues_found'
            logging.warning("⚠️ Configuração do ambiente: PROBLEMAS ENCONTRADOS")
        
        return validation_results

    @task
    def generate_config_report(variables_data: dict, connections_data: dict, validation_data: dict):
        """
        Gera um relatório consolidado da configuração
        """
        logging.info("📊 Gerando relatório de configuração...")
        
        report = {
            'report_timestamp': datetime.now().isoformat(),
            'environment_status': validation_data['overall_status'],
            'variables_summary': {
                'total_read': len(variables_data),
                'environment': variables_data.get('environment', 'unknown'),
                'data_path': variables_data.get('data_path', 'not_set')
            },
            'connections_summary': {
                'total_checked': len(connections_data),
                'available': len([c for c in connections_data.values() if 'host' in c]),
                'missing': len([c for c in connections_data.values() if c.get('status') == 'not_found'])
            },
            'recommendations': []
        }
        
        # Adicionar recomendações baseadas na validação
        if validation_data['overall_status'] != 'healthy':
            report['recommendations'].append("Execute o script setup_airflow.py para configurar o ambiente")
            
            # Recomendações específicas para variáveis
            for var, status in validation_data['variables'].items():
                if status == 'missing':
                    report['recommendations'].append(f"Configure a variable '{var}' no Airflow UI")
            
            # Recomendações específicas para conexões
            for conn, status in validation_data['connections'].items():
                if status == 'missing':
                    report['recommendations'].append(f"Configure a connection '{conn}' no Airflow UI")
        
        logging.info("📋 Relatório de Configuração:")
        logging.info(f"  Status do Ambiente: {report['environment_status']}")
        logging.info(f"  Variables lidas: {report['variables_summary']['total_read']}")
        logging.info(f"  Connections disponíveis: {report['connections_summary']['available']}")
        
        if report['recommendations']:
            logging.info("💡 Recomendações:")
            for rec in report['recommendations']:
                logging.info(f"  - {rec}")
        
        return report

    # Definir dependências das tarefas
    variables_data = read_airflow_variables()
    connections_data = get_connection_info()
    validation_data = validate_configuration(variables_data, connections_data)
    final_report = generate_config_report(variables_data, connections_data, validation_data)

    # Dependências implícitas pela passagem de dados entre tarefas
    variables_data >> validation_data
    connections_data >> validation_data
    validation_data >> final_report


# Instanciar a DAG
dag_instance = variables_connections_dag()
