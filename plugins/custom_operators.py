"""
Operadores customizados para demonstração no Airflow Labs
"""

from typing import Any, Dict
from airflow.models import BaseOperator
from airflow.utils.context import Context
import pandas as pd
import json
import logging


class DataValidationOperator(BaseOperator):
    """
    Operador customizado para validação de dados
    Valida se um DataFrame atende a critérios específicos
    """
    
    def __init__(
        self,
        data_source: str,
        validation_rules: Dict[str, Any],
        **kwargs
    ):
        super().__init__(**kwargs)
        self.data_source = data_source
        self.validation_rules = validation_rules
    
    def execute(self, context: Context):
        """Executa a validação dos dados"""
        
        logging.info(f"Iniciando validação de dados para: {self.data_source}")
        
        # Em um cenário real, você carregaria os dados do data_source
        # Para demo, vamos simular alguns dados
        sample_data = pd.DataFrame({
            'id': range(1, 101),
            'name': [f'Item {i}' for i in range(1, 101)],
            'value': [i * 10.5 for i in range(1, 101)],
            'category': ['A' if i % 2 == 0 else 'B' for i in range(1, 101)]
        })
        
        validation_results = {}
        
        # Aplicar regras de validação
        for rule_name, rule_config in self.validation_rules.items():
            try:
                if rule_name == 'min_records':
                    result = len(sample_data) >= rule_config
                    validation_results[rule_name] = {
                        'passed': result,
                        'expected': f">= {rule_config}",
                        'actual': len(sample_data)
                    }
                
                elif rule_name == 'required_columns':
                    missing_cols = set(rule_config) - set(sample_data.columns)
                    result = len(missing_cols) == 0
                    validation_results[rule_name] = {
                        'passed': result,
                        'expected': rule_config,
                        'missing': list(missing_cols)
                    }
                
                elif rule_name == 'no_nulls':
                    null_cols = sample_data.columns[sample_data.isnull().any()].tolist()
                    result = len(null_cols) == 0
                    validation_results[rule_name] = {
                        'passed': result,
                        'columns_with_nulls': null_cols
                    }
                
                logging.info(f"Regra '{rule_name}': {'✅ PASSOU' if result else '❌ FALHOU'}")
                
            except Exception as e:
                logging.error(f"Erro ao aplicar regra '{rule_name}': {e}")
                validation_results[rule_name] = {
                    'passed': False,
                    'error': str(e)
                }
        
        # Resultado final
        all_passed = all(rule['passed'] for rule in validation_results.values())
        
        result = {
            'data_source': self.data_source,
            'timestamp': context['ts'],
            'total_records': len(sample_data),
            'validation_results': validation_results,
            'overall_status': 'PASSED' if all_passed else 'FAILED'
        }
        
        logging.info(f"Validação concluída: {result['overall_status']}")
        
        if not all_passed:
            raise ValueError(f"Validação falhou para: {self.data_source}")
        
        return result


class FileProcessorOperator(BaseOperator):
    """
    Operador customizado para processamento de arquivos
    Demonstra operações comuns de ETL
    """
    
    def __init__(
        self,
        input_file: str,
        output_file: str,
        processing_type: str = 'clean',
        **kwargs
    ):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.output_file = output_file
        self.processing_type = processing_type
    
    def execute(self, context: Context):
        """Processa o arquivo conforme o tipo especificado"""
        
        logging.info(f"Processando arquivo: {self.input_file}")
        logging.info(f"Tipo de processamento: {self.processing_type}")
        
        try:
            # Simular leitura do arquivo (em produção, use o caminho real)
            df = pd.DataFrame({
                'id': range(1, 51),
                'name': [f'Product {i}' for i in range(1, 51)],
                'price': [round(i * 19.99, 2) for i in range(1, 51)],
                'category': ['Electronics' if i % 3 == 0 else 'Books' if i % 2 == 0 else 'Clothing' for i in range(1, 51)],
                'in_stock': [True if i % 4 != 0 else False for i in range(1, 51)]
            })
            
            if self.processing_type == 'clean':
                # Limpeza básica
                df = df.dropna()
                df['name'] = df['name'].str.strip()
                df['price'] = df['price'].round(2)
                
            elif self.processing_type == 'aggregate':
                # Agregação por categoria
                df = df.groupby('category').agg({
                    'id': 'count',
                    'price': ['mean', 'sum', 'min', 'max'],
                    'in_stock': 'sum'
                }).round(2)
                df.columns = ['product_count', 'avg_price', 'total_value', 'min_price', 'max_price', 'in_stock_count']
                df = df.reset_index()
                
            elif self.processing_type == 'filter':
                # Filtrar apenas produtos em estoque
                df = df[df['in_stock'] == True]
                
            # Simular salvamento (em produção, salve no caminho real)
            logging.info(f"Processamento concluído. Registros processados: {len(df)}")
            logging.info(f"Arquivo de saída (simulado): {self.output_file}")
            
            return {
                'input_file': self.input_file,
                'output_file': self.output_file,
                'processing_type': self.processing_type,
                'records_processed': len(df),
                'columns': list(df.columns),
                'sample_data': df.head(3).to_dict('records')
            }
            
        except Exception as e:
            logging.error(f"Erro no processamento: {e}")
            raise


class NotificationOperator(BaseOperator):
    """
    Operador customizado para envio de notificações
    Simula envio de alertas e relatórios
    """
    
    def __init__(
        self,
        message: str,
        notification_type: str = 'info',
        recipients: list = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.message = message
        self.notification_type = notification_type
        self.recipients = recipients or ['admin@example.com']
    
    def execute(self, context: Context):
        """Simula envio de notificação"""
        
        notification_data = {
            'timestamp': context['ts'],
            'dag_id': context['dag'].dag_id,
            'task_id': context['task'].task_id,
            'run_id': context['run_id'],
            'message': self.message,
            'type': self.notification_type,
            'recipients': self.recipients
        }
        
        # Em produção, aqui você integraria com:
        # - Email (SMTP)
        # - Slack API
        # - Teams Webhook
        # - SMS Gateway
        # etc.
        
        logging.info("📧 Simulando envio de notificação:")
        logging.info(f"Tipo: {self.notification_type.upper()}")
        logging.info(f"Para: {', '.join(self.recipients)}")
        logging.info(f"Mensagem: {self.message}")
        
        if self.notification_type == 'error':
            logging.error("🚨 NOTIFICAÇÃO DE ERRO ENVIADA")
        elif self.notification_type == 'success':
            logging.info("✅ NOTIFICAÇÃO DE SUCESSO ENVIADA")
        else:
            logging.info("ℹ️ NOTIFICAÇÃO INFORMATIVA ENVIADA")
        
        return notification_data
