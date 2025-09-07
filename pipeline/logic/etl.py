import os
import logging
from django.db import connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCHEMAS_FILE = os.path.join(BASE_PATH, 'scripts', '01-schemas.sql')
DDL_FILE = os.path.join(BASE_PATH, 'scripts', '02-ddl.sql')
COPY_FILE = os.path.join(BASE_PATH, 'scripts', '03-copy.sql')
APP_TRANSFORM_FILE = os.path.join(BASE_PATH, 'scripts', '04-load_transform.sql')

class PipelineStep:
    """Classe base para um passo no pipeline de ETL."""
    def __init__(self, cursor):
        self.cursor = cursor

    # O parâmetro contexts funciona como uma "mochila" ou um "dicionário de estado" que é carregado de um passo para o outro na sua "esteira de produção" de ETL. 
    # Cada passo pode olhar dentro da mochila, usar alguma informação, adicionar uma nova informação ou simplesmente passar a mochila adiante para o próximo passo.
    def execute(self, context=None):
        """Executa o passo do pipeline. Deve ser implementado pelas subclasses."""
        raise NotImplementedError

    def open_script(self, script_path):
        with open(script_path, 'r', encoding='utf-8') as f:
            return f.read()

class ExecuteSqlFileStep(PipelineStep):
    """Um passo do pipeline que executa um arquivo SQL."""
    def __init__(self, cursor, sql_file_path):
        super().__init__(cursor)
        self.sql_file_path = sql_file_path
        self.script_name = os.path.basename(sql_file_path)

    def execute(self, context=None):
        logging.info(f"Executando passo: {self.script_name}")
        self.cursor.execute(self.open_script(self.sql_file_path))
        logging.info(f"Passo {self.script_name} concluído com sucesso.")
        return context

class CopyDataStep(PipelineStep):
    """Um passo do pipeline que copia dados de um arquivo local para o banco."""
    def __init__(self, cursor, copy_sql_path):
        super().__init__(cursor)
        self.copy_sql_path = copy_sql_path

    def execute(self, context):
        data_file_path = context.get('data_file_path')
        if not data_file_path:
            raise ValueError("Caminho do arquivo de dados não encontrado no contexto do pipeline.")
        
        logging.info(f"Iniciando passo de cópia de dados de '{data_file_path}'...")
        copy_sql = self.open_script(self.copy_sql_path)

        with open(data_file_path, 'r', encoding='utf-8') as data_file:
            self.cursor.copy_expert(sql=copy_sql, file=data_file)
        
        logging.info(f"{self.cursor.rowcount} linhas carregadas.")
        return context

class ETLPipeline:
    """Orquestra uma sequência de passos de ETL."""
    def __init__(self, cursor):
        self.cursor = cursor
        self.steps = []

    def add_step(self, step_class, *args):
        self.steps.append(step_class(self.cursor, *args))
        return self

    def run(self, initial_context=None):
        context = initial_context or {}
        for step in self.steps:
            context = step.execute(context)
        return context

def run_full_pipeline(data_file_path):
    """Orquestra a execução completa do pipeline de ETL usando o padrão Pipeline."""
    logging.info("Iniciando processo de ETL completo...")
    try:
        with connection.cursor() as cursor:
            pipeline = ETLPipeline(cursor)
            
            pipeline.add_step(ExecuteSqlFileStep, DDL_FILE) \
                    .add_step(CopyDataStep, COPY_FILE) \
                    .add_step(ExecuteSqlFileStep, APP_TRANSFORM_FILE)

            initial_context = {'data_file_path': data_file_path}
            pipeline.run(initial_context)
        
        logging.info("Pipeline de ETL concluído com sucesso!")
        return True, None
    except Exception as e:
        logging.exception(f"Falha no pipeline de ETL: {e}")
        return False, str(e)
