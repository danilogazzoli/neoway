import os
import logging
from django.db import connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCHEMAS_FILE = os.path.join(BASE_PATH, 'scripts', '01-schemas.sql')
DDL_FILE = os.path.join(BASE_PATH, 'scripts', '02-ddl.sql')
COPY_FILE = os.path.join(BASE_PATH, 'scripts', '03-copy.sql')
APP_TRANSFORM_FILE = os.path.join(BASE_PATH, 'scripts', '04-load_transform.sql')

def open_script(file_script):
    """Abre um arquivo de script SQL e retorna seu conteúdo."""
    with open(file_script, 'r', encoding='utf-8') as f:
        return f.read()
    return None    

def execute_sql_file(cursor, file_path):
    logging.info(f"Executando script: {file_path}")
    cursor.execute(open_script(file_path))
    logging.info(f"Script {os.path.basename(file_path)} executado com sucesso.")

def copy_from_local_file(cursor, data_file_path):
    logging.info(f"Iniciando COPY de '{data_file_path}' para 'raw.transacao'...")
    cursor.copy_expert(sql=open_script(COPY_FILE), file=open(data_file_path, 'r', encoding='utf-8'))
    logging.info(f"{cursor.rowcount} linhas carregadas em 'raw.transacao'.")

def run_full_pipeline(data_file_path):
    logging.info("Iniciando processo de ETL completo...")
    try:
        with connection.cursor() as cursor:
            execute_sql_file(cursor, SCHEMAS_FILE)
            execute_sql_file(cursor, DDL_FILE)
            copy_from_local_file(cursor, data_file_path)
            execute_sql_file(cursor, APP_TRANSFORM_FILE)
        
        logging.info("Pipeline de ETL concluído com sucesso!")
        return True, None
    except Exception as e:
        logging.error(f"Falha no pipeline de ETL: {e}", exc_info=True)
        return False, str(e)
