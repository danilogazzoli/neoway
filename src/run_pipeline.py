import os
import psycopg2
from abc import ABC, abstractmethod
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5432"),
    "dbname": os.getenv("PG_DBNAME", "postgres"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}

BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_TXT_FILE = os.path.join(BASE_PATH, 'data', 'base_ficticia_dados_prova.txt')
SCHEMAS_FILE = os.path.join(BASE_PATH, 'scripts', '01-schemas.sql')
DDL_FILE = os.path.join(BASE_PATH, 'scripts', '02-ddl.sql')
COPY_FILE = os.path.join(BASE_PATH, 'scripts', '03-copy.sql')
TRANSFORM_FILE = os.path.join(BASE_PATH, 'scripts', '04-load_transform.sql')


def execute_sql_file(cursor, file_path):
    """Lê e executa o conteúdo de um arquivo SQL."""
    logging.info(f"Executando script: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        cursor.execute(f.read())
    logging.info(f"Script {os.path.basename(file_path)} executado com sucesso.")


def copy_from_local_file(cursor, copy_sql_path, data_file_path):
    """
    Executa um comando COPY FROM STDIN, alimentando-o com o conteúdo de um arquivo local.
    """
    logging.info(f"Iniciando COPY de '{data_file_path}' para 'raw.faturas'...")
    
    with open(copy_sql_path, 'r', encoding='utf-8') as sql_file:
        copy_sql = sql_file.read()

    with open(data_file_path, 'r', encoding='utf-8') as data_file:
        cursor.copy_expert(sql=copy_sql, file=data_file)
    
    logging.info(f"{cursor.rowcount} linhas carregadas em 'raw.faturas'.")


def main():
    """Função principal para orquestrar o pipeline."""
    conn = None
    try:
        logging.info("Conectando ao banco de dados PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            execute_sql_file(cursor, SCHEMAS_FILE)
            execute_sql_file(cursor, DDL_FILE)
            copy_from_local_file(cursor, COPY_FILE, INPUT_TXT_FILE)
            execute_sql_file(cursor, TRANSFORM_FILE)

        conn.commit()
        logging.info("Pipeline de dados concluído com sucesso! Alterações foram comitadas.")

    except psycopg2.Error as e:
        logging.error(f"ERRO de banco de dados: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")


if __name__ == "__main__":
    main()

