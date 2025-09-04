# c:\projetos\neoway\python\run_pipeline.py

import os
import sys
import psycopg2
from abc import ABC, abstractmethod

DB_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5432"),
    "dbname": os.getenv("PG_DBNAME", "postgres"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}

BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_TXT_FILE = os.path.join(BASE_PATH, 'data','base_ficticia_dados_prova.txt')

SCHEMAS_SQL_FILE = os.path.join(BASE_PATH, 'scripts','01-schemas.sql')
CREATE_TABLE_SQL_FILE = os.path.join(BASE_PATH, 'scripts','02-create_tables.sql')
COPY_SQL_FILE = os.path.join(BASE_PATH, 'scripts','03-copy.sql')


class PipelineStep(ABC):
    @abstractmethod
    def execute(self):
        pass

    @abstractmethod
    def execute_script(self, sql_script=None):
        pass

    @abstractmethod
    def copy_from(self, file_path=None):
        pass


class DbStep(PipelineStep):
    def __init__(self, cur):
        self.cur = cur

    def execute_script(self, sql_script=None):
        try:
            with open(sql_script, 'r', encoding='utf-8') as f:
                sql_script = f.read()
            self.cur.execute(sql_script)
        except FileNotFoundError:
            print(f"ERRO: Arquivo SQL '{sql_script}' não encontrado.")
            raise

    def copy_from(self, file_path=None):
        try:
            print(f"\nIniciando cópia de dados do arquivo '{file_path}' para a tabela 'raw.faturas'...")
            with open(file_path, 'r', encoding='utf-8') as f:
                self.cur.copy_from(f, 'raw.faturas', sep='\t', columns=('emitente','documento','contrato','categoria','qtdNota','fatura','valor','data_compra','data_pagamento'))
        except FileNotFoundError:
            print(f"ERRO: Arquivo CSV '{file_path}' não encontrado.")
            raise

class SchemaStep(DbStep):
    def __init__(self, cur):
        super().__init__(cur)

    def execute(self):
        return super().execute_script(SCHEMAS_SQL_FILE)

class CreateTableStep(DbStep):
    def __init__(self, cur):
        super().__init__(cur)

    def execute(self):
        return super().execute_script(CREATE_TABLE_SQL_FILE)


class DatabaseLoadStep(DbStep):
    def __init__(self, cur):
        super().__init__(cur)

    def execute(self):
        return super().copy_from(INPUT_TXT_FILE)    

class PipelineExecutor:
    def __init__(self, steps):
        self.steps = steps

    def run(self):
        result = None
        for step in self.steps:
            res = step.execute()
            if res is False:
                break
            if res is not None:
                result = res
        return result


def main():
    """Função principal para orquestrar o pipeline."""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        schema_step = SchemaStep(cur)
        create_table_step = CreateTableStep(cur)
        db_load = DatabaseLoadStep(cur)
        steps = [schema_step, create_table_step, db_load]
        executor = PipelineExecutor(steps)
        executor.run()
        conn.commit()
        for notice in conn.notices:
            print(f"DB LOG: {notice.strip()}")
        print("\nPipeline de banco de dados concluído com sucesso!")
    except FileNotFoundError as e:
        print(f"ERRO: {e}")
        conn.rollback()
    except psycopg2.Error as e:
        print(f"ERRO de banco de dados: {e}")
        conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
