from django.utils import timezone
from .models import Arquivo
from .logic.etl import run_full_pipeline

def process_file_task(arquivo_id):
    try:
        arquivo = Arquivo.objects.get(id=arquivo_id)
        arquivo.status = 'PROCESSANDO'
        arquivo.data_inicio_processamento = timezone.now()
        arquivo.save()

        success, error_message = run_full_pipeline(arquivo.arquivo_txt.path)

        if success:
            arquivo.status = 'CONCLUIDO'
        else:
            arquivo.status = 'ERRO'
            arquivo.detalhes_erro = error_message
        
        arquivo.data_fim_processamento = timezone.now()
        arquivo.save()

    except Arquivo.DoesNotExist:
        print(f"Arquivo com ID {arquivo_id} não encontrado.")
    except Exception as e:
        if 'arquivo' in locals():
            arquivo.status = 'ERRO'
            arquivo.detalhes_erro = str(e)
            arquivo.data_fim_processamento = timezone.now()
            arquivo.save()
