from django.db import models

class Arquivo(models.Model):
    STATUS_CHOICES = [
        ('PENDENTE', 'Pendente'),
        ('PROCESSANDO', 'Processando'),
        ('CONCLUIDO', 'Concluído'),
        ('ERRO', 'Erro'),
    ]

    nome_arquivo = models.CharField(max_length=255)
    arquivo_txt = models.FileField(upload_to='uploads/')
    data_recebimento = models.DateTimeField(auto_now_add=True)
    data_inicio_processamento = models.DateTimeField(null=True, blank=True)
    data_fim_processamento = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDENTE')
    detalhes_erro = models.TextField(null=True, blank=True)

    class Meta:
        db_table = 'rst"."arquivo'

    def __str__(self):
        return f"{self.id} - {self.nome_arquivo} ({self.status})"

    @property
    def tempo_processamento(self):
        if self.data_inicio_processamento and self.data_fim_processamento:
            return self.data_fim_processamento - self.data_inicio_processamento
        return None
