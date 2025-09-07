from rest_framework import serializers
from .models import Arquivo

class ArquivoUploadSerializer(serializers.ModelSerializer):
    class Meta:
        model = Arquivo
        fields = ['id', 'nome_arquivo', 'status']

class ArquivoStatusSerializer(serializers.ModelSerializer):
    tempo_processamento = serializers.DurationField(read_only=True)

    class Meta:
        model = Arquivo
        fields = [
            'id', 
            'nome_arquivo', 
            'status', 
            'data_recebimento', 
            'data_inicio_processamento', 
            'data_fim_processamento',
            'tempo_processamento',
            'detalhes_erro'
        ]
