from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import MultiPartParser, FormParser

import threading
from .models import Arquivo
from .serializers import ArquivoUploadSerializer, ArquivoStatusSerializer
from .tasks import process_file_task
# Imports para o Swagger
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi

class FileUploadView(APIView):
    parser_classes = (MultiPartParser, FormParser)

    @swagger_auto_schema(
        operation_summary="Upload de arquivo para processamento",
        operation_description="Recebe um arquivo .txt, salva, e dispara o pipeline de ETL de forma assíncrona. Retorna o ID para consulta de status.",
        manual_parameters=[
            openapi.Parameter(
                'file',
                openapi.IN_FORM,
                description="Arquivo .txt a ser processado",
                type=openapi.TYPE_FILE,
                required=True
            )
        ],
        responses={
            202: openapi.Response('Requisição aceita, processamento iniciado.', ArquivoUploadSerializer),
            400: 'Nenhum arquivo enviado.'
        }
    )
    def post(self, request, *args, **kwargs):
        uploaded_file = request.FILES.get('file')
        if not uploaded_file:
            return Response({"erro": "Nenhum arquivo enviado."}, status=status.HTTP_400_BAD_REQUEST)

        arquivo = Arquivo.objects.create(
            nome_arquivo=uploaded_file.name,
            arquivo_txt=uploaded_file
        )

        thread = threading.Thread(target=process_file_task, args=(arquivo.id,))
        thread.start()

        serializer = ArquivoUploadSerializer(arquivo)
        return Response(serializer.data, status=status.HTTP_202_ACCEPTED)

class FileStatusView(APIView):
    @swagger_auto_schema(
        operation_summary="Consulta de status do processamento",
        operation_description="Verifica o status de um arquivo em processamento a partir do seu ID.",
        manual_parameters=[
            openapi.Parameter('arquivo_id', openapi.IN_PATH, description="ID do arquivo retornado no upload.", type=openapi.TYPE_INTEGER, required=True)
        ],
        responses={
            200: openapi.Response('Status do arquivo.', ArquivoStatusSerializer),
            404: 'Arquivo não encontrado.'
        }
    )
    def get(self, request, arquivo_id, *args, **kwargs):
        try:
            arquivo = Arquivo.objects.get(id=arquivo_id)
            serializer = ArquivoStatusSerializer(arquivo)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Arquivo.DoesNotExist:
            return Response({"erro": "Arquivo não encontrado."}, status=status.HTTP_404_NOT_FOUND)
