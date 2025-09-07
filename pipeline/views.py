from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import MultiPartParser, FormParser

from .models import Arquivo
from .serializers import ArquivoUploadSerializer, ArquivoStatusSerializer
from .tasks import process_file_task

class FileUploadView(APIView):
    parser_classes = (MultiPartParser, FormParser)

    def post(self, request, *args, **kwargs):
        uploaded_file = request.FILES.get('file')
        if not uploaded_file:
            return Response({"error": "Nenhum arquivo enviado."}, status=status.HTTP_400_BAD_REQUEST)

        arquivo = Arquivo.objects.create(
            nome_arquivo=uploaded_file.name,
            arquivo_txt=uploaded_file
        )

        process_file_task(arquivo.id)

        serializer = ArquivoUploadSerializer(arquivo)
        return Response(serializer.data, status=status.HTTP_202_ACCEPTED)

class FileStatusView(APIView):
    def get(self, request, arquivo_id, *args, **kwargs):
        try:
            arquivo = Arquivo.objects.get(id=arquivo_id)
            serializer = ArquivoStatusSerializer(arquivo)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Arquivo.DoesNotExist:
            return Response({"error": "Arquivo não encontrado."}, status=status.HTTP_404_NOT_FOUND)
