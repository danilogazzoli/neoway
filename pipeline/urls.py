from django.urls import path
from .views import FileUploadView, FileStatusView

urlpatterns = [
    path('upload/', FileUploadView.as_view(), name='file-upload'),
    path('status/<int:arquivo_id>/', FileStatusView.as_view(), name='file-status'),
]
