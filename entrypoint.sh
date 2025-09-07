#!/bin/sh

# Aborta o script se qualquer comando falhar
set -e

echo "Aplicando migrações do banco de dados..."
python manage.py migrate

echo "Iniciando o servidor Django..."
python manage.py runserver 0.0.0.0:8000