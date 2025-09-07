FROM python:3.11-alpine

WORKDIR /app

# Instala dependências do sistema necessárias para psycopg2
RUN apk add --no-cache gcc musl-dev postgresql-dev

COPY entrypoint.sh /entrypoint.sh
COPY requirements.txt /app/requirements.txt

# Forçar quebras de linha no formato Unix (LF) e não Windows (CRLF)
RUN sed -i 's/\r$//' /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN pip install --no-cache-dir --upgrade pip
RUN pip install -r /app/requirements.txt

# Cria usuário não-root
RUN adduser -D appuser

# Altera para o usuário não-root
USER appuser

ENTRYPOINT ["/entrypoint.sh"]