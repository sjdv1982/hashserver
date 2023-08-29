# syntax=docker/dockerfile:1
FROM python:3.10-alpine
RUN pip install fastapi uvicorn[standard]
COPY hashserver.py .
COPY hash_file_response.py .
ENV HASHSERVER_DIRECTORY /buffers
CMD uvicorn hashserver:app --port $HASHSERVER_PORT --host $HASHSERVER_HOST
