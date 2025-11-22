# syntax=docker/dockerfile:1
FROM python:3.10-alpine
RUN apk add --no-cache curl
RUN pip install aiofiles fastapi uvicorn[standard]
COPY hashserver.py .
COPY hash_file_response.py .
ENV HASHSERVER_DIRECTORY /buffers
CMD uvicorn hashserver:app --port $HASHSERVER_PORT --host $HASHSERVER_HOST
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:$HASHSERVER_PORT/healthcheck || exit 1