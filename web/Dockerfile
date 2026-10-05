FROM python:3.11-slim

# 纯标准库应用，无需安装第三方依赖。
WORKDIR /app
COPY app/ /app/

ENV SEAL_HOST=0.0.0.0 \
    SEAL_PORT=8080 \
    DATA_DIR=/data \
    PYTHONUNBUFFERED=1

EXPOSE 8080
VOLUME ["/data"]

HEALTHCHECK --interval=3s --timeout=3s --start-period=5s --retries=20 \
  CMD python -c "import json,urllib.request,sys; r=urllib.request.urlopen('http://127.0.0.1:8080/health',timeout=3); sys.exit(0 if r.status==200 and json.load(r)['status']=='ok' else 1)"

CMD ["python", "/app/server.py"]
