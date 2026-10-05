FROM python:3.11-slim

# verify 镜像同时携带应用代码，用于以子进程冷启动方式验证重启恢复。
WORKDIR /verify
COPY web/app/ /app/
COPY verify/verify.py /verify/verify.py

ENV BASE_URL=http://web:8080 \
    APP_PATH=/app/server.py \
    SOURCE_DATA_DIR=/data \
    RESTART_ROOT=/tmp/restart \
    PYTHONUNBUFFERED=1

CMD ["python", "/verify/verify.py"]
