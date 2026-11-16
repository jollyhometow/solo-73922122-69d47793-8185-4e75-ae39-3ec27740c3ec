FROM python:3.11-slim

# 不写 .pyc、日志直出
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    WORK_DIR=/tmp/recon-work

WORKDIR /app

# 先装依赖，利用镜像层缓存；psycopg[binary] 自带 libpq，无需系统编译链
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && groupadd --system recon && useradd --system --gid recon --no-create-home recon \
    && mkdir -p /tmp/recon-work && chown recon:recon /tmp/recon-work

COPY app ./app

USER recon
EXPOSE 8000

# 单 worker：任务名额是进程内信号量，多 worker 会破坏“一次一组”语义
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
