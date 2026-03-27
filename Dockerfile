# syntax=docker/dockerfile:1.6
# AI Stock Scheduler - 独立定时任务调度服务
FROM python:3.11-slim-bookworm

WORKDIR /srv/app

# 设置时区
ENV TZ=Asia/Shanghai \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple/ \
    PIP_TRUSTED_HOST=mirrors.aliyun.com \
    PIP_TIMEOUT=300

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 安装系统依赖
RUN set -eux; \
    rm -rf /etc/apt/sources.list.d/*; \
    truncate -s 0 /etc/apt/sources.list; \
    printf "deb http://mirrors.aliyun.com/debian bookworm main\n" > /etc/apt/sources.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        build-essential \
        libxml2 libxml2-dev \
        libxslt1.1 libxslt1-dev \
        ca-certificates; \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 安装 Python 依赖
COPY requirements.txt ./
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

# 拷贝项目代码
COPY . .

EXPOSE 8001

# Gunicorn + Uvicorn Worker
CMD ["gunicorn", "app.main:app", \
     "--workers", "1", \
     "--worker-class", "uvicorn.workers.UvicornWorker", \
     "--bind", "0.0.0.0:8001", \
     "--preload", \
     "--timeout", "1200", \
     "--graceful-timeout", "120", \
     "--keep-alive", "75", \
     "--max-requests", "500", \
     "--worker-tmp-dir", "/dev/shm", \
     "--access-logfile", "-", \
     "--error-logfile", "-", \
     "--log-level", "info"]
