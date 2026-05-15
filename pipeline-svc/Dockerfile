# pipeline-svc — FastAPI sidecar for PeCause (Linux/amd64 & arm64)
#
# Build (from repo root):
#   docker build -t pecause-pipeline-svc:latest pipeline-svc
FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIPELINE_DATA_DIR=/data/pipeline

# OCR 回退（pytesseract）与健康检查用 curl
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    tesseract-ocr \
    tesseract-ocr-chi-sim \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --break-system-packages uv

# 配置 uv 和 pip 使用国内镜像（服务器无法访问 pypi.org）
ENV UV_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ENV UV_EXTRA_INDEX_URL=https://mirrors.aliyun.com/pypi/simple/
ENV PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ENV PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn

COPY pyproject.toml uv.lock ./
COPY server ./server
COPY vendor ./vendor

RUN uv sync --frozen --no-dev

# 创建启动脚本，直接使用 .venv 中的 Python 完全绕过 uv 的依赖解析
RUN printf '#!/bin/sh\nexec /app/.venv/bin/python -m uvicorn server.main:app --host 0.0.0.0 --port 8001\n' \
    > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

EXPOSE 8001

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8001/api/pipeline/health || exit 1

# 直接使用 .venv/bin/python，完全不经过 uv run，零网络请求
CMD ["/app/entrypoint.sh"]
