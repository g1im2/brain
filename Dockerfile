# Integration Service Dockerfile
# 三层金融交易系统集成服务

FROM python:3.11-slim as base

# 设置环境变量
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    git \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 构建阶段
FROM base as builder

WORKDIR /app

# 复制依赖文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir --user -r requirements.txt

# 复制项目文件
COPY . .

# 安装子模块依赖
# 安装asyncron子模块
RUN if [ -d "libs/asyncron" ]; then \
        pip install --no-cache-dir --user ./libs/asyncron/; \
    fi

# 安装econdb子模块（如果存在）
RUN if [ -d "econdb" ]; then \
        pip install --no-cache-dir --user ./econdb/; \
    fi

# 运行阶段
FROM base as runtime

# 创建非root用户
RUN useradd --create-home --shell /bin/bash integration

WORKDIR /app

# 复制Python包
COPY --from=builder /root/.local /home/integration/.local

# 复制应用代码
COPY --chown=integration:integration integration/ ./integration/
COPY --chown=integration:integration config/ ./config/

# 创建必要的目录
RUN mkdir -p logs data && chown -R integration:integration logs data

# 设置环境变量
ENV PATH=/home/integration/.local/bin:$PATH
ENV PYTHONPATH=/app

# 切换到非root用户
USER integration

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8088/health || exit 1

# 暴露端口
EXPOSE 8088

# 启动命令
CMD ["python", "-m", "integration.main"]
