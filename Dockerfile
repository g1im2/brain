# Brain Integration Service Dockerfile
# AutoTM三层金融交易系统集成协调服务

FROM python:3.11-slim as base

# 设置环境变量
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app
ENV TZ=Asia/Shanghai

# 安装系统依赖
RUN apt-get update && apt-get install -y --fix-missing curl && rm -rf /var/lib/apt/lists/*

# 构建阶段
FROM base as builder

WORKDIR /app

# 复制依赖文件
COPY requirements.txt .

# 升级pip并安装Python依赖
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --user -r requirements.txt

# 复制本地共享库
COPY external/econdb/ ./external/econdb/
COPY external/asyncron/ ./external/asyncron/

# 安装本地共享库
RUN cd external/econdb && pip install --no-cache-dir --user .
RUN cd external/asyncron && pip install --no-cache-dir --user .

# 复制服务代码
COPY . ./

# 运行阶段
FROM base as runtime

# 创建非root用户
RUN useradd --create-home --shell /bin/bash brain

WORKDIR /app

# 复制Python包
COPY --from=builder /root/.local /home/brain/.local

# 复制应用代码（从构建阶段复制）
COPY --from=builder --chown=brain:brain /app/*.py ./
COPY --from=builder --chown=brain:brain /app/adapters/ ./adapters/
COPY --from=builder --chown=brain:brain /app/coordinators/ ./coordinators/
COPY --from=builder --chown=brain:brain /app/handlers/ ./handlers/
COPY --from=builder --chown=brain:brain /app/managers/ ./managers/
COPY --from=builder --chown=brain:brain /app/monitors/ ./monitors/
COPY --from=builder --chown=brain:brain /app/routers/ ./routers/
COPY --from=builder --chown=brain:brain /app/routes/ ./routes/
COPY --from=builder --chown=brain:brain /app/scheduler/ ./scheduler/
COPY --from=builder --chown=brain:brain /app/validators/ ./validators/
COPY --from=builder --chown=brain:brain /app/initializers/ ./initializers/

# 创建必要的目录
RUN mkdir -p logs data config && chown -R brain:brain logs data config

# 设置环境变量
ENV PATH=/home/brain/.local/bin:$PATH
ENV PYTHONPATH=/app

# 切换到非root用户
USER brain

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8088/health || exit 1

# 暴露端口
EXPOSE 8088

# 启动命令
CMD ["python", "main.py"]
