# Brain 服务（集成协调层）

## 作用
Brain 是系统编排中枢，负责聚合与调度 `macro`、`portfolio`、`execution`、`flowhub` 等服务，提供统一的健康检查与集成视图。

## 依赖
- TimescaleDB（存储）
- Redis（缓存与任务状态）
- 其他服务：macro / portfolio / execution / flowhub

## 运行方式
### Docker（推荐）
```bash
docker-compose up -d brain
```

### 本地启动
```bash
cd services/brain
pip install -r requirements.txt
python main.py
```

## 端口
- HTTP: `8088`

## 配置
- Docker 环境变量以 `docker-compose.yml` 为准
- 配置目录：`config/brain`
- 主要依赖服务地址：`MACRO_SERVICE_URL`、`PORTFOLIO_SERVICE_URL`、`EXECUTION_SERVICE_URL`、`FLOWHUB_SERVICE_URL`

## 健康检查
- `GET /health`
