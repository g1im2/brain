# Integration Service

Integration Service 是三层金融交易系统的核心微服务，作为系统的"指挥中心"，负责协调和管理宏观战略系统、组合管理系统、个股战术系统和数据抓取服务之间的交互。

## 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                    Integration Service                       │
│                     (指挥中心)                               │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   aiohttp   │  │  asyncron   │  │     econdb          │  │
│  │  Web框架    │  │  定时任务   │  │   数据访问层        │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ 系统协调器  │  │ 信号路由器  │  │   数据流管理器      │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ 服务适配器  │  │ 监控告警    │  │   验证协调器        │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│   Macro     │  │ Portfolio   │  │ Execution   │  │  Flowhub    │
│ Service     │  │ Service     │  │ Service     │  │ Service     │
│ :8080       │  │ :8080       │  │ :8087       │  │ :8080       │
└─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘
```

## 技术栈

- **Web框架**: aiohttp - 高性能异步HTTP服务器
- **定时任务**: asyncron - 分布式任务调度器
- **数据访问**: econdb - 统一数据访问层
- **数据库**: TimescaleDB - 时序数据库
- **缓存**: Redis - 内存数据库
- **容器化**: Docker & Docker Compose

## 核心功能

### 1. 系统协调 (SystemCoordinator)
- 统一协调三层系统的分析周期
- 管理系统启动和关闭流程
- 监控系统整体健康状态
- 处理系统级异常和恢复

### 2. 信号路由 (SignalRouter)
- 管理和路由各系统间的交易信号
- 信号冲突检测和解决
- 信号优先级管理和验证
- 信号历史记录和统计分析

### 3. 数据流管理 (DataFlowManager)
- 优化和管理系统间的数据流
- 数据缓存和压缩优化
- 数据质量监控和同步协调
- 性能监控和优化建议

### 4. 定时任务调度 (IntegrationScheduler)
- 基于asyncron的定时任务管理
- 每日数据抓取任务调度
- 完整分析周期触发
- 系统健康检查任务

### 5. 服务适配器 (ServiceAdapters)
- HTTP客户端封装，与四个微服务通信
- 连接池管理和重试机制
- 服务健康检查和故障恢复
- 请求统计和性能监控

## API接口

### 健康检查
```http
GET /health                    # 基础健康检查
GET /api/v1/status            # 详细状态信息
GET /api/v1/info              # API信息
```

### 系统协调
```http
POST /api/v1/system/startup           # 启动系统
POST /api/v1/system/shutdown          # 关闭系统
GET  /api/v1/system/status            # 获取系统状态
POST /api/v1/system/analysis/trigger  # 触发分析周期
GET  /api/v1/system/analysis/history  # 获取分析历史
GET  /api/v1/system/resources         # 获取资源状态
```

### 服务管理
```http
GET  /api/v1/services                      # 获取服务列表
GET  /api/v1/services/{service}/status     # 获取服务状态
POST /api/v1/services/{service}/health-check # 健康检查
POST /api/v1/services/{service}/reconnect  # 重连服务
GET  /api/v1/services/{service}/config     # 获取服务配置
```

### 信号路由
```http
POST /api/v1/signals/route      # 路由信号
GET  /api/v1/signals/conflicts  # 获取信号冲突
POST /api/v1/signals/resolve    # 解决信号冲突
GET  /api/v1/signals/history    # 获取信号历史
GET  /api/v1/signals/stats      # 获取信号统计
POST /api/v1/signals/cleanup    # 清理过期信号
```

### 数据流管理
```http
GET  /api/v1/dataflow/status        # 数据流状态
POST /api/v1/dataflow/optimize      # 优化数据流
GET  /api/v1/dataflow/metrics       # 数据流指标
POST /api/v1/dataflow/cache/clear   # 清理缓存
GET  /api/v1/dataflow/cache/stats   # 缓存统计
POST /api/v1/dataflow/sync          # 触发数据同步
```

### 定时任务管理
```http
GET    /api/v1/tasks                    # 获取任务列表
POST   /api/v1/tasks                    # 创建定时任务
GET    /api/v1/tasks/{task_id}          # 获取任务详情
PUT    /api/v1/tasks/{task_id}          # 更新任务
DELETE /api/v1/tasks/{task_id}          # 删除任务
POST   /api/v1/tasks/{task_id}/trigger  # 手动触发任务
POST   /api/v1/tasks/{task_id}/toggle   # 启用/禁用任务
GET    /api/v1/tasks/{task_id}/history  # 获取任务历史
```

### 监控告警
```http
GET  /api/v1/monitoring/metrics      # 获取监控指标
GET  /api/v1/monitoring/alerts       # 获取告警信息
POST /api/v1/monitoring/alerts/ack   # 确认告警
GET  /api/v1/monitoring/performance  # 获取性能指标
GET  /api/v1/monitoring/health       # 获取健康状态
POST /api/v1/monitoring/rules        # 设置告警规则
GET  /api/v1/monitoring/rules        # 获取告警规则
```

## 配置管理

### 环境变量配置

```bash
# 应用配置
INTEGRATION_HOST=0.0.0.0
INTEGRATION_PORT=8088
INTEGRATION_DEBUG=false

# 服务发现配置
MACRO_SERVICE_URL=http://macro-service:8080
PORTFOLIO_SERVICE_URL=http://portfolio-service:8080
EXECUTION_SERVICE_URL=http://execution-service:8087
FLOWHUB_SERVICE_URL=http://flowhub-service:8080

# 数据库配置
DATABASE_URL=postgresql://postgres:password@timescaledb:5432/stock_data

# Redis配置
REDIS_URL=redis://redis:6379/0

# 定时任务配置
SCHEDULER_ENABLED=true
SCHEDULER_TIMEZONE=Asia/Shanghai

# 监控配置
MONITORING_ENABLED=true

# 日志配置
LOG_LEVEL=INFO
LOG_FILE=logs/integration.log
```

## 启动数据初始化功能（Brain → Flowhub）

为确保首次部署或重启后系统具备可用的基础数据，Brain 在服务启动后可异步触发一次“数据初始化”。该过程与 Flowhub 服务交互，按阶段批量拉取基础数据，且不阻塞对外 API。

- 初始化阶段顺序：股票基础 → 宏观核心 → 股票历史 → 其他宏观
- 并发控制：每阶段可配置并发度，避免对 Flowhub 造成瞬时压力
- 幂等与断点续传：进度持久化，重复启动不会重复初始化
- 失败重试：依赖服务不可用时指数退避重试，超过阈值后降级等待定时任务补齐

### 新增配置（环境变量）

```bash
# 是否在启动后自动触发初始化
INIT_DATA_ON_STARTUP=true|false  # 默认 true

# 初始化前等待的依赖（逗号分隔），目前支持 flowhub
INIT_WAIT_DEPENDENCIES=flowhub   # 默认 flowhub

# 首次运行是否允许最长历史范围（由 Flowhub 智能增量判断起点）
INIT_MAX_HISTORY_FIRST_RUN=true|false  # 默认 true

# 单阶段抓取并发度
INIT_CONCURRENCY=2  # 默认 2
```

> 以上环境变量映射到 config.ServiceConfig 中的对应字段，支持通过 docker-compose 或 k8s 注入。

### 运行与验证（本地/Compose）

```bash
# 单独启动 Brain（需要提供 FLOWHUB_SERVICE_URL 指向 Flowhub）
export FLOWHUB_SERVICE_URL=http://localhost:8080
export INIT_DATA_ON_STARTUP=true
python -m services.brain.main

# 或使用项目内的示例 compose（构建并启动 brain + flowhub）
docker compose -f docker-compose.init.yml up -d --build

# 健康与进度检查
curl -s http://localhost:8088/health
curl -s http://localhost:8088/api/v1/system/init/status | jq .

# 提供一个简单的冒烟脚本
scripts/init_smoke_test.sh
```

接口：
- GET /api/v1/system/init/status
  - 返回是否启用、当前进度、各阶段状态、错误信息等

### 部署注意事项
- 初始化过程为后台任务，不阻塞服务启动；首启建议在业务低峰进行
- 确保 Brain 容器具备写入 /data/brain（或项目配置的持久化目录）权限，用于存放 init_state.json
- 根据环境资源适当调小 INIT_CONCURRENCY（建议 2~4）
- 若不希望首启进行较长历史拉取，可设置 INIT_MAX_HISTORY_FIRST_RUN=false（仍与 Flowhub 智能增量兼容）

### 故障排除（初始化相关）
- INIT 状态一直未开始：检查 Brain 日志与 /health，确认是否已通过健康检查；检查 Flowhub /health
- INIT 状态失败：查看 status 中的 errors 列表；重启后会断点续传
- 环境变量未生效：检查大小写、是否被部署编排覆盖；也可通过 GET /api/v1/services/brain/config（若有）核验


## 快速开始

### 开发环境

1. **安装依赖**
```bash
pip install -r requirements.txt
```

2. **配置环境变量**
```bash
export INTEGRATION_DEBUG=true
export LOG_LEVEL=DEBUG
```

3. **启动服务**
```bash
python -m integration.main
```

4. **验证服务**
```bash
curl http://localhost:8088/health
```

### Docker部署

1. **构建镜像**
```bash
cd integration/
docker build -t integration-service .
```

2. **启动服务**
```bash
docker-compose up -d
```

3. **查看日志**
```bash
docker-compose logs -f integration-service
```

4. **健康检查**
```bash
curl http://localhost:8088/health
```

## 监控和运维

### 健康检查

```bash
# 基础健康检查
curl http://localhost:8088/health

# 详细状态信息
curl http://localhost:8088/api/v1/status

# 服务状态检查
curl http://localhost:8088/api/v1/services
```

### 日志管理

```bash
# 查看实时日志
docker-compose logs -f integration-service

# 查看错误日志
docker-compose logs integration-service | grep ERROR

# 日志文件位置
tail -f logs/integration.log
```

## 故障排除

### 常见问题

1. **服务启动失败**
   - 检查端口占用: `netstat -tlnp | grep 8088`
   - 检查配置文件: 验证YAML语法
   - 检查依赖服务: Redis、TimescaleDB是否正常

2. **外部服务连接失败**
   - 检查服务URL配置
   - 验证网络连通性: `curl http://macro-service:8080/health`
   - 检查防火墙设置

3. **定时任务不执行**
   - 检查调度器状态: `GET /api/v1/tasks`
   - 验证cron表达式
   - 查看任务执行历史

## 版本历史

- **v2.0.0**: 微服务化改造
  - 基于aiohttp的Web服务框架
  - 集成asyncron定时任务管理
  - HTTP服务适配器重构
  - Docker容器化部署
  - 完整的API接口设计

## 许可证

MIT License
