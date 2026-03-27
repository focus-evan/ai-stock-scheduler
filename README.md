# AI Stock Scheduler

独立部署的定时任务调度服务，从 `ai-stock` 项目中拆分出来。

## 架构

```
ai-stock (主服务 :8000)           ai-stock-scheduler (调度服务 :8001)
┌─────────────────────┐          ┌─────────────────────┐
│  前端 API 路由       │  HTTP    │  定时任务调度器       │
│  portfolio_endpoints │ ──────> │  portfolio_scheduler  │
│  scheduler_proxy     │         │  portfolio_manager    │
│                     │          │  portfolio_llm        │
│  数据查询 (只读)     │          │  10个策略模块         │
│  portfolio_repo      │          │  结算 & 复盘         │
└─────────────────────┘          └─────────────────────┘
         │                                │
         └──────── MySQL / Redis ─────────┘
```

## 快速开始

### 1. 配置环境变量
```bash
cp .env.example .env
vim .env  # 填写 MySQL、Redis、LLM 等配置
```

### 2. 本地开发
```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
```

### 3. Docker 部署 (阿里云 ECS)
```bash
chmod +x deploy.sh
./deploy.sh
```

### 4. 在 ai-stock 中启用转发
在 `ai-stock/.env` 中添加：
```env
SCHEDULER_URL=http://<scheduler-ip>:8001
SCHEDULER_PROXY_ENABLED=true
```

## API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 健康检查 |
| GET | `/api/scheduler/status` | 调度器状态 |
| POST | `/api/scheduler/recommendations` | 触发推荐生成 |
| POST | `/api/scheduler/trade` | 触发交易决策 |
| POST | `/api/scheduler/rebalance/{id}` | 触发调仓 |
| POST | `/api/scheduler/follow/{id}` | 触发跟投 |
| POST | `/api/scheduler/settle` | 触发结算 |
| POST | `/api/scheduler/review/{id}` | 触发复盘 |
| POST | `/api/scheduler/review-all` | 触发所有复盘 |
| POST | `/api/scheduler/watchlist-guidance` | 触发盯盘指导 |

## 定时任务时间表

### 凌晨预生成
- 01:00 突破战法
- 02:30 量价关系
- 04:00 均线战法
- 05:30 趋势动量

### 盘中交易
- 09:25-09:40 隔夜施工法推荐 (集合竞价)
- 09:40-09:55 事件驱动推荐
- 09:45-10:05 趋势动量交易
- 09:55-10:20 事件驱动交易
- 10:00-10:15 龙头战法推荐
- 10:15-10:40 龙头/突破交易
- 10:30-10:45 情绪战法推荐
- 10:45-11:35 情绪/量价/均线交易
- 13:00-14:45 下午推荐更新 + 交易
- 14:45-14:58 隔夜/综合战法尾盘交易

### 收盘
- 15:05-16:00 结算
- 16:00-16:30 复盘
