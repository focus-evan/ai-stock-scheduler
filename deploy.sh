#!/bin/bash
# =============================================
# AI Stock Scheduler - 阿里云 ECS 部署脚本
# =============================================
set -e

echo "=========================================="
echo " AI Stock Scheduler 部署脚本"
echo "=========================================="

# 1. 检查 .env 配置
if [ ! -f ".env" ]; then
    echo "[ERROR] .env 文件不存在！"
    echo "请复制 .env.example 并填写配置："
    echo "  cp .env.example .env"
    echo "  vim .env"
    exit 1
fi

# 2. 检查 Docker
if ! command -v docker &> /dev/null; then
    echo "[ERROR] Docker 未安装"
    exit 1
fi

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "[ERROR] Docker Compose 未安装"
    exit 1
fi

# 3. 创建日志目录
mkdir -p logs

# 4. 构建镜像
echo "[INFO] 构建 Docker 镜像..."
docker compose build --no-cache

# 5. 启动服务
echo "[INFO] 启动调度服务..."
docker compose up -d

# 6. 检查健康
echo "[INFO] 等待服务启动..."
sleep 5

HEALTH=$(curl -s http://127.0.0.1:8001/health 2>/dev/null || echo "FAILED")
if echo "$HEALTH" | grep -q "ok"; then
    echo "[OK] 服务已启动！"
    echo "$HEALTH"
else
    echo "[WARNING] 健康检查失败，查看日志："
    docker compose logs --tail 50
fi

echo ""
echo "=========================================="
echo " 部署完成"
echo " API 地址: http://$(hostname -I | awk '{print $1}'):8001"
echo " 健康检查: curl http://localhost:8001/health"
echo " 任务状态: curl http://localhost:8001/api/scheduler/status"
echo " 查看日志: docker compose logs -f"
echo "=========================================="
