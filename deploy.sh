#!/bin/bash
# =============================================
# AI Stock Scheduler - 一键部署脚本
# 用法: ./deploy.sh
# =============================================
set -e

echo "=========================================="
echo " AI Stock Scheduler 部署"
echo " $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="

# 1. 检查 .env
if [ ! -f ".env" ]; then
    echo "[ERROR] .env 文件不存在！请先配置："
    echo "  cp .env.example .env && vim .env"
    exit 1
fi

# 2. 检测 docker-compose 版本
if docker compose version &> /dev/null; then
    DC="docker compose"
elif command -v docker-compose &> /dev/null; then
    DC="docker-compose"
else
    echo "[ERROR] Docker Compose 未安装"
    exit 1
fi

# 3. 拉取最新代码
echo ""
echo "[1/4] 拉取最新代码..."
git pull origin main
echo ""

# 4. 构建镜像
echo "[2/4] 构建 Docker 镜像..."
$DC build
echo ""

# 5. 停掉旧容器 + 启动新容器
echo "[3/4] 重启服务..."
$DC rm -f -s scheduler 2>/dev/null || true
$DC up -d
echo ""

# 6. 等待启动 + 查看日志
echo "[4/4] 等待启动..."
sleep 3

# 健康检查
HEALTH=$(curl -s http://127.0.0.1:8001/health 2>/dev/null || echo "")
if echo "$HEALTH" | grep -q "ok"; then
    echo "=========================================="
    echo " ✅ 部署成功！"
    echo " 健康检查: $HEALTH"
    echo "=========================================="
else
    echo "=========================================="
    echo " ⚠️  服务启动中，请查看日志确认..."
    echo "=========================================="
fi

echo ""
echo "进入日志查看（Ctrl+C 退出日志，服务不会停止）："
echo "------------------------------------------"
$DC logs -f --tail 100
