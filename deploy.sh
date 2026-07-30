#!/bin/bash
# ============================================================
# hltgq-mq 部署脚本（银河麒麟 ARM64 服务器上执行）
# ============================================================
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
IMAGE_NAME="hltgq-mq"
CONTAINER_NAME="hltgq-mq"

docker stop ${CONTAINER_NAME} 2>/dev/null || true
docker rm ${CONTAINER_NAME} 2>/dev/null || true
docker build -t ${IMAGE_NAME}:latest "$PROJECT_DIR"
docker run -d \
    --name ${CONTAINER_NAME} \
    --restart unless-stopped \
    -p 8081:8080 \
    -v /service/hltgq/logs/hltgq-mq:/app/logs \
    -e TZ=Asia/Shanghai \
    -e SPRING_RABBITMQ_HOST=10.68.18.8 \
    -e SPRING_RABBITMQ_PORT=5672 \
    -e SPRING_RABBITMQ_USERNAME=sunny \
    -e "SPRING_RABBITMQ_PASSWORD=sunny@2025" \
    -e SPRING_RABBITMQ_VIRTUAL_HOST=/ \
    --memory="512m" \
    ${IMAGE_NAME}:latest

echo "部署完成，查看日志："
docker logs -f ${CONTAINER_NAME}
