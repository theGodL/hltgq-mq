#!/bin/bash
# ============================================================
# hltgq-mq 部署脚本（银河麒麟 ARM64 服务器上执行）
# ============================================================
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
IMAGE_NAME="hltgq-mq"
CONTAINER_NAME="hltgq-mq"

# 短信告警回调 token（hltgq-site 提供）：部署前在服务器当前 shell 先 export，例如
#   export APP_SMS_ALARM_EVENT_TOKEN='<site提供的token>'   # 含 # 时必须加引号
# 未 export 时容器照常启动，仅短信回调跳过（启动日志 WARN）
SMS_TOKEN="${APP_SMS_ALARM_EVENT_TOKEN:-}"

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
    -e "APP_SMS_ALARM_EVENT_TOKEN=${SMS_TOKEN}" \
    --memory="1g" \
    ${IMAGE_NAME}:latest

echo "部署完成，查看日志："
docker logs -f ${CONTAINER_NAME}
