#!/bin/bash
set -e
AT=$(echo QA== | base64 -d)
NODO1="rabbit${AT}rabbit1"

docker-entrypoint.sh rabbitmq-server &

until rabbitmqctl await_startup >/dev/null 2>&1; do
  sleep 3
done

if ! rabbitmqctl cluster_status 2>/dev/null | grep -q "rabbit1"; then
  rabbitmqctl stop_app
  rabbitmqctl reset
  rabbitmqctl join_cluster "$NODO1"
  rabbitmqctl start_app
fi

rabbitmqctl set_policy ha-all ".*" '{"ha-mode":"all","ha-sync-mode":"automatic"}' --apply-to queues

wait