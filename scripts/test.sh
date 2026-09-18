#!/usr/bin/env bash
# Chạy toàn bộ test bên trong container spark-master.
set -euo pipefail
cd "$(dirname "$0")/../docker"
docker compose exec -T spark-master python -m pytest /opt/app/tests -v "$@"
