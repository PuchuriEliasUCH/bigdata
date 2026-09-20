#!/bin/sh
set -eu

# The scheduler is PID 1; Jupyter runs alongside it so the same service is the
# reproducible Spark runtime and the notebook environment.
mkdir -p "${BRONZE_PATH:-/data/bronze}" "${SILVER_PATH:-/data/silver}"
jupyter lab \
  --ip=0.0.0.0 \
  --port=8888 \
  --no-browser \
  --allow-root \
  --ServerApp.token='' \
  --ServerApp.password='' \
  >/tmp/jupyter.log 2>&1 &

exec /usr/local/bin/supercronic /app/scheduler/pipeline.cron
