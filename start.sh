#!/bin/bash
set -e
cd "$(dirname "$0")"

python3 -c 'import sys; sys.exit(sys.version_info < (3, 9))' \
  || { echo "нужен python 3.9+"; exit 1; }

[ -d .venv ] || python3 -m venv .venv
.venv/bin/pip install -qU pip
.venv/bin/pip install -q -r requirements.txt

if [ "${1:-}" = --setup-only ]; then
  exit 0
fi

docker compose up -d

for _ in $(seq 1 30); do
  curl -sf http://localhost:9090 >/dev/null && break
  sleep 1
done

.venv/bin/python scripts/seed_s3.py

# если source ./start.sh — подхватить venv в текущей сессии
if [ "$0" != "${BASH_SOURCE[0]}" ]; then
  case "$SHELL" in
    *fish*) echo "source .venv/bin/activate.fish" ;;
    *) source .venv/bin/activate ;;
  esac
fi
