#!/usr/bin/env bash
# One-time setup on Ubuntu 22.04+ or Amazon Linux 2023: Docker, Compose v2.29.7, Python venv.
set -euo pipefail
COMPOSE_VERSION="${COMPOSE_VERSION:-v2.29.7}"
case "$(uname -m)" in x86_64) ARCH=x86_64 ;; aarch64) ARCH=aarch64 ;; *) echo "Unsupported CPU" >&2; exit 1 ;; esac

if command -v apt-get >/dev/null 2>&1; then
  sudo apt-get update -y && sudo apt-get install -y docker.io curl jq python3 python3-venv python3-pip git
elif command -v dnf >/dev/null 2>&1; then
  sudo dnf install -y docker curl jq python3 python3-pip git
else
  echo "Unsupported OS: install docker, curl, jq, python3 manually." >&2; exit 1
fi
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"
if ! docker compose version >/dev/null 2>&1; then
  sudo mkdir -p /usr/local/lib/docker/cli-plugins
  sudo curl -fsSL -o /usr/local/lib/docker/cli-plugins/docker-compose "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-${ARCH}"
  sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
fi

cd "$(dirname "$0")/.."
python3 -m venv .venv
.venv/bin/pip install -q -r requirements-dev.txt
sudo docker compose version
echo "Setup complete. IMPORTANT: run 'newgrp docker' (or log out and back in) so docker works without sudo."
