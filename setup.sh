#!/usr/bin/env bash
set -euo pipefail

echo "=== ZEUS Setup ==="

# System deps
echo "[1/3] Instalando dependencias del sistema..."
apt-get update -q && apt-get install -y -q ffmpeg python3-pip python3-venv xvfb x11vnc novnc

# Python venv
echo "[2/3] Creando entorno virtual..."
python3 -m venv venv
source venv/bin/activate

# Python deps
echo "[3/4] Instalando dependencias Python..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

# Playwright browser
echo "[4/4] Instalando Chromium para Playwright..."
playwright install chromium --with-deps

# .env
if [ ! -f ".env" ]; then
  cp .env.example .env
  echo ""
  echo ">>> Edita .env y añade tu API_KEY <<<"
fi

echo ""
echo "=== Setup completo ==="
echo "Pasos siguientes:"
echo "  1. nano .env  (añadir API_KEY)"
echo "  2. source venv/bin/activate"
echo "  3. uvicorn main:app --host 0.0.0.0 --port 8000"
echo ""
echo "  Producción con systemd:"
echo "  4. sudo cp zeus.service /etc/systemd/system/"
echo "  5. sudo systemctl enable --now zeus"
