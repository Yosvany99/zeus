# ZEUS — Asistente de Voz Personal

Asistente de voz con acceso real al sistema VPS. STT offline (Vosk) → Claude Code CLI → TTS (edge-tts).

## Estructura

```
zeus/
├── main.py              # FastAPI backend
├── requirements.txt
├── .env.example
├── setup.sh             # Instalación
├── nginx.conf           # Config nginx (HTTPS)
├── zeus.service         # Systemd service
├── start-tunnel.sh      # Tunnel temporal (Cloudflare)
├── models/
│   └── vosk-es/         # Modelo STT offline español
└── client/
    ├── index.html       # UI móvil
    ├── manifest.json
    ├── sw.js
    └── icon*.svg
```

## Instalación en VPS

```bash
cd /home/axel/zeus
chmod +x setup.sh
sudo ./setup.sh
```

## Configuración

```bash
cp .env.example .env
nano .env
# API_KEY=tu-clave-secreta
# CLAUDE_BIN=/home/axel/.local/bin/claude  (opcional, es el default)
```

## Ejecutar (desarrollo)

```bash
source venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## Producción con systemd

```bash
# Nginx
sudo cp nginx.conf /etc/nginx/sites-available/zeus
sudo ln -s /etc/nginx/sites-available/zeus /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx

# HTTPS
sudo certbot --nginx -d tu-dominio.com

# Servicio
sudo cp zeus.service /etc/systemd/system/
sudo systemctl enable --now zeus
sudo systemctl status zeus
```

## API (todas requieren header `X-API-Key`)

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/` | GET | UI móvil |
| `/estado` | GET | `{"estado": "ACTIVO\|INACTIVO"}` |
| `/activar` | POST | Activa ZEUS |
| `/desactivar` | POST | Desactiva + limpia historial |
| `/voz` | POST | Procesa audio → respuesta por voz |
| `/siguiente/{id}` | GET | Stream de chunks de audio |
| `/tarea-actual` | GET | Task en curso (reconexión) |

## Flujo de voz

1. Abre `https://tu-dominio.com` en el móvil
2. Configura URL y API Key (se guardan en localStorage)
3. Pulsa **ACTIVAR ZEUS**
4. Mantén el botón del micrófono → habla → suelta (o usa modo LLAMADA con VAD)
5. Di **"Gracias"** → se desactiva

## Notas

- **HTTPS obligatorio** para micrófono en móvil
- STT: Vosk offline primero, Google como fallback
- Sin dominio: `./start-tunnel.sh` para URL temporal via Cloudflare
