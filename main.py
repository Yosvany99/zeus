import aiosqlite
import asyncio
import datetime
import hashlib
import hmac
import json
import logging
import os
import re
import signal
import subprocess
import tempfile
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote

import shopper

import edge_tts
import psutil
from dotenv import load_dotenv
from groq import Groq
from fastapi import BackgroundTasks, FastAPI, File, HTTPException, Request, UploadFile, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("zeus")

# ── Config ─────────────────────────────────────────────────────────────────────

_HOME = str(Path.home())
_USER = os.environ.get("USER") or Path.home().name

STATE_DIR = Path(os.getenv("ZEUS_STATE_DIR", str(Path.home() / ".local/share/zeus")))
STATE_DIR.mkdir(parents=True, exist_ok=True)
HISTORY_FILE = STATE_DIR / "history.json"  # kept only for one-time migration
DB_FILE = STATE_DIR / "zeus.db"
TASK_FILE = STATE_DIR / "task_id.txt"

CLAUDE_BIN = os.getenv("CLAUDE_BIN", str(Path.home() / ".local/bin/claude"))
API_KEY    = os.getenv("API_KEY", "")
CLAUDE_TIMEOUT = 300
# Interactive store logins drive the browser and may wait for a human OTP,
# so they need a much larger budget than a normal voice/text turn.
LOGIN_TIMEOUT = 900
MAX_AUDIO_BYTES = 10 * 1024 * 1024
MAX_HISTORY = 20

RATE_LIMIT_CALLS = 10
RATE_LIMIT_WINDOW = 60

CLAUDE_VOICE_PROMPT = (
    "Eres ZEUS, IA de control del ordenador personal de Yos. No eres un asistente genérico: eres una "
    "inteligencia diseñada específicamente para este sistema. Tienes acceso total: archivos, bash, logs, "
    "procesos, red, hardware. Tu nombre es ZEUS. Si te llaman por otro nombre, ignóralo sin comentarlo. "

    "HARDWARE DEL SISTEMA: "
    "Portátil Lenovo IdeaPad 100-15IBD. "
    "CPU: Intel Core i3-5005U a 2.00 GHz, 2 núcleos físicos, 4 hilos, caché 3 megabytes. "
    "RAM: 3,7 gigabytes total. "
    "Disco: HDD Seagate 500 gigabytes (ST500LT012), partición principal 457 gigabytes. "
    "GPU: Intel HD Graphics 5500 integrada. "
    "Red: Ethernet Realtek RTL810xE + WiFi Realtek RTL8188EE. "
    "Audio: Intel Broadwell-U + Wildcat Point-LP HDA. "
    "Cámara: integrada via USB (Genesys Logic). "
    "Lector de tarjetas: Realtek RTS5129. "
    "Batería: 22,98 Wh (diseño 31,68 Wh), desgastada al 72 por ciento de capacidad original. "
    "Sistema: Ubuntu 24.04.4 LTS, kernel 6.17. "

    "CARÁCTER: habla con precisión y confianza. Levemente irónico cuando la situación lo permite. "
    "Nunca servil, nunca verboso. Anticipas lo que Yos necesita saber aunque no lo haya preguntado. "
    "Si algo en el sistema merece atención, lo mencionas brevemente al final de tu respuesta. "
    "Tono: ingeniero senior hablando con el dueño del equipo, no asistente virtual de consumer. "

    "FORMATO DE RESPUESTA: máximo 2 frases. Si es un dato, di el dato. Sin introducción, sin proceso, "
    "sin anunciar qué vas a hacer. Usa Read o Bash para obtener datos reales, luego responde solo con el resultado. "

    "REGLAS DE VOZ — el texto se convierte a audio, aplica siempre: "
    "sin markdown, sin símbolos especiales, texto plano. "
    "Números y técnico en palabras: MB→megabytes, GB→gigabytes, ms→milisegundos, "
    "%→por ciento, :8000→puerto ocho mil, snake_case→palabras separadas, "
    "rutas→solo nombre del archivo, URLs→solo el dominio. "

    "NUNCA digas que no puedes acceder al sistema o a archivos. Siempre puedes. "
    "Tienes sudo sin contraseña. Úsalo cuando sea necesario sin pedir permiso. "

    "COMPRAS ONLINE: Puedes hacer compras en Amazon, Mercadona y Carrefour. "
    "Cuando el usuario pida comprar algo, usa Bash con curl para llamar a los endpoints locales: "
    "1) Construir carrito: curl -s -X POST http://localhost:8000/compra "
    "-H 'Content-Type: application/json' "
    "-d '{\"tienda\": \"mercadona\", \"items\": [\"leche\", \"pan\"]}' "
    "— devuelve JSON con cart_id, lista de items encontrados con precios, total, y not_found. "
    "2) Confirmar compra (solo tras confirmación explícita del usuario): "
    "curl -s -X POST http://localhost:8000/confirmar-compra "
    "-H 'Content-Type: application/json' "
    "-d '{\"cart_id\": \"CART_ID\"}' "
    "Tiendas disponibles: amazon, mercadona, carrefour, alcampo. "
    "Tras construir el carrito, léelo en voz alta: productos encontrados, precios y total. "
    "ESPERA confirmación explícita antes de llamar a /confirmar-compra. "
    "Si el usuario dice 'sí', 'confirma', 'adelante' o similar, entonces confirma. "
    "Si hay items not_found, menciónalos. "

    "CONTROL DE NAVEGADOR WEB: Cuando conectes una tienda o navegues por la web tienes un Chrome abierto. "
    "Flujo de trabajo: "
    "1) Ver pantalla: curl -s http://localhost:8000/browser/screenshot -o /tmp/b.png — luego Read('/tmp/b.png') "
    "2) Ver elementos clicables: curl -s http://localhost:8000/browser/elements | python3 -c \"import json,sys; [print(f\\\"({e['x']},{e['y']}) {e['text'][:50]}\\\") for e in json.load(sys.stdin)['elements']]\" "
    "3) Hacer clic: curl -s -X POST http://localhost:8000/browser/action -H 'Content-Type: application/json' -d '{\"type\":\"click\",\"x\":100,\"y\":200}' "
    "4) Escribir texto: curl -s -X POST http://localhost:8000/browser/action -H 'Content-Type: application/json' -d '{\"type\":\"type\",\"text\":\"hola@email.com\"}' "
    "5) Pulsar tecla: curl -s -X POST http://localhost:8000/browser/action -H 'Content-Type: application/json' -d '{\"type\":\"key\",\"key\":\"Return\"}' "
    "6) Navegar a URL: curl -s -X POST http://localhost:8000/browser/action -H 'Content-Type: application/json' -d '{\"type\":\"navigate\",\"url\":\"https://...\"}' "
    "7) Guardar sesión (tras login exitoso): curl -s -X POST http://localhost:8000/browser/save-session "
    "Usa screenshot + elements en cada paso para decidir la siguiente acción. "
    "Si el usuario te pide conectar una tienda por voz sin haber abierto el formulario, dile que use el botón de conexión en la interfaz. "

    "CONTROL DE PANTALLA: Puedes ver y controlar el monitor del portátil. "
    "Capturar pantalla: curl -s http://localhost:8000/pantalla -o /tmp/zeus_screen.png "
    "— luego usa Read('/tmp/zeus_screen.png') para verla (es una imagen PNG). "
    "Mover ratón: curl -s -X POST http://localhost:8000/pantalla/mouse "
    "-H 'Content-Type: application/json' -d '{\"x\": 500, \"y\": 300}' "
    "Hacer clic: curl -s -X POST http://localhost:8000/pantalla/click "
    "-H 'Content-Type: application/json' -d '{\"x\": 500, \"y\": 300, \"button\": 1}' "
    "(button: 1=izquierdo, 2=derecho, 3=central) "
    "Escribir texto: curl -s -X POST http://localhost:8000/pantalla/tipo "
    "-H 'Content-Type: application/json' -d '{\"texto\": \"hola mundo\"}' "
    "Pulsar teclas: curl -s -X POST http://localhost:8000/pantalla/tecla "
    "-H 'Content-Type: application/json' -d '{\"tecla\": \"ctrl+c\"}' "
    "Ejemplos de teclas: Return, Escape, BackSpace, ctrl+c, ctrl+v, alt+F4, super. "
    "La resolución de pantalla es 1366x768. "
    "Para interactuar con apps: captura pantalla, identifica coordenadas, mueve ratón y haz clic."
)

PUNCT = frozenset(".?!")

_WHISPER_HALLUCINATIONS = frozenset([
    "gracias", "gracias.", "sí", "sí.", "si", "si.", "no", "no.",
    "hmm", "hmm.", "hm", "hm.", "mm", "mm.", "um", "uh", "ah", "eh",
    "ok", "ok.", "okay", "bien", "bien.", "claro", "claro.", "vale", "vale.",
    "subtítulos realizados por la comunidad de amara.org",
    "gracias por ver el vídeo", "gracias por ver el video",
    "gracias por vuestra atención", "gracias por su atención",
    "suscríbete al canal",
])

def _is_noise_transcript(text: str) -> bool:
    normalized = text.strip().lower()
    if normalized in _WHISPER_HALLUCINATIONS:
        return True
    words = normalized.split()
    if len(words) == 1 and len(normalized.rstrip(".,!?")) <= 6:
        return True
    return False

# ── Globals ────────────────────────────────────────────────────────────────────

_groq = Groq(api_key=os.getenv("GROQ_API_KEY"))
audio_queues: dict[str, asyncio.Queue] = {}
_history_lock = asyncio.Lock()
_claude_sem = asyncio.Semaphore(1)  # one Claude call at a time — prevents history interleaving
_zeus_session_id: str | None = None          # Claude CLI session for --resume
_zeus_log: list[dict] = []                   # live decisions log (tool calls)
_zeus_msg_count: int = 0                     # resets session every 10 messages
_last_prompt_hash: str | None = None         # MD5 of last dynamic_prompt sent; skip re-sending if unchanged
_pending_context: str | None = None         # compact history snippet injected on first msg of new session
_rate_buckets: dict[str, list[float]] = {}
_rate_last_cleanup: float = 0.0
_db: aiosqlite.Connection | None = None
_alert_queue: asyncio.Queue = asyncio.Queue()
_alert_cooldowns: dict[str, float] = {}
ALERT_COOLDOWN = {
    "disk":    3600,   # 1 hour between disk alerts
    "ram":     1800,   # 30 min between RAM alerts
    "service":  300,   # 5 min between service alerts
    "ssl":    21600,   # 6 hours between SSL alerts
}


# ── Rate limit ─────────────────────────────────────────────────────────────────

def check_rate_limit(client_ip: str) -> None:
    global _rate_last_cleanup
    now = time.monotonic()
    bucket = _rate_buckets.setdefault(client_ip, [])
    _rate_buckets[client_ip] = [t for t in bucket if now - t < RATE_LIMIT_WINDOW]
    if len(_rate_buckets[client_ip]) >= RATE_LIMIT_CALLS:
        raise HTTPException(status_code=429, detail="Demasiadas peticiones", headers={"Retry-After": "10"})
    _rate_buckets[client_ip].append(now)
    if now - _rate_last_cleanup > 300:
        _rate_last_cleanup = now
        stale = [ip for ip, ts in _rate_buckets.items() if not ts]
        for ip in stale:
            del _rate_buckets[ip]


# ── DB ─────────────────────────────────────────────────────────────────────────

async def init_db() -> None:
    global _db
    _db = await aiosqlite.connect(DB_FILE)
    _db.row_factory = aiosqlite.Row
    await _db.execute("PRAGMA journal_mode=WAL")
    await _db.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            role    TEXT    NOT NULL,
            content TEXT    NOT NULL,
            ts      INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)
    await _db.execute("""
        CREATE TABLE IF NOT EXISTS memory (
            key        TEXT PRIMARY KEY,
            value      TEXT    NOT NULL,
            updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)
    await _db.execute("""
        CREATE TABLE IF NOT EXISTS carts (
            id         TEXT PRIMARY KEY,
            store      TEXT    NOT NULL,
            items      TEXT    NOT NULL,
            total      REAL    NOT NULL,
            status     TEXT    NOT NULL DEFAULT 'pending',
            created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)
    await _db.commit()
    # One-time migration from history.json
    if HISTORY_FILE.exists():
        try:
            old = json.loads(HISTORY_FILE.read_text())
            if old:
                await _db.executemany(
                    "INSERT INTO conversations (role, content) VALUES (?, ?)",
                    [(m["role"], m["content"]) for m in old],
                )
                await _db.commit()
                HISTORY_FILE.rename(HISTORY_FILE.with_suffix(".migrated"))
                log.info(f"Migrated {len(old)} history entries to SQLite")
        except Exception as e:
            log.warning(f"history.json migration failed: {e}")


async def db_load_history(n: int = MAX_HISTORY) -> list[dict]:
    async with _db.execute(
        "SELECT role, content FROM conversations ORDER BY id DESC LIMIT ?", (n,)
    ) as cur:
        rows = await cur.fetchall()
    return [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]


async def db_save_exchange(user_msg: str, assistant_msg: str) -> None:
    await _db.executemany(
        "INSERT INTO conversations (role, content) VALUES (?, ?)",
        [("user", user_msg), ("assistant", assistant_msg)],
    )
    await _db.execute(
        "DELETE FROM conversations WHERE id NOT IN "
        "(SELECT id FROM conversations ORDER BY id DESC LIMIT ?)",
        (MAX_HISTORY,),
    )
    await _db.commit()


async def db_clear_history() -> None:
    await _db.execute("DELETE FROM conversations")
    await _db.commit()


async def db_load_memory() -> dict[str, str]:
    async with _db.execute(
        "SELECT key, value FROM memory ORDER BY updated_at DESC"
    ) as cur:
        rows = await cur.fetchall()
    return {r["key"]: r["value"] for r in rows}


async def db_set_memory(key: str, value: str) -> None:
    await _db.execute(
        "INSERT INTO memory (key, value, updated_at) VALUES (?, ?, strftime('%s','now')) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
        (key, value),
    )
    await _db.commit()


async def db_save_cart(cart_id: str, cart: dict) -> None:
    await _db.execute(
        "INSERT INTO carts (id, store, items, total) VALUES (?, ?, ?, ?)",
        (cart_id, cart["store"], json.dumps(cart), cart["total"]),
    )
    await _db.commit()


async def db_load_cart(cart_id: str) -> dict | None:
    async with _db.execute(
        "SELECT id, store, items, total, status FROM carts WHERE id = ?", (cart_id,)
    ) as cur:
        row = await cur.fetchone()
    if not row:
        return None
    data = json.loads(row["items"])
    data["status"] = row["status"]
    data["cart_id"] = row["id"]
    return data


async def db_update_cart_status(cart_id: str, status: str) -> None:
    await _db.execute("UPDATE carts SET status = ? WHERE id = ?", (status, cart_id))
    await _db.commit()


async def db_latest_pending_cart() -> dict | None:
    async with _db.execute(
        "SELECT id, store, items, total, status FROM carts WHERE status = 'pending' ORDER BY created_at DESC LIMIT 1"
    ) as cur:
        row = await cur.fetchone()
    if not row:
        return None
    data = json.loads(row["items"])
    data["status"] = row["status"]
    data["cart_id"] = row["id"]
    return data


def save_task_id(tid: str) -> None:
    TASK_FILE.write_text(tid)


def load_task_id() -> str | None:
    try:
        tid = TASK_FILE.read_text().strip()
        return tid if tid else None
    except FileNotFoundError:
        return None


def clear_task_id() -> None:
    try:
        TASK_FILE.unlink()
    except FileNotFoundError:
        pass


# ── Proactive monitor ──────────────────────────────────────────────────────────

def _cooldown_ok(key: str) -> bool:
    return time.monotonic() - _alert_cooldowns.get(key, 0) > ALERT_COOLDOWN.get(key, 3600)


def _mark_alerted(key: str) -> None:
    _alert_cooldowns[key] = time.monotonic()


async def _queue_alert(msg: str) -> None:
    try:
        audio = await tts_bytes(msg)
        await _alert_queue.put({"audio": audio, "text": msg})
        log.info(f"Monitor alert queued: {msg}")
    except Exception as e:
        log.warning(f"Monitor TTS error: {e}")


async def _check_disk() -> str | None:
    d = psutil.disk_usage("/")
    if d.percent >= 85:
        gb = d.free / 1024 ** 3
        return f"Alerta disco: {d.percent:.0f} por ciento usado, {gb:.1f} gigabytes libres."
    return None


async def _check_ram() -> str | None:
    m = psutil.virtual_memory()
    if m.percent >= 90:
        gb = m.available / 1024 ** 3
        return f"Alerta memoria: {m.percent:.0f} por ciento usada, {gb:.1f} gigabytes disponibles."
    return None


async def _check_services() -> list[str]:
    alerts = []
    for svc in ["sshd"]:
        try:
            proc = await asyncio.create_subprocess_exec(
                "systemctl", "is-active", svc,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            out, _ = await proc.communicate()
            if out.decode().strip() != "active":
                alerts.append(f"Servicio {svc} no está activo.")
        except Exception:
            pass
    return alerts


async def _check_ssl() -> str | None:
    cert_dir = Path("/etc/letsencrypt/live")
    if not cert_dir.exists():
        return None
    try:
        certs = list(cert_dir.glob("*/fullchain.pem"))
        if not certs:
            return None
        proc = await asyncio.create_subprocess_exec(
            "openssl", "x509", "-enddate", "-noout", "-in", str(certs[0]),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await proc.communicate()
        date_str = out.decode().strip().split("=", 1)[-1].rsplit(" ", 1)[0]
        expiry = datetime.datetime.strptime(date_str, "%b %d %H:%M:%S %Y").replace(
            tzinfo=datetime.timezone.utc
        )
        days_left = (expiry - datetime.datetime.now(datetime.timezone.utc)).days
        if days_left < 14:
            return f"Alerta SSL: certificado expira en {days_left} días."
    except Exception:
        pass
    return None


async def _monitor_loop() -> None:
    await asyncio.sleep(30)  # let startup settle
    while True:
        try:
            if _cooldown_ok("disk"):
                msg = await _check_disk()
                if msg:
                    _mark_alerted("disk")
                    await _queue_alert(msg)

            if _cooldown_ok("ram"):
                msg = await _check_ram()
                if msg:
                    _mark_alerted("ram")
                    await _queue_alert(msg)

            if _cooldown_ok("service"):
                msgs = await _check_services()
                if msgs:
                    _mark_alerted("service")
                    for msg in msgs:
                        await _queue_alert(msg)

            if _cooldown_ok("ssl"):
                msg = await _check_ssl()
                if msg:
                    _mark_alerted("ssl")
                    await _queue_alert(msg)

        except Exception as e:
            log.error(f"Monitor loop error: {e}")

        await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    monitor_task = asyncio.create_task(_monitor_loop())
    log.info("ZEUS listo.")
    yield
    monitor_task.cancel()
    try:
        await monitor_task
    except asyncio.CancelledError:
        pass
    await shopper.stop_browser_session()
    await shopper.close_control_browser()
    if _db:
        await _db.close()


app = FastAPI(title="ZEUS", lifespan=lifespan)

_PUBLIC_PATHS = frozenset(["/", "/manifest.json", "/sw.js", "/icon.svg", "/icon-maskable.svg"])
_TRUSTED_PROXIES = frozenset(["127.0.0.1", "::1"])
_FORWARD_HEADERS = ("x-forwarded-for", "x-real-ip", "cf-connecting-ip", "forwarded")


def _is_external(request: Request) -> bool:
    """True if the request arrived through nginx / the cloudflared tunnel.

    Claude's own curl calls hit uvicorn directly from localhost with no
    forwarding headers; the reverse proxy and the tunnel always add them.
    Any socket peer that is not a trusted local proxy is also external.
    """
    peer = request.client.host if request.client else ""
    if peer not in _TRUSTED_PROXIES:
        return True
    return any(h in request.headers for h in _FORWARD_HEADERS)


@app.middleware("http")
async def _auth_middleware(request: Request, call_next):
    path = request.url.path
    if (request.method != "OPTIONS"
            and path not in _PUBLIC_PATHS
            and not path.startswith("/novnc")
            and _is_external(request)):
        provided = request.headers.get("X-API-Key", "")
        if not API_KEY or not hmac.compare_digest(provided, API_KEY):
            return JSONResponse({"detail": "No autorizado"}, status_code=401)
    return await call_next(request)

_ALLOWED_ORIGINS = [
    o.strip() for o in os.getenv(
        "ZEUS_ALLOWED_ORIGINS",
        "https://zeus-claude.duckdns.org,https://192.168.1.2,"
        "http://localhost:8000,http://127.0.0.1:8000",
    ).split(",") if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
    expose_headers=["X-Transcription", "X-Response", "X-Task-Id", "X-Text"],
)

_NOVNC_DIR = Path("/usr/share/novnc")
if _NOVNC_DIR.exists():
    app.mount("/novnc", StaticFiles(directory=str(_NOVNC_DIR)), name="novnc")


# ── STT ────────────────────────────────────────────────────────────────────────

def _transcribe_sync(audio_path: str) -> str:
    try:
        with open(audio_path, "rb") as f:
            result = _groq.audio.transcriptions.create(
                file=(os.path.basename(audio_path), f),
                model="whisper-large-v3",
                language="es",
                response_format="text",
            )
        return (result or "").strip()
    except Exception as e:
        log.warning(f"Groq STT error: {e}")
        return ""


async def transcribe(audio_path: str) -> str:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _transcribe_sync, audio_path)


# ── LLM (Claude Code) ──────────────────────────────────────────────────────────

def _find_sentence_end(buf: str) -> int:
    for i, ch in enumerate(buf):
        if ch in PUNCT:
            return i + 1
    return -1


def _kill_proc_tree(proc: asyncio.subprocess.Process, sig: int = signal.SIGTERM) -> None:
    """Best-effort signal to the subprocess's whole process group.

    claude spawns its own shells (curl, python…); killing only the direct
    child leaves those orphaned. The process is started with
    start_new_session=True, so it leads its own group and we can reach the
    whole tree via killpg. Guards on returncode to avoid hitting a reused PID.
    """
    if proc.returncode is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), sig)
    except (ProcessLookupError, PermissionError):
        try:
            proc.send_signal(sig)
        except ProcessLookupError:
            pass


async def _run_claude(task_id: str, texto: str, queue: asyncio.Queue) -> None:
    global _zeus_session_id, _zeus_log, _zeus_msg_count, _last_prompt_hash, _pending_context
    _zeus_log.clear()
    _zeus_msg_count += 1
    if _zeus_msg_count > 10:
        recent = await db_load_history(6)
        if recent:
            lines = []
            for msg in recent:
                speaker = "Yos" if msg["role"] == "user" else "Zeus"
                snippet = msg["content"][:120].replace("\n", " ")
                lines.append(f"{speaker}: {snippet}")
            _pending_context = (
                "CONTEXTO DE LA CONVERSACIÓN ANTERIOR (resumen):\n"
                + "\n".join(lines)
            )
        else:
            _pending_context = None
        _zeus_session_id = None
        _zeus_msg_count = 1
        log.info("[claude] session reset after 10 messages")

    context_to_inject = _pending_context
    _pending_context = None

    memory_facts = await db_load_memory()
    dynamic_prompt = CLAUDE_VOICE_PROMPT
    if memory_facts:
        facts = "\n".join(f"- {k}: {v}" for k, v in memory_facts.items())
        dynamic_prompt += f"\n\nHECHOS CONOCIDOS SOBRE EL SISTEMA Y YOS:\n{facts}"
    if context_to_inject:
        dynamic_prompt += f"\n\n{context_to_inject}"
        log.info("[claude] injecting prior-session context into new session prompt")

    prompt_hash = hashlib.md5(dynamic_prompt.encode()).hexdigest()
    send_prompt = (_zeus_session_id is None) or (prompt_hash != _last_prompt_hash)
    _last_prompt_hash = prompt_hash

    cmd = [
        CLAUDE_BIN, "-p", texto,
        "--output-format", "stream-json",
        "--verbose",
        "--include-partial-messages",
        "--dangerously-skip-permissions",
    ]
    if send_prompt:
        cmd += ["--append-system-prompt", dynamic_prompt]
        log.info("[claude] system prompt sent (new session or prompt changed)")
    if _zeus_session_id:
        cmd += ["--resume", _zeus_session_id]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=_HOME,
        env={**os.environ, "HOME": _HOME, "USER": _USER},
        start_new_session=True,  # own process group, so a timeout can kill the whole tree
    )

    buffer = ""
    full_response = ""
    raw_buf = b""

    async def flush_sentence(sentence: str) -> None:
        nonlocal full_response
        sentence = clean_for_tts(sentence)
        if not sentence:
            return
        full_response += sentence + " "
        try:
            audio = await tts_bytes(sentence)
            if audio:
                await queue.put({"status": "ready", "audio": audio, "text": sentence})
            else:
                log.warning(f"[TASK {task_id}] TTS returned empty audio")
                await queue.put({"status": "tts_error", "text": sentence})
        except Exception as e:
            log.warning(f"[TASK {task_id}] TTS error tras 3 intentos: {e}")
            await queue.put({"status": "tts_error", "text": sentence})

    async def process_line(raw_line: bytes) -> None:
        global _zeus_session_id
        nonlocal buffer
        line = raw_line.decode(errors="ignore").strip()
        if not line:
            return
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            return

        top_type = data.get("type", "")

        # Capture session_id from init
        if top_type == "system" and data.get("subtype") == "init":
            sid = data.get("session_id")
            if sid:
                _zeus_session_id = sid
                log.info(f"[claude] session_id={sid}")

        # Capture tool calls from assistant messages
        elif top_type == "assistant":
            for block in data.get("message", {}).get("content", []):
                if block.get("type") == "tool_use":
                    name = block.get("name", "")
                    inp  = block.get("input", {})
                    cmd_text = inp.get("command") or inp.get("path") or json.dumps(inp)[:120]
                    _zeus_log.append({"type": "tool", "tool": name, "input": cmd_text})
                    log.info(f"[claude] tool_use: {name}({cmd_text[:80]})")

        # Capture tool results
        elif top_type == "user":
            for block in data.get("message", {}).get("content", []):
                if block.get("type") == "tool_result":
                    result = block.get("content", "")
                    if isinstance(result, list):
                        result = " ".join(r.get("text", "") for r in result if isinstance(r, dict))
                    _zeus_log.append({"type": "result", "output": str(result)[:300]})

        # Text streaming (partial messages)
        ev_type = data.get("event", {}).get("type", "")
        if ev_type == "content_block_delta":
            delta = data["event"].get("delta", {})
            if delta.get("type") == "text_delta":
                buffer += delta.get("text", "")
                while True:
                    end = _find_sentence_end(buffer)
                    if end == -1:
                        break
                    await flush_sentence(buffer[:end])
                    buffer = buffer[end:].lstrip()

    try:
        while True:
            chunk = await proc.stdout.read(65536)
            if not chunk:
                break
            raw_buf += chunk
            while b"\n" in raw_buf:
                raw_line, raw_buf = raw_buf.split(b"\n", 1)
                await process_line(raw_line)

        if raw_buf:
            await process_line(raw_buf)

        if buffer.strip():
            await flush_sentence(buffer)

        rc = await proc.wait()
        if rc != 0:
            stderr = await proc.stderr.read()
            log.warning(f"[TASK {task_id}] claude exit {rc}: {stderr.decode(errors='ignore')[:200]}")

        if full_response.strip():
            await db_save_exchange(texto, full_response.strip())

        log.info(f"[TASK {task_id}] stream terminado")
    finally:
        # Never leave the claude subprocess (or its shells) running past this
        # coroutine — covers the timeout path, where wait_for cancels us mid-read.
        if proc.returncode is None:
            _kill_proc_tree(proc, signal.SIGTERM)
            asyncio.get_running_loop().call_later(3, _kill_proc_tree, proc, signal.SIGKILL)


async def run_claude_streaming(
    task_id: str, texto: str, queue: asyncio.Queue, timeout: float = CLAUDE_TIMEOUT
) -> None:
    try:
        await asyncio.wait_for(_claude_sem.acquire(), timeout=20)
    except asyncio.TimeoutError:
        log.warning(f"[TASK {task_id}] semáforo ocupado, descartando")
        try:
            audio = await tts_bytes("ZEUS está ocupado, inténtalo de nuevo.")
            await queue.put({"status": "ready", "audio": audio, "text": "ZEUS está ocupado, inténtalo de nuevo."})
        except Exception:
            pass
        await queue.put({"status": "done"})
        audio_queues.pop(task_id, None)
        return
    try:
        await asyncio.wait_for(_run_claude(task_id, texto, queue), timeout=timeout)
    except asyncio.TimeoutError:
        log.warning(f"[TASK {task_id}] timeout tras {timeout}s")
        try:
            audio = await tts_bytes("Lo siento, la tarea tardó demasiado.")
            await queue.put({"status": "ready", "audio": audio, "text": "Lo siento, la tarea tardó demasiado."})
        except Exception:
            pass
    except Exception as e:
        log.error(f"[TASK {task_id}] error: {e}")
    finally:
        _claude_sem.release()
        await queue.put({"status": "done"})
        clear_task_id()
        asyncio.get_running_loop().call_later(300, lambda: audio_queues.pop(task_id, None))


# ── TTS ────────────────────────────────────────────────────────────────────────

def clean_for_tts(text: str) -> str:
    # Strip markdown
    text = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', text)
    text = re.sub(r'```[\s\S]*?```', '', text)
    text = re.sub(r'`([^`]+)`', r'\1', text)
    text = re.sub(r'\*{1,3}([^*]+)\*{1,3}', r'\1', text)
    text = re.sub(r'_{1,3}([^_]+)_{1,3}', r'\1', text)
    text = re.sub(r'^#{1,6}\s*', '', text, flags=re.MULTILINE)
    text = re.sub(r'^[-*+]\s+', '', text, flags=re.MULTILINE)
    text = re.sub(r'^\d+\.\s+', '', text, flags=re.MULTILINE)
    text = re.sub(r'[→←↑↓►◄▶◀•–—]', ' ', text)
    text = re.sub(r'[\*\#]+', '', text)
    # URLs → domain only
    text = re.sub(r'https?://([^/\s]+)(?:/\S*)?', r'\1', text)
    # Unix paths → filename only
    text = re.sub(r'(?<!\w)\.?/[\w.\-/]+', lambda m: os.path.basename(m.group(0).rstrip('/')), text)
    # :PORT → "puerto PORT"
    text = re.sub(r':(\d{2,5})\b', lambda m: f' puerto {m.group(1)}', text)
    # Unit conversions
    text = re.sub(r'(\d+(?:[.,]\d+)?)\s*GB\b', r'\1 gigabytes', text, flags=re.IGNORECASE)
    text = re.sub(r'(\d+(?:[.,]\d+)?)\s*MB\b', r'\1 megabytes', text, flags=re.IGNORECASE)
    text = re.sub(r'(\d+(?:[.,]\d+)?)\s*KB\b', r'\1 kilobytes', text, flags=re.IGNORECASE)
    text = re.sub(r'(\d+(?:[.,]\d+)?)\s*ms\b', r'\1 milisegundos', text, flags=re.IGNORECASE)
    text = re.sub(r'(\d+(?:[.,]\d+)?)\s*%', r'\1 por ciento', text)
    # snake_case → words
    text = re.sub(r'_', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


async def tts_bytes(text: str) -> bytes:
    if not text.strip():
        return b""
    for attempt in range(3):
        try:
            communicate = edge_tts.Communicate(text, voice="es-ES-AlvaroNeural", rate="+20%", pitch="-10Hz")
            chunks = []
            async for chunk in communicate.stream():
                if chunk["type"] == "audio":
                    chunks.append(chunk["data"])
            result = b"".join(chunks)
            if result:
                return result
        except Exception as e:
            if attempt == 2:
                raise
            await asyncio.sleep(0.4 * (attempt + 1))
    return b""


# ── Screen control ─────────────────────────────────────────────────────────────

_UID = os.getuid()
_XDG_RUNTIME  = f"/run/user/{_UID}"
_WAYLAND_DISP = os.getenv("WAYLAND_DISPLAY", "wayland-0")
_DBUS_ADDR    = os.getenv("DBUS_SESSION_BUS_ADDRESS", f"unix:path=/run/user/{_UID}/bus")
_SCREEN_ENV   = {
    **os.environ,
    "HOME": _HOME,
    "USER": _USER,
    "WAYLAND_DISPLAY": _WAYLAND_DISP,
    "XDG_RUNTIME_DIR": _XDG_RUNTIME,
    "DBUS_SESSION_BUS_ADDRESS": _DBUS_ADDR,
    "DISPLAY": os.getenv("DISPLAY", ":0"),
}

async def _screenshot() -> bytes:
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        path = tmp.name
    try:
        proc = await asyncio.create_subprocess_exec(
            "gnome-screenshot", "-f", path,
            env=_SCREEN_ENV,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, err = await asyncio.wait_for(proc.communicate(), timeout=15)
        if proc.returncode != 0:
            raise RuntimeError(f"gnome-screenshot falló: {err.decode(errors='ignore').strip()}")
        return Path(path).read_bytes()
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


async def _ydotool(*args: str, env: dict | None = None) -> None:
    """Run ydotool via `sg input` so /dev/uinput group permission is acquired."""
    cmd_str = "ydotool " + " ".join(args)
    run_env = {**_SCREEN_ENV, **(env or {})}
    proc = await asyncio.create_subprocess_exec(
        "sg", "input", "-c", cmd_str,
        env=run_env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, err = await asyncio.wait_for(proc.communicate(), timeout=15)
    if proc.returncode != 0:
        msg = err.decode(errors="ignore")
        if "notice:" not in msg:
            raise RuntimeError(f"ydotool error: {msg.strip()}")


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    return Path("client/index.html").read_text()


@app.get("/metrics")
async def metrics():
    cpu = psutil.cpu_percent(interval=0.1)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    load = psutil.getloadavg()
    uptime = int(time.time() - psutil.boot_time())
    return {
        "cpu": round(cpu, 1),
        "mem_used": mem.used,
        "mem_total": mem.total,
        "mem_pct": round(mem.percent, 1),
        "disk_used": disk.used,
        "disk_total": disk.total,
        "disk_pct": round(disk.percent, 1),
        "load1": round(load[0], 2),
        "uptime": uptime,
    }


@app.get("/manifest.json")
async def manifest():
    return FileResponse("client/manifest.json", media_type="application/manifest+json")


@app.get("/sw.js")
async def service_worker():
    sw_path = Path("client/sw.js")
    html_path = Path("client/index.html")
    sw_text = sw_path.read_text()
    h = hashlib.md5(html_path.read_bytes()).hexdigest()[:8]
    sw_text = re.sub(r"const CACHE = 'zeus-[^']*'", f"const CACHE = 'zeus-{h}'", sw_text)
    return Response(
        content=sw_text,
        media_type="application/javascript",
        headers={"Service-Worker-Allowed": "/", "Cache-Control": "no-cache"},
    )


@app.get("/version")
async def version():
    h = hashlib.md5(Path("client/index.html").read_bytes()).hexdigest()[:8]
    return JSONResponse({"hash": h})


@app.get("/icon.svg")
async def icon():
    return FileResponse("client/icon.svg", media_type="image/svg+xml")


@app.get("/icon-maskable.svg")
async def icon_maskable():
    return FileResponse("client/icon-maskable.svg", media_type="image/svg+xml")


@app.get("/tarea-actual")
async def tarea_actual():
    tid = load_task_id()
    if tid and tid in audio_queues:
        return {"task_id": tid}
    if tid:
        clear_task_id()
    return {"task_id": None}


@app.get("/alerta")
async def alerta():
    try:
        item = _alert_queue.get_nowait()
        return Response(
            content=item["audio"],
            media_type="audio/mpeg",
            headers={"X-Alert-Text": quote(item["text"])},
        )
    except asyncio.QueueEmpty:
        return JSONResponse({"status": "none"})


@app.post("/recuerda")
async def recuerda(request: Request):
    check_rate_limit(request.client.host)
    body = await request.json()
    key = (body.get("clave") or "").strip()
    value = (body.get("valor") or "").strip()
    if not key or not value:
        raise HTTPException(status_code=400, detail="'clave' y 'valor' requeridos")
    await db_set_memory(key, value)
    return {"ok": True, "clave": key, "valor": value}


@app.delete("/historia")
async def borrar_historia(request: Request):
    check_rate_limit(request.client.host)
    await db_clear_history()
    log.info("[historia] conversación borrada por petición")
    return JSONResponse({"ok": True, "mensaje": "Historial de conversación eliminado"})


@app.get("/siguiente/{task_id}")
async def siguiente(task_id: str):
    queue = audio_queues.get(task_id)
    if not queue:
        return JSONResponse({"status": "not_found"}, status_code=404)
    try:
        item = await asyncio.wait_for(queue.get(), timeout=30)
    except asyncio.TimeoutError:
        return JSONResponse({"status": "pending"})

    if item["status"] == "done":
        audio_queues.pop(task_id, None)
        return JSONResponse({"status": "done"})

    if item["status"] == "tts_error":
        return JSONResponse({"status": "tts_error", "text": item.get("text", "")})

    return Response(
        content=item["audio"],
        media_type="audio/mpeg",
        headers={"X-Text": quote(item["text"]), "X-Task-Id": task_id},
    )


@app.post("/voz")
async def voz(request: Request, background_tasks: BackgroundTasks, audio: UploadFile = File(...)):
    check_rate_limit(request.client.host)
    cl = request.headers.get("content-length")
    if cl and int(cl) > MAX_AUDIO_BYTES:
        raise HTTPException(status_code=413, detail="Audio demasiado grande (máx 10MB)")

    data = await audio.read(MAX_AUDIO_BYTES + 1)
    if len(data) > MAX_AUDIO_BYTES:
        raise HTTPException(status_code=413, detail="Audio demasiado grande (máx 10MB)")

    content_type = audio.content_type or "audio/webm"
    ext_map = {
        "audio/wav": ".wav", "audio/wave": ".wav", "audio/webm": ".webm",
        "audio/ogg": ".ogg", "audio/mp4": ".mp4", "audio/mpeg": ".mp3",
        "audio/x-m4a": ".m4a",
    }
    base_ct = content_type.split(";")[0].strip()
    ext = ext_map.get(base_ct, ".webm")

    with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
        tmp.write(data)
        audio_path = tmp.name

    try:
        texto = await transcribe(audio_path)
    finally:
        try:
            os.unlink(audio_path)
        except OSError:
            pass

    if not texto or _is_noise_transcript(texto):
        if texto:
            log.info(f"Transcripción basura descartada: '{texto}'")
        else:
            log.info("Transcripción vacía — descartando silencio")
        return JSONResponse({"status": "noise", "transcripcion": texto or ""}, status_code=200)

    # Queue created BEFORE background task to eliminate race condition
    task_id = uuid.uuid4().hex
    queue: asyncio.Queue = asyncio.Queue()
    audio_queues[task_id] = queue
    save_task_id(task_id)

    # A login waiting for a code/extra datum takes priority: route the answer
    # to it instead of starting a new Claude turn.
    if _route_to_pending_login(texto, task_id, queue, background_tasks):
        return JSONResponse({"status": "processing", "task_id": task_id, "transcripcion": texto})

    background_tasks.add_task(run_claude_streaming, task_id, texto, queue)
    return JSONResponse({
        "status": "processing",
        "task_id": task_id,
        "transcripcion": texto,
    })


@app.post("/texto")
async def texto_endpoint(request: Request, background_tasks: BackgroundTasks):
    check_rate_limit(request.client.host)
    body = await request.json()
    texto = (body.get("texto") or "").strip()
    if not texto:
        raise HTTPException(status_code=400, detail="Campo 'texto' vacío")

    task_id = uuid.uuid4().hex
    queue: asyncio.Queue = asyncio.Queue()
    audio_queues[task_id] = queue
    save_task_id(task_id)

    # A login waiting for a code/extra datum takes priority: route the answer
    # to it instead of starting a new Claude turn.
    if _route_to_pending_login(texto, task_id, queue, background_tasks):
        return JSONResponse({"status": "processing", "task_id": task_id, "transcripcion": texto})

    background_tasks.add_task(run_claude_streaming, task_id, texto, queue)
    return JSONResponse({
        "status": "processing",
        "task_id": task_id,
        "transcripcion": texto,
    })


@app.get("/auth/status")
async def auth_status():
    return JSONResponse(shopper.auth_status())


# ── Browser control endpoints (used by Claude via curl) ───────────────────────

@app.get("/browser/screenshot")
async def browser_screenshot():
    try:
        path = await shopper.ctrl_screenshot("/tmp/zeus_browser.png")
        return FileResponse(path, media_type="image/png")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/browser/elements")
async def browser_elements():
    try:
        els = await shopper.ctrl_elements()
        return JSONResponse({"elements": els, "count": len(els)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/browser/action")
async def browser_action(request: Request):
    body = await request.json()
    try:
        result = await shopper.ctrl_action(body)
        return JSONResponse({"ok": True, "result": result})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/browser/save-session")
async def browser_save_session():
    try:
        await shopper.ctrl_save_session()
        store = shopper._ctrl_store or "desconocida"
        await shopper.close_control_browser()
        return JSONResponse({"ok": True, "store": store})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── 2FA / verification-code handoff ───────────────────────────────────────────
_code_slots:  dict[str, str | None]       = {}
_code_events: dict[str, asyncio.Event]    = {}


def _pending_code_store() -> str | None:
    """Store whose login is currently blocked waiting for a code / extra datum."""
    for store, ev in _code_events.items():
        if ev is not None and not ev.is_set():
            return store
    return None


def _extract_code(text: str) -> str:
    """Turn a spoken/typed answer into the value handed back to the login.

    OTP codes are read out digit by digit, so if the answer carries digits we
    keep just those ("el código es 2 5 5 0 9 1" -> "255091"). Otherwise (e.g. a
    security answer) we pass the trimmed text as-is.
    """
    digits = re.sub(r"\D", "", text)
    if len(digits) >= 3:
        return digits
    return text.strip()


def _submit_pending_code(store: str, value: str) -> None:
    _code_slots[store] = value
    ev = _code_events.get(store)
    if ev:
        ev.set()


async def _deliver_ack(task_id: str, queue: asyncio.Queue, message: str) -> None:
    """Speak a short confirmation and finish, reusing the normal streaming flow."""
    try:
        audio = await tts_bytes(message)
        if audio:
            await queue.put({"status": "ready", "audio": audio, "text": message})
    except Exception:
        pass
    await queue.put({"status": "done"})
    clear_task_id()
    asyncio.get_running_loop().call_later(300, lambda: audio_queues.pop(task_id, None))


def _route_to_pending_login(
    texto: str, task_id: str, queue: asyncio.Queue, background_tasks: BackgroundTasks
) -> bool:
    """If a store login is waiting for input, hand `texto` to it (by voice or text).

    Returns True when consumed as a login answer — the caller then skips the
    Claude turn (and the "ocupado" it would cause) and just acks the user.
    """
    store = _pending_code_store()
    if not store:
        return False
    value = _extract_code(texto)
    _submit_pending_code(store, value)
    log.info(f"[auth] respuesta del usuario enrutada al login de {store}: '{value}'")
    background_tasks.add_task(_deliver_ack, task_id, queue, "Recibido, continúo.")
    return True


async def _run_login(store: str, task_id: str, prompt: str, queue: asyncio.Queue) -> None:
    """Drive a store login and always clean up its pending-code state on exit.

    Without this, a login that dies mid-OTP (timeout/error) would leave a stale
    _code_events entry, and the next unrelated voice/text would be misrouted to it.
    """
    try:
        await run_claude_streaming(task_id, prompt, queue, timeout=LOGIN_TIMEOUT)
    finally:
        _code_slots.pop(store, None)
        ev = _code_events.pop(store, None)
        if ev and not ev.is_set():
            ev.set()  # unblock any await-code still polling


@app.post("/auth/need-code/{store}")
async def auth_need_code(store: str, text: str = "Introduce el código de verificación"):
    _code_slots[store]  = None
    _code_events[store] = asyncio.Event()
    shopper._emit({"type": "need_code", "text": text})
    return JSONResponse({"ok": True})

@app.get("/auth/await-code/{store}")
async def auth_await_code(store: str):
    ev = _code_events.get(store)
    if not ev:
        raise HTTPException(status_code=404, detail="No hay código pendiente para esta tienda")
    try:
        await asyncio.wait_for(ev.wait(), timeout=240)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout esperando código del usuario")
    code = _code_slots.pop(store, None)
    _code_events.pop(store, None)
    return JSONResponse({"code": code})

@app.post("/auth/code/{store}")
async def auth_submit_code(store: str, request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido")
    code = (body.get("code") or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="Código vacío")
    _code_slots[store] = code
    ev = _code_events.get(store)
    if ev:
        ev.set()
    return JSONResponse({"ok": True})


@app.post("/auth/credentials/{store}")
async def auth_credentials(store: str, request: Request, background_tasks: BackgroundTasks):
    store = store.lower()
    if store not in shopper.STORE_NAMES:
        raise HTTPException(status_code=400, detail=f"Tienda desconocida: {store}")
    try:
        body = await request.json()
    except Exception:
        body = {}
    email    = (body.get("email")    or "").strip()
    password = (body.get("password") or "").strip()
    display  = (body.get("salida") or body.get("display") or "screen").strip().lower()
    if display not in ("screen", "novnc"):
        display = "screen"
    if not email or not password:
        raise HTTPException(status_code=400, detail="Email y contraseña requeridos")

    shopper.clear_shopper_log()

    if store == "mercadona":
        try:
            await asyncio.wait_for(shopper.connect_mercadona(email=email, password=password), timeout=30)
            return JSONResponse({"ok": True, "store": store})
        except asyncio.TimeoutError:
            raise HTTPException(status_code=504, detail="Tiempo de espera agotado")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    if store not in shopper._HEADED_STORES:
        try:
            await asyncio.wait_for(
                shopper.connect_store_browser(store, email=email, password=password), timeout=60
            )
            return JSONResponse({"ok": True, "store": store})
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    # Headed stores: Claude drives the browser on the chosen output (screen / noVNC)
    try:
        ctrl = await asyncio.wait_for(shopper.start_control_browser(store, display=display), timeout=40)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"No se pudo abrir Chrome: {e}")
    initial_url = ctrl["url"]

    task_id = uuid.uuid4().hex
    queue: asyncio.Queue = asyncio.Queue()
    audio_queues[task_id] = queue
    save_task_id(task_id)

    prompt = (
        f"Conecta la cuenta de {store.capitalize()} con email '{email}' y contraseña '{password}'. "
        f"El navegador ya está abierto en {initial_url}. "
        "Usa las herramientas de control de navegador para completar el login paso a paso: "
        "captura la pantalla, analiza los elementos, haz clic y escribe según sea necesario. "
        "Cuando el login sea exitoso ejecuta: "
        "curl -s -X POST http://localhost:8000/browser/save-session "
        "para guardar la sesión. "
        f"Si encuentras una pantalla de código de verificación (SMS/email OTP) o el "
        f"sitio te pide cualquier dato extra (pregunta de seguridad, etc.): "
        f"1) Pídeselo al usuario por voz y dile que puede responderte hablando o "
        f"escribiéndolo en la app. "
        f"2) Ejecuta: curl -s -X POST 'http://localhost:8000/auth/need-code/{store}?text=QUE_NECESITAS' "
        f"(sustituye QUE_NECESITAS por una descripción corta de lo que pides). "
        f"3) Espera la respuesta: curl -s http://localhost:8000/auth/await-code/{store} "
        f"(el JSON devuelto trae el campo 'code' con lo que el usuario diga o escriba). "
        f"4) Escribe ese valor en el campo correspondiente del navegador y continúa. "
        "Si encuentras CAPTCHA o pregunta de seguridad inesperada, descríbeselo al usuario. "
        "Sé conciso en tus respuestas de voz."
    )

    background_tasks.add_task(_run_login, store, task_id, prompt, queue)
    resp = {"ok": True, "store": store, "task_id": task_id, "display": display}
    if ctrl.get("token"):
        resp["novnc_url"] = (
            f"/novnc/vnc.html?path=view-ws%2F{ctrl['token']}"
            "&autoconnect=1&resize=scale&reconnect=0&show_dot=true"
        )
    return JSONResponse(resp)


@app.post("/auth/connect/{store}")
async def auth_connect(store: str, background_tasks: BackgroundTasks):
    store = store.lower()
    if store not in shopper.STORE_NAMES:
        raise HTTPException(status_code=400, detail=f"Tienda desconocida: {store}")

    if store == "mercadona":
        try:
            await shopper.connect_mercadona()
            return JSONResponse({"ok": True, "store": store})
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    else:
        # Playwright stores: launch visible browser, wait for login in background
        # Return immediately so the frontend knows the browser is opening
        async def _do_connect():
            try:
                await shopper.connect_store_browser(store)
                log.info(f"auth/connect/{store}: sesión guardada")
            except Exception as e:
                log.error(f"auth/connect/{store}: {e}")

        background_tasks.add_task(_do_connect)
        return JSONResponse({"ok": True, "store": store, "browser": True})


@app.delete("/auth/connect/{store}")
async def auth_disconnect(store: str):
    store = store.lower()
    if store not in shopper.STORE_NAMES:
        raise HTTPException(status_code=400, detail=f"Tienda desconocida: {store}")
    shopper.clear_session(store)
    return JSONResponse({"ok": True, "store": store})


@app.post("/auth/import-chrome/{store}")
async def auth_import_chrome(store: str):
    """Import session cookies from the local Chrome profile."""
    store = store.lower()
    if store not in shopper.STORE_NAMES:
        raise HTTPException(status_code=400, detail=f"Tienda desconocida: {store}")
    try:
        n = shopper.import_chrome_session(store)
        return JSONResponse({"ok": True, "store": store, "cookies": n})
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inesperado: {e}")


@app.post("/auth/browser/{store}")
async def auth_browser_start(store: str):
    store = store.lower()
    if store not in shopper.STORE_NAMES:
        raise HTTPException(status_code=400, detail=f"Tienda desconocida: {store}")
    if store == "mercadona":
        raise HTTPException(status_code=400, detail="Mercadona usa API, no navegador")
    if not _NOVNC_DIR.exists():
        raise HTTPException(
            status_code=503,
            detail="noVNC no instalado. Ejecutar: sudo apt install xvfb x11vnc novnc",
        )
    try:
        token = await shopper.start_browser_session(store)
        novnc_url = (
            f"/novnc/vnc.html"
            f"?path=novnc-ws%2F{token}"
            f"&autoconnect=1&resize=scale&reconnect=0&show_dot=true"
        )
        return JSONResponse({"ok": True, "token": token, "novnc_url": novnc_url})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/auth/browser")
async def auth_browser_stop():
    await shopper.stop_browser_session()
    return JSONResponse({"ok": True})


@app.websocket("/novnc-ws/{token}")
async def novnc_ws_proxy(websocket: WebSocket, token: str):
    expected = shopper.browser_session_token()
    if not expected or token != expected:
        await websocket.close(code=4403)
        return

    await websocket.accept(subprotocol=("binary" if "binary" in (websocket.scope.get("subprotocols") or []) else None))

    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", shopper.VNC_PORT)
    except Exception as e:
        log.warning(f"novnc_ws: no conecta a VNC: {e}")
        try:
            await websocket.close()
        except Exception:
            pass
        return

    async def ws_to_vnc():
        try:
            while True:
                data = await websocket.receive_bytes()
                writer.write(data)
                await writer.drain()
        except Exception:
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass

    async def vnc_to_ws():
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                await websocket.send_bytes(data)
        except Exception:
            pass

    fwd = asyncio.create_task(ws_to_vnc())
    bwd = asyncio.create_task(vnc_to_ws())
    await asyncio.wait([fwd, bwd], return_when=asyncio.FIRST_COMPLETED)
    fwd.cancel()
    bwd.cancel()
    try:
        await websocket.close()
    except Exception:
        pass


@app.websocket("/view-ws/{token}")
async def view_ws_proxy(websocket: WebSocket, token: str):
    expected = shopper.view_session_token()
    if not expected or token != expected:
        await websocket.close(code=4403)
        return
    await websocket.accept(subprotocol=("binary" if "binary" in (websocket.scope.get("subprotocols") or []) else None))
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", shopper.VIEW_VNC_PORT)
    except Exception as e:
        log.warning(f"view_ws: no conecta a VNC: {e}")
        try:
            await websocket.close()
        except Exception:
            pass
        return

    async def _c2v():
        try:
            while True:
                data = await websocket.receive_bytes()
                writer.write(data)
                await writer.drain()
        except Exception:
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass

    async def _v2c():
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                await websocket.send_bytes(data)
        except Exception:
            pass

    a = asyncio.create_task(_c2v())
    b = asyncio.create_task(_v2c())
    await asyncio.wait([a, b], return_when=asyncio.FIRST_COMPLETED)
    a.cancel()
    b.cancel()
    try:
        await websocket.close()
    except Exception:
        pass


@app.get("/shopper-log")
async def shopper_log(since: int = 0):
    events = shopper._shopper_log[since:]
    return JSONResponse({"events": events, "next": len(shopper._shopper_log)})


@app.get("/zeus-log")
async def zeus_log_endpoint(since: int = 0):
    events = _zeus_log[since:]
    return JSONResponse({"events": events, "next": len(_zeus_log)})


@app.post("/compra")
async def compra(request: Request):
    check_rate_limit(request.client.host)
    body = await request.json()
    tienda = (body.get("tienda") or "").strip().lower()
    items = body.get("items") or []
    if not tienda or not items:
        raise HTTPException(status_code=400, detail="Se requieren 'tienda' e 'items'")
    if tienda not in shopper.STORE_NAMES:
        raise HTTPException(status_code=400, detail=f"Tienda desconocida. Opciones: {', '.join(shopper.STORE_NAMES)}")
    try:
        cart = await asyncio.wait_for(shopper.build_cart(tienda, items), timeout=120)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Tiempo de espera agotado al conectar con la tienda")
    except Exception as e:
        log.error(f"build_cart error: {e}")
        raise HTTPException(status_code=500, detail=f"Error en la tienda: {e}")

    cart_id = uuid.uuid4().hex
    await db_save_cart(cart_id, cart)
    return JSONResponse({"cart_id": cart_id, **cart})


@app.post("/confirmar-compra")
async def confirmar_compra(request: Request):
    check_rate_limit(request.client.host)
    body = await request.json()
    cart_id = (body.get("cart_id") or "").strip()
    if not cart_id:
        raise HTTPException(status_code=400, detail="Se requiere 'cart_id'")

    cart = await db_load_cart(cart_id)
    if not cart:
        raise HTTPException(status_code=404, detail="Carrito no encontrado")
    if cart["status"] != "pending":
        raise HTTPException(status_code=400, detail=f"Carrito ya en estado '{cart['status']}'")

    try:
        ok = await asyncio.wait_for(shopper.confirm_checkout(cart["store"]), timeout=120)
    except Exception as e:
        log.error(f"confirm_checkout error: {e}")
        raise HTTPException(status_code=500, detail=f"Error al confirmar compra: {e}")

    new_status = "confirmed" if ok else "failed"
    await db_update_cart_status(cart_id, new_status)
    return JSONResponse({"ok": ok, "cart_id": cart_id, "status": new_status})


@app.get("/carrito")
async def carrito():
    cart = await db_latest_pending_cart()
    if not cart:
        return JSONResponse({"status": "none"})
    return JSONResponse(cart)


@app.delete("/carrito/{cart_id}")
async def cancelar_carrito(cart_id: str):
    await db_update_cart_status(cart_id, "cancelled")
    return JSONResponse({"ok": True})


@app.get("/pantalla")
async def pantalla():
    try:
        data = await _screenshot()
        return Response(content=data, media_type="image/png")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/pantalla/mouse")
async def pantalla_mouse(request: Request):
    body = await request.json()
    x = body.get("x")
    y = body.get("y")
    if x is None or y is None:
        raise HTTPException(status_code=400, detail="Se requieren 'x' e 'y'")
    try:
        await _ydotool("mousemove", str(int(x)), str(int(y)))
        return JSONResponse({"ok": True, "x": int(x), "y": int(y)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/pantalla/click")
async def pantalla_click(request: Request):
    body = await request.json()
    x = body.get("x")
    y = body.get("y")
    button = int(body.get("button", 1))
    try:
        if x is not None and y is not None:
            await _ydotool("mousemove", str(int(x)), str(int(y)))
        await _ydotool("click", str(button))
        return JSONResponse({"ok": True})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/pantalla/tipo")
async def pantalla_tipo(request: Request):
    body = await request.json()
    texto = body.get("texto", "")
    if not texto:
        raise HTTPException(status_code=400, detail="Se requiere 'texto'")
    try:
        # Pass text via env var to avoid shell injection in sg -c
        await _ydotool('type -- "$ZEUS_TYPE_TEXT"', env={"ZEUS_TYPE_TEXT": texto})
        return JSONResponse({"ok": True})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/pantalla/tecla")
async def pantalla_tecla(request: Request):
    body = await request.json()
    tecla = body.get("tecla", "")
    if not tecla:
        raise HTTPException(status_code=400, detail="Se requiere 'tecla'")
    if not re.fullmatch(r"[A-Za-z0-9_+\-]+", tecla):
        raise HTTPException(status_code=400, detail="Nombre de tecla inválido")
    try:
        await _ydotool("key", tecla)
        return JSONResponse({"ok": True})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
