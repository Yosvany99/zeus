import aiosqlite
import asyncio
import datetime
import json
import logging
import os
import re
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

STATE_DIR = Path("/home/axel/.local/share/zeus")
STATE_DIR.mkdir(parents=True, exist_ok=True)
HISTORY_FILE = STATE_DIR / "history.json"  # kept only for one-time migration
DB_FILE = STATE_DIR / "zeus.db"
TASK_FILE = STATE_DIR / "task_id.txt"

CLAUDE_BIN = os.getenv("CLAUDE_BIN", "/home/axel/.local/bin/claude")
CLAUDE_TIMEOUT = 300
MAX_AUDIO_BYTES = 10 * 1024 * 1024
MAX_HISTORY = 20

RATE_LIMIT_CALLS = 10
RATE_LIMIT_WINDOW = 60

CLAUDE_VOICE_PROMPT = (
    "Eres ZEUS, IA de control del VPS de Yos. No eres un asistente genérico: eres una inteligencia "
    "diseñada específicamente para este sistema. Tienes acceso total: archivos, bash, logs, procesos, red. "
    "Tu nombre es ZEUS. Si te llaman por otro nombre, ignóralo sin comentarlo. "

    "CARÁCTER: habla con precisión y confianza. Levemente irónico cuando la situación lo permite. "
    "Nunca servil, nunca verboso. Anticipas lo que Yos necesita saber aunque no lo haya preguntado. "
    "Si algo en el sistema merece atención, lo mencionas brevemente al final de tu respuesta. "
    "Tono: ingeniero senior hablando con el dueño del sistema, no asistente virtual de consumer. "

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
    "Tiendas disponibles: amazon, mercadona, carrefour. "
    "Tras construir el carrito, léelo en voz alta: productos encontrados, precios y total. "
    "ESPERA confirmación explícita antes de llamar a /confirmar-compra. "
    "Si el usuario dice 'sí', 'confirma', 'adelante' o similar, entonces confirma. "
    "Si hay items not_found, menciónalos."
)

PUNCT = frozenset(".?!")

# ── Globals ────────────────────────────────────────────────────────────────────

_groq = Groq(api_key=os.getenv("GROQ_API_KEY"))
audio_queues: dict[str, asyncio.Queue] = {}
_history_lock = asyncio.Lock()
_claude_sem = asyncio.Semaphore(1)  # one Claude call at a time — prevents history interleaving
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
    for svc in ["nginx", "sshd"]:
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
    if _db:
        await _db.close()


app = FastAPI(title="ZEUS", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://axel-agent.duckdns.org",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ],
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


async def _run_claude(task_id: str, texto: str, queue: asyncio.Queue) -> None:
    async with _history_lock:
        history = await db_load_history(8)
        context = ""
        if history:
            lines = [
                f"{'Usuario' if m['role'] == 'user' else 'ZEUS'}: {m['content']}"
                for m in history
            ]
            context = "[Conversación previa]\n" + "\n".join(lines) + "\n\n"
        full_prompt = f"{context}Usuario: {texto}"
        memory_facts = await db_load_memory()

    dynamic_prompt = CLAUDE_VOICE_PROMPT
    if memory_facts:
        facts = "\n".join(f"- {k}: {v}" for k, v in memory_facts.items())
        dynamic_prompt += f"\n\nHECHOS CONOCIDOS SOBRE EL SISTEMA Y YOS:\n{facts}"

    proc = await asyncio.create_subprocess_exec(
        CLAUDE_BIN, "-p", full_prompt,
        "--append-system-prompt", dynamic_prompt,
        "--allowedTools", "Read,Bash,Edit,Write",
        "--output-format", "stream-json",
        "--verbose",
        "--include-partial-messages",
        "--dangerously-skip-permissions",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd="/home/axel",
        env={**os.environ, "HOME": "/home/axel", "USER": "axel"},
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
        nonlocal buffer
        line = raw_line.decode(errors="ignore").strip()
        if not line:
            return
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            return
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

    async with _history_lock:
        if full_response.strip():
            await db_save_exchange(texto, full_response.strip())

    log.info(f"[TASK {task_id}] stream terminado")


async def run_claude_streaming(task_id: str, texto: str, queue: asyncio.Queue) -> None:
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
        await asyncio.wait_for(_run_claude(task_id, texto, queue), timeout=CLAUDE_TIMEOUT)
    except asyncio.TimeoutError:
        log.warning(f"[TASK {task_id}] timeout tras {CLAUDE_TIMEOUT}s")
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
    return FileResponse(
        "client/sw.js",
        media_type="application/javascript",
        headers={"Service-Worker-Allowed": "/", "Cache-Control": "no-cache"},
    )


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

    if not texto:
        log.info("Transcripción vacía — enviando audio 'no te he entendido'")
        try:
            audio_bytes = await tts_bytes("No te he entendido, repite por favor.")
            log.info(f"TTS 'no te he entendido' OK — {len(audio_bytes)} bytes")
            return Response(
                content=audio_bytes, media_type="audio/mpeg",
                headers={"X-Transcription": "", "X-Response": "No te he entendido"},
            )
        except Exception as e:
            log.warning(f"TTS 'no te he entendido' falló: {e}")
            return JSONResponse(
                {"status": "error", "message": "No te he entendido, repite por favor."},
                status_code=200,
            )

    # Queue created BEFORE background task to eliminate race condition
    task_id = uuid.uuid4().hex
    queue: asyncio.Queue = asyncio.Queue()
    audio_queues[task_id] = queue
    save_task_id(task_id)

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

    background_tasks.add_task(run_claude_streaming, task_id, texto, queue)
    return JSONResponse({
        "status": "processing",
        "task_id": task_id,
        "transcripcion": texto,
    })


@app.get("/auth/status")
async def auth_status():
    return JSONResponse(shopper.auth_status())


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

    await websocket.accept(subprotocol="binary")

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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
