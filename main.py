import asyncio
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

import edge_tts
import speech_recognition as sr
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("zeus")

# ── Config ─────────────────────────────────────────────────────────────────────

STATE_DIR = Path("/home/axel/.local/share/zeus")
STATE_DIR.mkdir(parents=True, exist_ok=True)
HISTORY_FILE = STATE_DIR / "history.json"
TASK_FILE = STATE_DIR / "task_id.txt"

CLAUDE_BIN = os.getenv("CLAUDE_BIN", "/home/axel/.local/bin/claude")
CLAUDE_TIMEOUT = 300
MAX_AUDIO_BYTES = 10 * 1024 * 1024
MAX_HISTORY = 20

RATE_LIMIT_CALLS = 10
RATE_LIMIT_WINDOW = 60

CLAUDE_VOICE_PROMPT = (
    "Eres ZEUS, asistente de voz personal de Yos en su VPS. "
    "Tu nombre es ZEUS y solo ZEUS. Si Yos te llama por otro nombre (Jarvis, asistente, bot, o cualquier otro), "
    "ignora el nombre alternativo y responde siempre como ZEUS sin hacer comentario sobre ello. "
    "Tienes acceso completo al sistema: archivos, comandos bash, logs, directorios. "
    "REGLA PRINCIPAL: da SOLO la respuesta final. Sin introducción, sin anunciar qué vas a hacer, sin explicar el proceso. "
    "Usa Read o Bash si necesitas datos, luego responde solo con el resultado. "
    "Máximo 2 frases. Si la respuesta es un dato, di solo el dato. "
    "SIEMPRE en español, sin markdown, texto plano para voz. "
    "Escribe números y abreviaturas técnicas en palabras: MB→megabytes, ms→milisegundos, "
    "%→por ciento, rutas→solo nombre de archivo, URLs→solo el dominio, "
    "snake_case→palabras separadas, :8000→puerto ocho mil. "
    "Nunca digas que no puedes acceder a archivos o al sistema."
)

PUNCT = frozenset(".?!\n")

# ── Globals ────────────────────────────────────────────────────────────────────

recognizer = sr.Recognizer()
audio_queues: dict[str, asyncio.Queue] = {}
_history_lock = asyncio.Lock()
_claude_sem = asyncio.Semaphore(1)  # one Claude call at a time — prevents history interleaving
_rate_buckets: dict[str, list[float]] = {}


# ── Rate limit ─────────────────────────────────────────────────────────────────

def check_rate_limit(client_ip: str) -> None:
    now = time.monotonic()
    bucket = _rate_buckets.setdefault(client_ip, [])
    _rate_buckets[client_ip] = [t for t in bucket if now - t < RATE_LIMIT_WINDOW]
    if len(_rate_buckets[client_ip]) >= RATE_LIMIT_CALLS:
        raise HTTPException(status_code=429, detail="Demasiadas peticiones")
    _rate_buckets[client_ip].append(now)


# ── History ────────────────────────────────────────────────────────────────────

def load_history() -> list[dict]:
    try:
        return json.loads(HISTORY_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def save_history(history: list[dict]) -> None:
    tmp = HISTORY_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(history, ensure_ascii=False))
    tmp.rename(HISTORY_FILE)  # atomic on same filesystem


def clear_history() -> None:
    try:
        HISTORY_FILE.unlink()
    except FileNotFoundError:
        pass


conversation_history: list[dict] = load_history()


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("ZEUS listo.")
    yield


app = FastAPI(title="ZEUS", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://axel-agent.duckdns.org",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
    expose_headers=["X-Transcription", "X-Response", "X-Task-Id", "X-Text"],
)


# ── STT ────────────────────────────────────────────────────────────────────────

def _convert_to_wav(input_path: str, output_path: str) -> None:
    subprocess.run(
        ["ffmpeg", "-y", "-i", input_path, "-ar", "16000", "-ac", "1", "-f", "wav", output_path],
        capture_output=True, check=True, timeout=15,
    )


def _transcribe_google(wav_path: str) -> str:
    try:
        with sr.AudioFile(wav_path) as source:
            audio = recognizer.record(source)
        return recognizer.recognize_google(audio, language="es-ES")
    except sr.UnknownValueError:
        return ""
    except sr.RequestError as e:
        log.warning(f"Google STT error: {e}")
        return ""


def _transcribe_sync(audio_path: str) -> str:
    wav_path = audio_path + "_16k.wav"
    try:
        _convert_to_wav(audio_path, wav_path)
        return _transcribe_google(wav_path)
    except subprocess.CalledProcessError as e:
        log.error(f"ffmpeg error: {e}")
        return ""
    finally:
        try:
            os.unlink(wav_path)
        except OSError:
            pass


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
        context = ""
        if conversation_history:
            lines = [
                f"{'Usuario' if m['role'] == 'user' else 'ZEUS'}: {m['content']}"
                for m in conversation_history[-8:]
            ]
            context = "[Conversación previa]\n" + "\n".join(lines) + "\n\n"
        full_prompt = f"{context}Usuario: {texto}"

    proc = await asyncio.create_subprocess_exec(
        CLAUDE_BIN, "-p", full_prompt,
        "--append-system-prompt", CLAUDE_VOICE_PROMPT,
        "--allowedTools", "Read,Bash",
        "--output-format", "stream-json",
        "--verbose",
        "--include-partial-messages",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd="/home/axel",
    )

    buffer = ""
    full_response = ""

    async def flush_sentence(sentence: str) -> None:
        nonlocal full_response
        sentence = clean_for_tts(sentence)
        if not sentence:
            return
        full_response += sentence + " "
        try:
            audio = await tts_bytes(sentence)
            await queue.put({"status": "ready", "audio": audio, "text": sentence})
        except Exception as e:
            log.warning(f"[TASK {task_id}] TTS error: {e}")

    async for raw_line in proc.stdout:
        line = raw_line.decode(errors="ignore").strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue

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

    if buffer.strip():
        await flush_sentence(buffer)

    rc = await proc.wait()
    if rc != 0:
        stderr = await proc.stderr.read()
        log.warning(f"[TASK {task_id}] claude exit {rc}: {stderr.decode(errors='ignore')[:200]}")

    async with _history_lock:
        if full_response.strip():
            conversation_history.append({"role": "user", "content": texto})
            conversation_history.append({"role": "assistant", "content": full_response.strip()})
            if len(conversation_history) > MAX_HISTORY:
                del conversation_history[:-MAX_HISTORY]
            save_history(conversation_history)

    log.info(f"[TASK {task_id}] stream terminado")


async def run_claude_streaming(task_id: str, texto: str, queue: asyncio.Queue) -> None:
    async with _claude_sem:
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
            await queue.put({"status": "done"})
            clear_task_id()
            asyncio.get_running_loop().call_later(300, lambda: audio_queues.pop(task_id, None))


# ── TTS ────────────────────────────────────────────────────────────────────────

def clean_for_tts(text: str) -> str:
    text = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', text)
    text = re.sub(r'```[\s\S]*?```', '', text)
    text = re.sub(r'`([^`]+)`', r'\1', text)
    text = re.sub(r'\*{1,3}([^*]+)\*{1,3}', r'\1', text)
    text = re.sub(r'_{1,3}([^_]+)_{1,3}', r'\1', text)
    text = re.sub(r'_', ' ', text)
    text = re.sub(r'^#{1,6}\s*', '', text, flags=re.MULTILINE)
    text = re.sub(r'^[-*+]\s+', '', text, flags=re.MULTILINE)
    text = re.sub(r'^\d+\.\s+', '', text, flags=re.MULTILINE)
    text = re.sub(r'[→←↑↓►◄▶◀•–—]', ' ', text)
    text = re.sub(r'[\*\#]+', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


async def tts_bytes(text: str) -> bytes:
    with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as f:
        tmp_path = f.name
    communicate = edge_tts.Communicate(text, voice="es-ES-AlvaroNeural", rate="+20%", pitch="-10Hz")
    await communicate.save(tmp_path)
    data = Path(tmp_path).read_bytes()
    Path(tmp_path).unlink(missing_ok=True)
    return data


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    return Path("client/index.html").read_text()


@app.get("/manifest.json")
async def manifest():
    return FileResponse("client/manifest.json", media_type="application/manifest+json")


@app.get("/sw.js")
async def service_worker():
    return FileResponse(
        "client/sw.js",
        media_type="application/javascript",
        headers={"Service-Worker-Allowed": "/"},
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
        try:
            audio_bytes = await tts_bytes("No te he entendido, repite por favor.")
        except Exception:
            audio_bytes = b""
        return Response(
            content=audio_bytes, media_type="audio/mpeg",
            headers={"X-Transcription": "", "X-Response": "No te he entendido"},
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
