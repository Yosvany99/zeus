"""
Shopping automation for Zeus.
Strategy:
  - Mercadona: REST API (httpx) — no browser needed
  - Amazon / Carrefour: Playwright with system Chromium (optional)
"""
import asyncio
import json
import logging
import os
import secrets
import shutil
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import base64
import httpx

log = logging.getLogger("zeus.shopper")

SESSIONS_DIR = Path(os.getenv("ZEUS_STATE_DIR", str(Path.home() / ".local/share/zeus"))) / "sessions"
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

# ── Live event feed (polled by /shopper-log endpoint in main.py) ──────────────
_shopper_log: list[dict] = []

def clear_shopper_log() -> None:
    _shopper_log.clear()

def _emit(event: dict) -> None:
    _shopper_log.append(event)
    if len(_shopper_log) > 300:
        del _shopper_log[:100]

_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


@dataclass
class CartItem:
    name: str
    price: float
    qty: int = 1


@dataclass
class Cart:
    store: str
    items: list
    total: float
    currency: str = "EUR"
    not_found: list = None

    def to_dict(self) -> dict:
        return {
            "store": self.store,
            "items": [asdict(i) for i in self.items],
            "total": self.total,
            "currency": self.currency,
            "not_found": self.not_found or [],
        }


def _parse_price(text: str) -> float:
    import re
    cleaned = re.sub(r"[^\d,.]", "", text).replace(",", ".")
    parts = cleaned.split(".")
    if len(parts) > 2:
        cleaned = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _env(key: str, store: str) -> str:
    val = os.getenv(key, "").strip()
    if not val:
        raise ValueError(f"{key} no configurado en .env — necesario para {store}")
    return val


# ── Mercadona REST API ─────────────────────────────────────────────────────────
# Reverse-engineered from tienda.mercadona.es SPA (v8974).
# Auth:   POST /api/auth/tokens/  → {"token": "...", "uuid": "..."}
# Search: Algolia index "products_prod" (app 7UZJKL1DJ0)
# Cart:   POST/GET /api/carts/    → Bearer token required

_MERC_BASE    = "https://tienda.mercadona.es/api"
_MERC_SESSION = SESSIONS_DIR / "mercadona_token.json"

_ALGOLIA_APP   = "7UZJKL1DJ0"
_ALGOLIA_KEY   = "9d8f2e39e90df472b4f2e559a116fe17"
_ALGOLIA_INDEX = "products_prod"
_ALGOLIA_URL   = f"https://{_ALGOLIA_APP}-dsn.algolia.net/1/indexes/{_ALGOLIA_INDEX}/query"


def _merc_headers(token: str | None = None) -> dict:
    h = {
        "User-Agent": _UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "es-ES,es;q=0.9",
        "Referer": "https://tienda.mercadona.es/",
        "Origin": "https://tienda.mercadona.es",
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


async def _merc_login(client: httpx.AsyncClient, email: str = None, password: str = None) -> str:
    """Log in and return JWT token. Saves token to disk."""
    email    = email    or os.getenv("MERCADONA_EMAIL",    "").strip()
    password = password or os.getenv("MERCADONA_PASSWORD", "").strip()
    if not email or not password:
        raise ValueError("Credenciales de Mercadona no disponibles")

    r = await client.post(
        f"{_MERC_BASE}/auth/tokens/",
        json={"username": email, "password": password},
        headers=_merc_headers(),
    )
    if r.status_code not in (200, 201):
        body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        detail = (body.get("errors") or [{}])[0].get("detail", r.text[:120])
        raise RuntimeError(f"Mercadona login falló: {detail}")

    data = r.json()
    token = data.get("token") or data.get("access") or data.get("access_token")
    if not token:
        raise RuntimeError(f"Mercadona: token no encontrado en respuesta: {list(data.keys())}")

    _MERC_SESSION.write_text(json.dumps({"token": token}))
    return token


async def _merc_get_token(client: httpx.AsyncClient, email: str = None, password: str = None) -> str:
    """Return cached token or re-login."""
    if _MERC_SESSION.exists():
        try:
            return json.loads(_MERC_SESSION.read_text())["token"]
        except Exception:
            pass
    return await _merc_login(client, email=email, password=password)


async def _merc_search(client: httpx.AsyncClient, query: str) -> dict | None:
    """Search via Algolia and return best matching product."""
    r = await client.post(
        _ALGOLIA_URL,
        json={"query": query, "hitsPerPage": 5},
        headers={
            "X-Algolia-API-Key": _ALGOLIA_KEY,
            "X-Algolia-Application-Id": _ALGOLIA_APP,
            "Content-Type": "application/json",
        },
    )
    if r.status_code != 200:
        log.warning(f"Mercadona Algolia search '{query}' status {r.status_code}")
        return None
    hits = r.json().get("hits", [])
    return hits[0] if hits else None


class _MercTokenExpired(Exception):
    """Raised when the cached Mercadona token is rejected (401)."""


async def _merc_ensure_postal(client: httpx.AsyncClient, token: str) -> None:
    """Best-effort: bind the session to a warehouse via postal code.
    Mercadona scopes the cart to a warehouse; without it, adds can fail."""
    code = os.getenv("MERCADONA_POSTAL", "").strip()
    if not code:
        return
    try:
        r = await client.put(
            f"{_MERC_BASE}/customers/postal-codes/",
            json={"new_postal_code": code},
            headers=_merc_headers(token),
        )
        if r.status_code not in (200, 201, 204):
            await client.get(f"{_MERC_BASE}/postal-codes/{code}/", headers=_merc_headers(token))
        log.info(f"Mercadona: codigo postal {code} aplicado (HTTP {r.status_code})")
    except Exception as e:
        log.warning(f"Mercadona: no se pudo fijar el codigo postal: {e}")


async def _merc_add_to_cart(client: httpx.AsyncClient, token: str, product: dict) -> bool:
    """Add product to Mercadona cart via API. Raises _MercTokenExpired on 401."""
    pid = product.get("id")
    if not pid:
        return False
    r = await client.post(
        f"{_MERC_BASE}/carts/",
        json={"id": pid, "amount": 1},
        headers=_merc_headers(token),
    )
    if r.status_code == 401:
        raise _MercTokenExpired()
    if r.status_code not in (200, 201, 204):
        log.warning(
            f"Mercadona: alta en carrito HTTP {r.status_code} "
            f"para '{product.get('display_name') or pid}': {r.text[:200]}"
        )
    return r.status_code in (200, 201, 204)


def _merc_extract_price(product: dict) -> float:
    price_data = product.get("price_instructions") or {}
    unit = price_data.get("unit_price") or price_data.get("bulk_price") or "0"
    try:
        return float(unit)
    except (ValueError, TypeError):
        return 0.0


async def _build_cart_mercadona(items: list[str]) -> Cart:
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        token = await _merc_get_token(client)
        await _merc_ensure_postal(client, token)

        found: list[CartItem] = []
        not_found: list[str] = []

        for query in items:
            product = await _merc_search(client, query)
            if product is None:
                not_found.append(query)
                log.warning(f"Mercadona: '{query}' no encontrado")
                continue

            name  = product.get("display_name") or product.get("name") or query
            price = _merc_extract_price(product)
            try:
                added = await _merc_add_to_cart(client, token, product)
            except _MercTokenExpired:
                log.info("Mercadona: token caducado — re-login y reintento")
                token = await _merc_login(client)
                await _merc_ensure_postal(client, token)
                try:
                    added = await _merc_add_to_cart(client, token, product)
                except _MercTokenExpired:
                    added = False
            if added:
                found.append(CartItem(name=name, price=price))
                log.info(f"Mercadona: añadido '{name}' @ {price}€")
            else:
                not_found.append(query)
                log.warning(f"Mercadona: no se pudo añadir '{name}' al carrito")

        total = round(sum(i.price * i.qty for i in found), 2)
        return Cart(store="mercadona", items=found, total=total, not_found=not_found)


async def _confirm_mercadona() -> bool:
    """Place the order via Mercadona API."""
    if not _MERC_SESSION.exists():
        raise RuntimeError("Sin sesión de Mercadona. Construye el carrito primero.")

    token = json.loads(_MERC_SESSION.read_text())["token"]
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        r = await client.get(f"{_MERC_BASE}/carts/", headers=_merc_headers(token))
        if r.status_code == 401:
            token = await _merc_login(client)
            r = await client.get(f"{_MERC_BASE}/carts/", headers=_merc_headers(token))
        if r.status_code != 200:
            log.warning(f"Mercadona: carrito no disponible HTTP {r.status_code}: {r.text[:200]}")
            return False

        r2 = await client.post(
            f"{_MERC_BASE}/orders/",
            json={},
            headers=_merc_headers(token),
        )
        if r2.status_code not in (200, 201):
            log.warning(f"Mercadona: orders/ HTTP {r2.status_code}: {r2.text[:300]}")
        return r2.status_code in (200, 201)


# ── Playwright-based stores (Amazon, Carrefour) ────────────────────────────────

_BROWSER_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-infobars",
    "--disable-dev-shm-usage",
]

_STEALTH_SCRIPT = (
    "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
)


def _chromium_executable() -> str | None:
    """Find Chromium or Google Chrome. Returns None if unavailable."""
    candidates = [
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/opt/google/chrome/google-chrome",
        "/usr/bin/chromium-browser",
        "/usr/bin/chromium",
        "/snap/bin/chromium",
        shutil.which("google-chrome"),
        shutil.which("google-chrome-stable"),
        shutil.which("chromium-browser"),
        shutil.which("chromium"),
    ]
    for p in candidates:
        if p and os.path.exists(p):
            return p
    return None


async def _pw_build_cart(store: str, items: list[str]) -> Cart:
    """Playwright-based cart builder for Amazon/Carrefour."""
    chromium = _chromium_executable()
    if not chromium:
        raise RuntimeError(
            f"Chromium no disponible para {store}. "
            "Instalar con: sudo apt install chromium-browser"
        )

    from playwright.async_api import async_playwright

    login_fn, add_fn, _ = _PW_STORES[store]
    session_file = SESSIONS_DIR / f"{store}.json"

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            executable_path=chromium,
            args=_BROWSER_ARGS,
        )
        ctx = await browser.new_context(
            storage_state=str(session_file) if session_file.exists() else None,
            user_agent=_UA,
            viewport={"width": 1280, "height": 720},
        )
        await ctx.add_init_script(_STEALTH_SCRIPT)
        page = await ctx.new_page()
        try:
            if not session_file.exists():
                await login_fn(page)
                await ctx.storage_state(path=str(session_file))

            found: list[CartItem] = []
            not_found: list[str] = []
            for q in items:
                item = await add_fn(page, q)
                if item:
                    found.append(item)
                    log.info(f"{store}: añadido '{item.name}' @ {item.price}€")
                else:
                    not_found.append(q)

            await ctx.storage_state(path=str(session_file))
            total = round(sum(i.price * i.qty for i in found), 2)
            return Cart(store=store, items=found, total=total, not_found=not_found)
        finally:
            await browser.close()


async def _pw_confirm(store: str) -> bool:
    chromium = _chromium_executable()
    if not chromium:
        return False

    from playwright.async_api import async_playwright

    _, _, checkout_fn = _PW_STORES[store]
    session_file = SESSIONS_DIR / f"{store}.json"

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            executable_path=chromium,
            args=_BROWSER_ARGS,
        )
        ctx = await browser.new_context(
            storage_state=str(session_file) if session_file.exists() else None,
            user_agent=_UA,
            viewport={"width": 1280, "height": 720},
        )
        await ctx.add_init_script(_STEALTH_SCRIPT)
        page = await ctx.new_page()
        try:
            ok = await checkout_fn(page)
            await ctx.storage_state(path=str(session_file))
            return ok
        finally:
            await browser.close()


# ── Amazon ─────────────────────────────────────────────────────────────────────

async def _amazon_login(page, email: str = None, password: str = None) -> None:
    session_file = SESSIONS_DIR / "amazon.json"
    if session_file.exists():
        return  # session loaded via storage_state
    email    = email    or os.getenv("AMAZON_EMAIL",    "").strip()
    password = password or os.getenv("AMAZON_PASSWORD", "").strip()
    if not email or not password:
        raise ValueError("Credenciales de Amazon no disponibles")
    await page.goto("https://www.amazon.es/ap/signin?openid.return_to=https://www.amazon.es&openid.mode=checkid_setup&openid.ns=http://specs.openid.net/auth/2.0", wait_until="domcontentloaded")
    await page.locator('#ap_email').fill(email)
    await page.locator('#continue').click()
    await page.wait_for_selector('#ap_password', timeout=6000)
    await page.locator('#ap_password').fill(password)
    await page.locator('#signInSubmit').click()
    await page.wait_for_load_state("networkidle")


async def _amazon_add(page, query: str) -> CartItem | None:
    from urllib.parse import quote
    await page.goto(f"https://www.amazon.es/s?k={quote(query)}", wait_until="domcontentloaded")
    await page.wait_for_timeout(2000)
    try:
        await page.wait_for_selector('[data-component-type="s-search-result"]', timeout=8000)
        result = page.locator('[data-component-type="s-search-result"]').first
        name = (await result.locator('h2 span').first.inner_text()).strip()
        whole = (await result.locator('.a-price-whole').first.inner_text()).strip()
        frac = (await result.locator('.a-price-fraction').first.inner_text()).strip()
        price = _parse_price(whole + "." + frac)
        link = await result.locator('h2 a').first.get_attribute('href')
        await page.goto(f"https://www.amazon.es{link}", wait_until="domcontentloaded")
        await page.locator('#add-to-cart-button').first.click(timeout=5000)
        await page.wait_for_timeout(800)
        return CartItem(name=name, price=price)
    except Exception as e:
        log.warning(f"Amazon add '{query}': {e}")
        return None


async def _amazon_checkout(page) -> bool:
    try:
        await page.goto("https://www.amazon.es/gp/cart/view.html", wait_until="networkidle")
        await page.locator('[name="proceedToRetailCheckout"]').first.click(timeout=6000)
        await page.wait_for_load_state("networkidle")
        return True
    except Exception as e:
        log.warning(f"Amazon checkout: {e}")
        return False


# ── Alcampo ────────────────────────────────────────────────────────────────────

async def _alcampo_login(page, email: str = None, password: str = None) -> None:
    session_file = SESSIONS_DIR / "alcampo.json"
    if session_file.exists():
        return
    email    = email    or os.getenv("ALCAMPO_EMAIL",    "").strip()
    password = password or os.getenv("ALCAMPO_PASSWORD", "").strip()
    if not email or not password:
        raise ValueError("Credenciales de Alcampo no disponibles")
    # Login redirects through Salesforce OAuth; Playwright follows cross-domain redirects
    await page.goto("https://www.compraonline.alcampo.es/login", wait_until="domcontentloaded")
    await page.wait_for_timeout(2000)
    # Dismiss cookie/GDPR banner if present
    for sel in (
        '#onetrust-accept-btn-handler',
        'button:has-text("Aceptar todas")',
        'button:has-text("Aceptar todo")',
        'button:has-text("Aceptar")',
    ):
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=2000):
                await btn.click()
                await page.wait_for_timeout(800)
                break
        except Exception:
            pass
    # Wait for OAuth redirect to settle and email field to appear
    await page.wait_for_selector(
        '#username, input[name="username"], input[type="email"]', timeout=15000
    )
    email_field = page.locator('#username, input[name="username"], input[type="email"]').first
    await email_field.fill(email)
    await email_field.press("Enter")
    await page.wait_for_timeout(1500)
    # Password may be on same page or appear after "next" step
    await page.wait_for_selector('#password, input[type="password"]', timeout=10000)
    pwd_field = page.locator('#password, input[type="password"]').first
    await pwd_field.fill(password)
    await pwd_field.press("Enter")
    await page.wait_for_load_state("networkidle")


async def _alcampo_add(page, query: str) -> CartItem | None:
    from urllib.parse import quote
    await page.goto(
        f"https://www.compraonline.alcampo.es/search?q={quote(query)}",
        wait_until="domcontentloaded",
    )
    await page.wait_for_timeout(2500)
    try:
        await page.wait_for_selector('a[href*="/products/"]', timeout=9000)
        prod = page.locator('a[href*="/products/"]').first
        name_el = prod.locator('h3, [class*="name"], [class*="title"], [class*="Name"]').first
        name = (await name_el.inner_text()).strip()
        try:
            price_raw = (await prod.locator('[class*="price"], [class*="Price"]').first.inner_text()).strip()
            price = _parse_price(price_raw)
        except Exception:
            price = 0.0
        await page.locator('button:has-text("Añadir")').first.click(timeout=5000)
        await page.wait_for_timeout(700)
        return CartItem(name=name, price=price)
    except Exception as e:
        log.warning(f"Alcampo add '{query}': {e}")
        return None


async def _alcampo_checkout(page) -> bool:
    try:
        await page.goto("https://www.compraonline.alcampo.es/basket", wait_until="networkidle")
        await page.locator(
            'button:has-text("Tramitar"), button:has-text("Finalizar"), '
            'button:has-text("Ir al pago"), a:has-text("Tramitar"), a:has-text("Pagar")'
        ).first.click(timeout=6000)
        await page.wait_for_load_state("networkidle")
        return True
    except Exception as e:
        log.warning(f"Alcampo checkout: {e}")
        return False


# ── Carrefour ──────────────────────────────────────────────────────────────────

_GROQ_VISION_MODEL = "meta-llama/llama-4-scout-17b-16e-instruct"
_SYSTEM_NAV = """\
Eres un agente de navegación web. Recibirás un screenshot y una lista de elementos \
interactivos visibles en la página (con su texto exacto). Tu trabajo es decidir qué hacer.

Responde SOLO con JSON válido, sin texto extra:
{
  "action": "click_text" | "type" | "done" | "back" | "navigate",
  "target": "<texto exacto del elemento a clicar, copiado literalmente de la lista>",
  "text": "<texto a escribir, solo para type>",
  "url": "<url completa, solo para navigate>",
  "reason": "<frase corta>"
}
Reglas:
- "click_text": elige el texto EXACTO del elemento de la lista que debes pulsar
- "type": escribe en el campo actualmente enfocado (SIEMPRE haz click_text sobre el campo input primero para enfocarlo)
- Los campos de formulario (email, contraseña) aparecen en la lista con su placeholder como texto; haz click_text sobre ellos antes de type
- "back": la página es incorrecta, vuelve atrás
- "navigate": ve directamente a esa URL
- "done": sesión iniciada correctamente
- NUNCA elijas "Crear cuenta", "Registrarse" ni similares si el objetivo es iniciar sesión
"""


async def _vision_step(page, goal: str, history: list[str]) -> dict:
    """Screenshot + lista DOM → Groq decide la acción → ejecutamos por texto exacto."""
    from groq import AsyncGroq
    api_key = os.getenv("GROQ_API_KEY", "")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY no configurada")

    png = await page.screenshot(type="png", full_page=False)
    b64 = base64.b64encode(png).decode()

    # Build a list of all visible clickable texts for the LLM to choose from
    els = await _page_clickables(page)
    visible_texts = [e["text"] for e in els if e["text"].strip()]
    log.info(f"[vision] {len(visible_texts)} clickables: {visible_texts[:15]}")
    _emit({"type": "scan", "text": f"Escaneando página: {len(visible_texts)} elementos interactivos", "url": page.url, "elements": visible_texts[:20]})
    el_list = "\n".join(f'  - "{t}"' for t in visible_texts) or "  (ninguno detectado)"

    history_text = ""
    if history:
        history_text = "Pasos ya realizados:\n" + "\n".join(f"  {i+1}. {h}" for i, h in enumerate(history)) + "\n"

    client = AsyncGroq(api_key=api_key)
    resp = await client.chat.completions.create(
        model=_GROQ_VISION_MODEL,
        max_tokens=300,
        messages=[
            {"role": "system", "content": _SYSTEM_NAV},
            {"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}},
                {"type": "text", "text": (
                    f"Objetivo: {goal}\n"
                    f"URL actual: {page.url}\n"
                    f"{history_text}"
                    f"Elementos interactivos visibles:\n{el_list}\n\n"
                    "¿Cuál es la siguiente acción? Elige 'target' copiando el texto exacto de la lista."
                )},
            ]},
        ],
    )
    raw = resp.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    result = json.loads(raw)
    _typed = result.get("text", "")
    _emit({"type": "decision", "action": result.get("action"), "target": result.get("target", ""), "text": ("•••" if _typed else ""), "reason": result.get("reason", "")})
    return result


async def _vision_navigate(page, goal: str, max_steps: int = 12) -> None:
    """Drive the browser using LLM vision to decide WHAT, DOM to execute WHERE."""
    _emit({"type": "start", "text": "Iniciando navegador visual con IA..."})
    history: list[str] = []
    _last_action_key = None
    _repeat_count = 0
    for step in range(max_steps):
        _emit({"type": "step_start", "step": step + 1, "max": max_steps, "text": f"Paso {step + 1} / {max_steps}"})
        action = await _vision_step(page, goal, history)
        act    = action.get("action", "")
        reason = action.get("reason", "")
        log.info(f"[vision step {step+1}] action={act} target='{action.get('target','')}' — {reason}")
        history.append(f"{act} '{action.get('target', action.get('text', ''))}' — {reason}")

        # Loop detection: same click 3 times → force navigate back to login
        action_key = f"{act}:{action.get('target','')}"
        if action_key == _last_action_key:
            _repeat_count += 1
        else:
            _repeat_count = 0
            _last_action_key = action_key
        if _repeat_count >= 2:
            _emit({"type": "step", "icon": "⚠", "text": "Loop detectado — volviendo al formulario de login"})
            _recover = ("/".join(page.url.split("/")[:3]) if "://" in page.url else "https://www.carrefour.es") + "/login"
            await page.goto(_recover, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)
            _repeat_count = 0
            _last_action_key = None
            continue

        if act == "done":
            log.info("[vision] objetivo completado")
            _emit({"type": "done", "text": "✓ Login completado correctamente"})
            return

        elif act == "back":
            await page.go_back()
            await page.wait_for_timeout(1500)

        elif act == "navigate":
            await page.goto(action["url"], wait_until="domcontentloaded")
            await page.wait_for_timeout(1500)

        elif act == "click_text":
            target = action.get("target", "").strip()
            els = await _page_clickables(page)
            # Exact match first, then case-insensitive, then partial
            matched = (
                next((e for e in els if e["text"] == target), None)
                or next((e for e in els if e["text"].lower() == target.lower()), None)
                or next((e for e in els if target.lower() in e["text"].lower()), None)
            )
            if matched:
                log.info(f"[vision] clicking '{matched['text']}' at ({matched['x']},{matched['y']})")
                await page.mouse.click(matched["x"], matched["y"])
                await page.wait_for_timeout(1400)
            else:
                log.warning(f"[vision] '{target}' no encontrado entre los {len(els)} elementos visibles — saltando paso")

        elif act == "type":
            await page.keyboard.type(action["text"], delay=50)
            await page.wait_for_timeout(500)

        else:
            log.warning(f"[vision] acción desconocida: {act}")

    _emit({"type": "error", "text": f"No se completó el login en {max_steps} pasos"})
    raise RuntimeError(f"[vision] objetivo no completado en {max_steps} pasos")


async def _page_clickables(page) -> list[dict]:
    """Return all visible clickable elements with their label and coordinates.
    Icon-only elements get a label from aria-label, title, alt, data-testid, or class.
    """
    return await page.evaluate("""() => {
        const results = [];
        const els = document.querySelectorAll(
            'a, button, [role="button"], [role="link"], input[type="submit"],' +
            'input[type="text"], input[type="email"], input[type="password"],' +
            'input[type="search"], input[type="tel"], textarea, [tabindex]'
        );
        const seen = new Set();
        for (const el of els) {
            if (seen.has(el)) continue;
            seen.add(el);
            const r = el.getBoundingClientRect();
            if (r.width < 1 || r.height < 1) continue;
            if (r.bottom < 0 || r.top > window.innerHeight) continue;
            const style = getComputedStyle(el);
            if (style.visibility === 'hidden' || style.display === 'none' || style.opacity === '0') continue;

            // Primary: visible text
            let text = (el.innerText || '').trim();

            // Fallback chain for icon-only / form elements
            if (!text) text = el.getAttribute('aria-label') || '';
            if (!text) text = el.getAttribute('placeholder') || '';
            if (!text) text = el.getAttribute('title') || '';
            if (!text) {
                const img = el.querySelector('img[alt]');
                if (img) text = img.getAttribute('alt') || '';
            }
            if (!text) {
                const svgTitle = el.querySelector('svg title');
                if (svgTitle) text = svgTitle.textContent || '';
            }
            if (!text) text = el.getAttribute('data-testid') || '';
            if (!text) text = el.getAttribute('data-test') || '';
            if (!text) text = el.getAttribute('name') || '';
            if (!text) {
                // Last resort: first meaningful CSS class
                const cls = Array.from(el.classList).find(
                    c => c.length > 3 && !/^(btn|icon|svg|wrap|container|flex|grid|row|col|d-)$/i.test(c)
                );
                if (cls) text = '[' + cls + ']';
            }
            if (!text) continue;

            results.push({
                tag:  el.tagName.toLowerCase(),
                text: text.trim(),
                href: el.href || '',
                x:    Math.round(r.x + r.width / 2),
                y:    Math.round(r.y + r.height / 2),
            });
        }
        return results;
    }""")


async def _smart_click(page, want: list[str], avoid: list[str] = None, timeout_ms: int = 2000) -> str | None:
    """
    Click the first visible element whose text contains a word from `want`
    but does NOT contain any word from `avoid`.
    Returns the matched text, or None if nothing found.
    """
    avoid_lower = [a.lower() for a in (avoid or [])]
    await page.wait_for_timeout(timeout_ms)
    els = await _page_clickables(page)
    log.info(f"[smart_click] page has {len(els)} clickables. want={want} avoid={avoid_lower}")
    for w in want:
        w_lower = w.lower()
        for el in els:
            t = el["text"].lower()
            if w_lower in t and not any(av in t for av in avoid_lower):
                log.info(f"[smart_click] clicking '{el['text']}' at ({el['x']},{el['y']})")
                await page.mouse.click(el["x"], el["y"])
                return el["text"]
    log.warning(f"[smart_click] nothing matched want={want}")
    return None


async def _smart_fill(page, want_labels: list[str], value: str, input_type: str = None) -> bool:
    """Fill the first visible input whose label/placeholder/name contains a word from want_labels."""
    type_filter = f'input[type="{input_type}"]' if input_type else "input, textarea"
    inputs = await page.evaluate(f"""() => {{
        const results = [];
        for (const el of document.querySelectorAll('{type_filter}')) {{
            const r = el.getBoundingClientRect();
            if (r.width < 1 || r.height < 1 || r.top > window.innerHeight) continue;
            const style = getComputedStyle(el);
            if (style.visibility === 'hidden' || style.display === 'none') continue;
            results.push({{
                type:        el.type || '',
                placeholder: el.placeholder || '',
                name:        el.name || '',
                id:          el.id || '',
                x:           Math.round(r.x + r.width / 2),
                y:           Math.round(r.y + r.height / 2),
            }});
        }}
        return results;
    }}""")
    for w in want_labels:
        w_lower = w.lower()
        for inp in inputs:
            haystack = f"{inp['placeholder']} {inp['name']} {inp['id']} {inp['type']}".lower()
            if w_lower in haystack:
                log.info(f"[smart_fill] filling input name='{inp['name']}' placeholder='{inp['placeholder']}'")
                await page.mouse.click(inp["x"], inp["y"])
                await page.wait_for_timeout(200)
                await page.keyboard.press("Control+a")
                await page.keyboard.type(value, delay=40)
                return True
    log.warning(f"[smart_fill] no input matched want={want_labels}")
    return False


async def _page_text(page) -> str:
    """Return all visible text on the page, lowercased."""
    try:
        return (await page.evaluate("() => document.body.innerText")).lower()
    except Exception:
        return ""


async def _on_wrong_page(page, wrong_signals: list[str], good_signals: list[str]) -> bool:
    """
    Return True if the current page looks wrong.
    wrong_signals: words that indicate a bad page (e.g. ["crear cuenta", "registrarse"])
    good_signals:  words that indicate the correct page (e.g. ["contraseña", "iniciar sesión"])
    A page is wrong if it has wrong_signals AND lacks good_signals.
    """
    text = await _page_text(page)
    has_wrong = any(w in text for w in wrong_signals)
    has_good  = any(g in text for g in good_signals)
    if has_wrong and not has_good:
        log.warning(f"[nav] wrong page detected (url={page.url}). Going back.")
        return True
    return False


async def _dismiss_cookies(page) -> None:
    _emit({"type": "step", "icon": "🍪", "text": "Buscando banner de cookies..."})
    await page.wait_for_timeout(1500)
    els = await _page_clickables(page)
    texts = [e["text"].lower() for e in els if e["text"].strip()]
    log.info(f"[dismiss_cookies] elementos visibles: {texts[:20]}")
    hit = await _smart_click(page,
        ["aceptar todas", "aceptar todo", "accept all", "aceptar todo",
         "allow all", "i accept", "acepto", "de acuerdo", "confirmar",
         "entendido", "aceptar y continuar", "allow cookies"],
        avoid=["rechazar", "reject", "solo esenciales", "gestionar", "preferencias", "personalizar"],
        timeout_ms=1000,
    )
    if not hit:
        # Last resort: click the first visible button in the cookie overlay
        for e in els:
            t = e["text"].lower()
            if any(w in t for w in ["aceptar", "accept", "allow", "confirmar", "agree"]):
                log.info(f"[dismiss_cookies] fallback click: '{e['text']}'")
                _emit({"type": "step", "icon": "🍪", "text": f"Cookies: clic en '{e['text']}'"})
                await page.mouse.click(e["x"], e["y"])
                await page.wait_for_timeout(800)
                break
    else:
        _emit({"type": "step", "icon": "✓", "text": f"Cookies aceptadas: '{hit}'"})
    if not hit:
        _emit({"type": "step", "icon": "−", "text": "No se encontró banner de cookies"})


async def _carrefour_login(page, email: str = None, password: str = None) -> None:
    session_file = SESSIONS_DIR / "carrefour.json"
    if session_file.exists():
        return
    email    = email    or os.getenv("CARREFOUR_EMAIL",    "").strip()
    password = password or os.getenv("CARREFOUR_PASSWORD", "").strip()
    if not email or not password:
        raise ValueError("Credenciales de Carrefour no disponibles")

    _emit({"type": "step", "icon": "🌐", "text": "Navegando a carrefour.es..."})
    await page.goto("https://www.carrefour.es", wait_until="domcontentloaded")
    await page.wait_for_timeout(3000)

    # Accept cookies first (DOM-based, fast)
    await _dismiss_cookies(page)
    await page.wait_for_timeout(1500)

    # Click the "Mi cuenta" div directly — it's a <div class="account-menu track-click">
    # _page_clickables never finds it because it's not a button/a/input
    _emit({"type": "step", "icon": "👤", "text": "Abriendo panel de login (Mi cuenta)..."})
    try:
        await page.click('.account-menu', timeout=6000)
        await page.wait_for_timeout(2500)
        _emit({"type": "step", "icon": "✓", "text": "Panel de login abierto"})
    except Exception as e:
        _emit({"type": "step", "icon": "⚠", "text": f"No se encontró .account-menu, dejando al LLM: {e}"})

    # Hand off to LLM vision navigator only for filling the form
    await _vision_navigate(
        page,
        goal=(
            f"Hay un panel/drawer de login abierto en la página de Carrefour. "
            f"Haz clic en el campo de email e introduce '{email}'. "
            f"Luego haz clic en el campo de contraseña e introduce '{password}'. "
            "Finalmente pulsa el botón 'Acceder' para iniciar sesión. "
            "NO pulses 'Empezar a comprar', 'Crear cuenta' ni 'Registrarse'. "
            "Cuando veas el nombre del usuario o 'Área privada' o 'Mis pedidos', responde con action=done."
        ),
        max_steps=8,
    )


async def _carrefour_add(page, query: str) -> CartItem | None:
    from urllib.parse import quote
    await page.goto(f"https://www.carrefour.es/supermercado/buscar?query={quote(query)}", wait_until="domcontentloaded")
    await page.wait_for_timeout(2000)
    try:
        await page.wait_for_selector('.product-card', timeout=8000)
        prod = page.locator('.product-card').first
        name = (await prod.locator('.product-card__title, .product-card__name').first.inner_text()).strip()
        price_raw = (await prod.locator('.product-card__price, .buy-box__price').first.inner_text()).strip()
        price = _parse_price(price_raw)
        await prod.locator('button[aria-label*="Añadir"], .add-to-cart-btn, button:has-text("Añadir")').first.click(timeout=4000)
        await page.wait_for_timeout(600)
        return CartItem(name=name, price=price)
    except Exception as e:
        log.warning(f"Carrefour add '{query}': {e}")
        return None


async def _carrefour_checkout(page) -> bool:
    try:
        await page.goto("https://www.carrefour.es/carrito", wait_until="networkidle")
        await page.locator('button:has-text("Tramitar"), button:has-text("Finalizar compra")').first.click(timeout=6000)
        await page.wait_for_load_state("networkidle")
        return True
    except Exception as e:
        log.warning(f"Carrefour checkout: {e}")
        return False


_PW_STORES: dict[str, tuple] = {
    "amazon":    (_amazon_login,    _amazon_add,    _amazon_checkout),
    "carrefour": (_carrefour_login, _carrefour_add, _carrefour_checkout),
    "alcampo":   (_alcampo_login,   _alcampo_add,   _alcampo_checkout),
}

STORE_NAMES = ["mercadona", "amazon", "carrefour", "alcampo"]


# ── Public API ─────────────────────────────────────────────────────────────────

async def build_cart(store: str, items: list[str]) -> dict:
    store = store.lower()
    if store not in STORE_NAMES:
        raise ValueError(f"Tienda desconocida: '{store}'. Opciones: {', '.join(STORE_NAMES)}")

    if store == "mercadona":
        cart = await _build_cart_mercadona(items)
    else:
        cart = await _pw_build_cart(store, items)

    return cart.to_dict()


async def confirm_checkout(store: str) -> bool:
    store = store.lower()
    if store == "mercadona":
        return await _confirm_mercadona()
    return await _pw_confirm(store)


def clear_session(store: str) -> None:
    for f in SESSIONS_DIR.glob(f"{store.lower()}*"):
        f.unlink(missing_ok=True)


_STORE_DOMAINS = {
    "carrefour": "carrefour.es",
    "alcampo":   "compraonline.alcampo.es",
    "amazon":    "amazon.es",
}


def import_chrome_session(store: str) -> int:
    """
    Read cookies for `store` from the local Chrome profile using browser_cookie3.
    Sets DBUS_SESSION_BUS_ADDRESS so the GNOME keyring is reachable from the service.
    Saves as Playwright storage state. Returns the number of cookies imported.
    """
    try:
        import browser_cookie3
    except ImportError:
        raise RuntimeError("browser_cookie3 no instalado: pip install browser-cookie3")

    store = store.lower()
    domain = _STORE_DOMAINS.get(store)
    if not domain:
        raise ValueError(f"import_chrome_session: tienda desconocida '{store}'")

    # Set D-Bus session bus so browser_cookie3 can reach the GNOME keyring
    uid = os.getuid()
    bus = f"unix:path=/run/user/{uid}/bus"
    old_bus = os.environ.get("DBUS_SESSION_BUS_ADDRESS")
    os.environ["DBUS_SESSION_BUS_ADDRESS"] = bus
    try:
        jar = browser_cookie3.chrome(domain_name=domain)
        cookies = list(jar)
    except Exception as e:
        raise RuntimeError(f"No se pudo leer el perfil de Chrome: {e}")
    finally:
        if old_bus is None:
            os.environ.pop("DBUS_SESSION_BUS_ADDRESS", None)
        else:
            os.environ["DBUS_SESSION_BUS_ADDRESS"] = old_bus

    if not cookies:
        raise RuntimeError(
            f"No hay cookies de {domain} en Chrome. "
            "Abre Chrome, inicia sesión en la tienda e inténtalo de nuevo."
        )

    pw_cookies = []
    for c in cookies:
        if not c.value:
            continue
        pw_cookies.append({
            "name":     c.name,
            "value":    c.value,
            "domain":   c.domain if c.domain.startswith(".") else "." + c.domain,
            "path":     c.path or "/",
            "expires":  int(c.expires) if c.expires else -1,
            "httpOnly": bool(getattr(c, "has_nonstandard_attr", lambda _: False)("HttpOnly")),
            "secure":   bool(c.secure),
            "sameSite": "Lax",
        })

    state = {"cookies": pw_cookies, "origins": []}
    session_file = SESSIONS_DIR / f"{store}.json"
    session_file.write_text(json.dumps(state))
    log.info(f"import_chrome_session: {len(pw_cookies)} cookies importadas para {store}")
    return len(pw_cookies)


_CDP_PORT = 9292  # Remote debugging port for the user-visible Chrome
_CDP_PROC: subprocess.Popen | None = None


async def connect_via_visible_chrome(store: str, email: str, password: str) -> None:
    """
    Launch Chrome on the user's real display (:0), navigate to the store's login page,
    fill credentials automatically, wait for the login-confirmed selector, then save
    the session. The user can watch everything happen on screen.
    """
    global _CDP_PROC
    store = store.lower()
    if store not in _STORE_LOGIN_URLS:
        raise ValueError(f"connect_via_visible_chrome: tienda desconocida '{store}'")

    chromium = _chromium_executable()
    if not chromium:
        raise RuntimeError("Google Chrome no encontrado")

    session_file = SESSIONS_DIR / f"{store}.json"
    logged_in_sel = _STORE_LOGGED_IN_SELECTOR[store]
    login_url = _STORE_LOGIN_URLS[store]

    # Kill any previous CDP Chrome
    if _CDP_PROC and _CDP_PROC.poll() is None:
        _CDP_PROC.terminate()
        await asyncio.sleep(0.5)

    # Resolve XAUTHORITY for the user's Wayland/X session
    import glob as _glob
    xauth_files = _glob.glob(f"/run/user/{os.getuid()}/.mutter-Xwaylandauth.*")
    xauth = xauth_files[0] if xauth_files else ""

    env = {**os.environ, "DISPLAY": ":0"}
    if xauth:
        env["XAUTHORITY"] = xauth

    _cdp_profile = SESSIONS_DIR.parent / ".chrome-cdp-profile"
    _CDP_PROC = subprocess.Popen(
        [
            chromium,
            f"--remote-debugging-port={_CDP_PORT}",
            f"--user-data-dir={_cdp_profile}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-infobars",
            "--disable-blink-features=AutomationControlled",
            "--exclude-switches=enable-automation",
            login_url,
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Wait for Chrome's CDP endpoint to become reachable (up to 20s)
    deadline = asyncio.get_event_loop().time() + 20
    while asyncio.get_event_loop().time() < deadline:
        if _CDP_PROC.poll() is not None:
            raise RuntimeError("Chrome terminó inesperadamente al arrancar")
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", _CDP_PORT), timeout=1
            )
            writer.close()
            await writer.wait_closed()
            break
        except Exception:
            pass
        await asyncio.sleep(0.5)
    else:
        raise RuntimeError(f"Chrome no abrió el puerto CDP {_CDP_PORT} en 20 segundos")
    await asyncio.sleep(0.5)  # small buffer after port opens

    from playwright.async_api import async_playwright

    async with async_playwright() as pw:
        browser = await pw.chromium.connect_over_cdp(
            f"http://localhost:{_CDP_PORT}", timeout=15000
        )
        ctx = browser.contexts[0] if browser.contexts else await browser.new_context()
        # Hide automation fingerprints from Cloudflare/anti-bot
        await ctx.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = { runtime: {} };
        """)
        pages = ctx.pages
        page = pages[0] if pages else await ctx.new_page()

        # Fill credentials (each login handler manages its own navigation and cookies)
        if store == "carrefour":
            await _carrefour_login(page, email=email, password=password)
        elif store == "alcampo":
            await _alcampo_login(page, email=email, password=password)
        elif store == "amazon":
            await _amazon_login(page, email=email, password=password)

        # Wait for confirmed login
        try:
            await page.wait_for_selector(logged_in_sel, timeout=60_000)
        except Exception:
            raise RuntimeError(
                f"Login fallido para {store}. Verifica email y contraseña."
            )

        await ctx.storage_state(path=str(session_file))
        log.info(f"connect_via_visible_chrome: sesión {store} guardada")


def stop_visible_chrome() -> None:
    global _CDP_PROC
    if _CDP_PROC and _CDP_PROC.poll() is None:
        _CDP_PROC.terminate()
    _CDP_PROC = None


# ── Claude-controlled browser session ─────────────────────────────────────────
# Keeps the CDP-connected browser alive so Claude can interact across turns.

_ctrl_pw      = None   # Playwright instance (started, not context-managed)
_ctrl_browser = None   # CDPSession browser
_ctrl_page    = None   # Active page
_ctrl_store: str | None = None
_ctrl_display: str = "screen"

# ── Output display for the Claude-driven browser (noVNC option) ────────────────
_VIEW_DISPLAY_NUM = 97
VIEW_VNC_PORT = 5997
_view_session: dict | None = None


async def _start_view_session() -> str:
    """Start Xvfb + x11vnc so the control browser is watchable via noVNC."""
    global _view_session
    for tool in ("Xvfb", "x11vnc"):
        if not shutil.which(tool):
            raise RuntimeError(f"'{tool}' no instalado (sudo apt install xvfb x11vnc novnc)")
    await _stop_view_session()
    token = secrets.token_urlsafe(16)
    xvfb = subprocess.Popen(
        ["Xvfb", f":{_VIEW_DISPLAY_NUM}", "-screen", "0", "1366x768x24", "-ac"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    await asyncio.sleep(0.8)
    if xvfb.poll() is not None:
        raise RuntimeError("Xvfb no arrancó para el visor")
    vnc = subprocess.Popen(
        ["x11vnc", "-display", f":{_VIEW_DISPLAY_NUM}", "-nopw", "-listen", "127.0.0.1",
         "-rfbport", str(VIEW_VNC_PORT), "-forever", "-shared", "-quiet",
         "-noxdamage", "-bg", "-nopw"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    # Wait until x11vnc is actually accepting connections on the VNC port
    deadline = asyncio.get_event_loop().time() + 10
    ready = False
    while asyncio.get_event_loop().time() < deadline:
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", VIEW_VNC_PORT), timeout=1
            )
            w.close()
            await w.wait_closed()
            ready = True
            break
        except Exception:
            await asyncio.sleep(0.4)
    if not ready:
        _kill_session_procs(vnc, xvfb)
        raise RuntimeError(f"x11vnc no abrió el puerto {VIEW_VNC_PORT} para el visor")
    _view_session = {"xvfb": xvfb, "vnc": vnc, "token": token}
    log.info(f"[view] visor noVNC listo en :{_VIEW_DISPLAY_NUM} (VNC {VIEW_VNC_PORT})")
    return token


async def _stop_view_session() -> None:
    global _view_session
    if not _view_session:
        return
    s = _view_session
    _view_session = None
    _kill_session_procs(s.get("vnc"), s.get("xvfb"))


def view_session_token() -> str | None:
    return _view_session["token"] if _view_session else None


async def start_control_browser(store: str, display: str = "screen") -> dict:
    """Launch CDP Chrome for Claude to drive.

    display="screen": physical monitor (:0).  display="novnc": virtual Xvfb + x11vnc.
    Returns {"url", "display", "token"}.
    """
    global _CDP_PROC, _ctrl_pw, _ctrl_browser, _ctrl_page, _ctrl_store, _ctrl_display

    await close_control_browser()

    chromium = _chromium_executable()
    if not chromium:
        raise RuntimeError("Google Chrome no encontrado")

    login_url = _STORE_LOGIN_URLS.get(store, f"https://www.{store}.es")

    # Kill any previous CDP Chrome
    if _CDP_PROC and _CDP_PROC.poll() is None:
        _CDP_PROC.terminate()
        await asyncio.sleep(0.5)

    token = None
    extra_args = []
    if display == "novnc":
        token = await _start_view_session()
        env = {**os.environ, "DISPLAY": f":{_VIEW_DISPLAY_NUM}"}
        env.pop("XAUTHORITY", None)
        # No --no-sandbox: Chrome's sandbox works here (AppArmor 'chrome' profile),
        # and the flag only triggered the "unsupported command-line flag" warning bar.
        extra_args = ["--window-position=0,0",
                      "--window-size=1366,768", "--start-maximized"]
    else:
        display = "screen"
        import glob as _glob
        xauth_files = _glob.glob(f"/run/user/{os.getuid()}/.mutter-Xwaylandauth.*")
        env = {**os.environ, "DISPLAY": ":0"}
        if xauth_files:
            env["XAUTHORITY"] = xauth_files[0]

    _cdp_profile = SESSIONS_DIR.parent / ".chrome-cdp-profile"
    _CDP_PROC = subprocess.Popen(
        [chromium, f"--remote-debugging-port={_CDP_PORT}",
         f"--user-data-dir={_cdp_profile}",
         "--no-first-run", "--no-default-browser-check",
         "--disable-infobars", "--disable-blink-features=AutomationControlled",
         "--exclude-switches=enable-automation", *extra_args, login_url],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    # Wait for CDP port
    deadline = asyncio.get_event_loop().time() + 20
    while asyncio.get_event_loop().time() < deadline:
        if _CDP_PROC.poll() is not None:
            await _stop_view_session()
            raise RuntimeError("Chrome terminó inesperadamente")
        try:
            r, w = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", _CDP_PORT), timeout=1)
            w.close(); await w.wait_closed(); break
        except Exception:
            pass
        await asyncio.sleep(0.5)
    else:
        await _stop_view_session()
        raise RuntimeError(f"Chrome no abrió CDP en {_CDP_PORT}")
    await asyncio.sleep(0.8)

    from playwright.async_api import async_playwright as _apf
    _ctrl_pw = await _apf().start()
    _ctrl_browser = await _ctrl_pw.chromium.connect_over_cdp(f"http://localhost:{_CDP_PORT}", timeout=15000)
    ctx = _ctrl_browser.contexts[0] if _ctrl_browser.contexts else await _ctrl_browser.new_context()
    await ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});window.chrome={runtime:{}};")
    pages = ctx.pages
    _ctrl_page = pages[0] if pages else await ctx.new_page()
    _ctrl_store = store
    _ctrl_display = display

    await _ctrl_page.wait_for_timeout(2000)
    return {"url": _ctrl_page.url, "display": display, "token": token}


async def ctrl_screenshot(path: str = "/tmp/zeus_browser.png") -> str:
    if not _ctrl_page:
        raise RuntimeError("No hay navegador activo")
    await _ctrl_page.screenshot(path=path, full_page=False)
    return path


async def ctrl_elements() -> list:
    if not _ctrl_page:
        raise RuntimeError("No hay navegador activo")
    return await _page_clickables(_ctrl_page)


async def ctrl_action(action: dict) -> str:
    if not _ctrl_page:
        raise RuntimeError("No hay navegador activo")
    t = action.get("type", "")
    if t == "click":
        await _ctrl_page.mouse.click(action["x"], action["y"])
        await _ctrl_page.wait_for_timeout(1000)
        return f"click en ({action['x']},{action['y']}) — URL: {_ctrl_page.url}"
    elif t == "type":
        await _ctrl_page.keyboard.type(str(action["text"]), delay=60)
        return f"escrito: {str(action['text'])[:40]}"
    elif t == "key":
        await _ctrl_page.keyboard.press(action["key"])
        await _ctrl_page.wait_for_timeout(500)
        return f"tecla: {action['key']}"
    elif t == "navigate":
        await _ctrl_page.goto(action["url"], wait_until="domcontentloaded")
        await _ctrl_page.wait_for_timeout(1500)
        return f"navegado a {_ctrl_page.url}"
    elif t == "fill":
        selector = action.get("selector", "")
        value = str(action.get("value", ""))
        await _ctrl_page.locator(selector).fill(value)
        await _ctrl_page.wait_for_timeout(500)
        return f"fill '{selector}' = '{value[:40]}'"
    elif t == "wait":
        await _ctrl_page.wait_for_timeout(int(action.get("ms", 1000)))
        return "esperado"
    else:
        return f"acción desconocida: {t}"


async def ctrl_save_session() -> None:
    if not _ctrl_browser or not _ctrl_store:
        raise RuntimeError("No hay sesión activa que guardar")
    ctx = _ctrl_browser.contexts[0] if _ctrl_browser.contexts else None
    if not ctx:
        raise RuntimeError("No hay contexto de navegador")
    session_file = SESSIONS_DIR / f"{_ctrl_store}.json"
    await ctx.storage_state(path=str(session_file))
    log.info(f"[ctrl_browser] sesión {_ctrl_store} guardada en {session_file}")


async def close_control_browser() -> None:
    global _ctrl_pw, _ctrl_browser, _ctrl_page, _ctrl_store, _CDP_PROC
    try:
        if _ctrl_browser:
            await _ctrl_browser.close()
        if _ctrl_pw:
            await _ctrl_pw.stop()
    except Exception:
        pass
    if _CDP_PROC and _CDP_PROC.poll() is None:
        try:
            _CDP_PROC.terminate()
        except Exception:
            pass
    await _stop_view_session()
    _ctrl_pw = _ctrl_browser = _ctrl_page = _ctrl_store = None


def auth_status() -> dict[str, bool]:
    """Return which stores have a saved session."""
    return {
        "mercadona": _MERC_SESSION.exists(),
        "amazon":    (SESSIONS_DIR / "amazon.json").exists(),
        "carrefour": (SESSIONS_DIR / "carrefour.json").exists(),
        "alcampo":   (SESSIONS_DIR / "alcampo.json").exists(),
    }


# Login URLs shown to user in the visible browser window
_STORE_LOGIN_URLS = {
    "amazon": (
        "https://www.amazon.es/ap/signin"
        "?openid.return_to=https://www.amazon.es"
        "&openid.mode=checkid_setup"
        "&openid.ns=http://specs.openid.net/auth/2.0"
    ),
    "carrefour": "https://www.carrefour.es",
    "alcampo":   "https://www.compraonline.alcampo.es/login",
}

# Selectors that confirm a successful login per store
_STORE_LOGGED_IN_SELECTOR = {
    "amazon":    "#nav-link-accountList-nav-line-1",
    "carrefour": '[class*="header__user"], [href*="mi-cuenta"], [href*="cuenta"]',
    "alcampo":   '[href*="/account"], [class*="userAvatar"], [class*="headerUser"], [aria-label*="cuenta"]',
}


# Stores that require a headed browser to pass Cloudflare bot protection
_HEADED_STORES = {"amazon", "carrefour", "alcampo"}


async def connect_store_browser(store: str, email: str = None, password: str = None) -> None:
    """
    Auto-login using provided credentials or .env fallback. Saves session cookies.
    Uses headed Chrome + Xvfb for stores protected by Cloudflare bot detection.
    Raises RuntimeError if Chromium missing, credentials absent, or login fails.
    """
    store = store.lower()
    if store not in _STORE_LOGIN_URLS:
        raise ValueError(f"connect_store_browser: tienda desconocida '{store}'")

    chromium = _chromium_executable()
    if not chromium:
        raise RuntimeError(
            "Google Chrome no disponible. Instalar desde https://www.google.com/chrome/"
        )

    from playwright.async_api import async_playwright

    session_file = SESSIONS_DIR / f"{store}.json"
    logged_in_sel = _STORE_LOGGED_IN_SELECTOR[store]
    headed = store in _HEADED_STORES

    xvfb_proc = None
    old_display = os.environ.get("DISPLAY")
    if headed:
        if not shutil.which("Xvfb"):
            raise RuntimeError("Xvfb no instalado. Ejecutar: sudo apt install xvfb")
        xvfb_proc = subprocess.Popen(
            ["Xvfb", ":98", "-screen", "0", "1280x800x24", "-ac"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        await asyncio.sleep(1.0)
        os.environ["DISPLAY"] = ":98"

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=not headed,
                executable_path=chromium,
                args=_BROWSER_ARGS,
            )
            ctx = await browser.new_context(
                user_agent=_UA,
                viewport={"width": 1280, "height": 800},
            )
            await ctx.add_init_script(_STEALTH_SCRIPT)
            page = await ctx.new_page()

            if store == "amazon":
                await _amazon_login(page, email=email, password=password)
            elif store == "carrefour":
                await _carrefour_login(page, email=email, password=password)
            elif store == "alcampo":
                await _alcampo_login(page, email=email, password=password)

            try:
                await page.wait_for_selector(logged_in_sel, timeout=30_000)
            except Exception:
                await browser.close()
                raise RuntimeError(
                    f"Login fallido para {store}. Verifica que el email y la contraseña sean correctos."
                )

            await ctx.storage_state(path=str(session_file))
            log.info(f"connect_store_browser: sesión {store} guardada en {session_file}")
            await browser.close()
    finally:
        if xvfb_proc:
            xvfb_proc.terminate()
        if old_display is None:
            os.environ.pop("DISPLAY", None)
        else:
            os.environ["DISPLAY"] = old_display


async def connect_mercadona(email: str = None, password: str = None) -> None:
    """
    Mercadona uses REST API — login with credentials (UI or .env) and cache JWT.
    Raises ValueError/RuntimeError if credentials missing or login fails.
    """
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        _MERC_SESSION.unlink(missing_ok=True)
        await _merc_login(client, email=email, password=password)


# ── Interactive browser login session (Xvfb + x11vnc + Playwright headed) ──────

_DISPLAY_NUM = 99
VNC_PORT = 5999  # x11vnc listens here (localhost only)

_browser_session: dict | None = None


async def start_browser_session(store: str) -> str:
    """
    Start Xvfb + x11vnc + headed Playwright for interactive login.
    Returns session token used to authenticate the WebSocket proxy.
    Raises RuntimeError if required tools are missing.
    """
    global _browser_session

    for tool in ("Xvfb", "x11vnc"):
        if not shutil.which(tool):
            raise RuntimeError(
                f"'{tool}' no instalado. Ejecutar: sudo apt install xvfb x11vnc novnc"
            )

    chromium = _chromium_executable()
    if not chromium:
        raise RuntimeError("Chromium no disponible. Instalar: sudo apt install chromium-browser")

    store = store.lower()
    if store not in _STORE_LOGIN_URLS:
        raise ValueError(f"Tienda desconocida: {store}")

    await stop_browser_session()

    token = secrets.token_urlsafe(16)
    login_url = _STORE_LOGIN_URLS[store]
    logged_in_sel = _STORE_LOGGED_IN_SELECTOR[store]
    session_file = SESSIONS_DIR / f"{store}.json"

    xvfb_proc = subprocess.Popen(
        ["Xvfb", f":{_DISPLAY_NUM}", "-screen", "0", "1280x800x24", "-ac"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    await asyncio.sleep(0.8)

    vnc_proc = subprocess.Popen(
        [
            "x11vnc",
            "-display", f":{_DISPLAY_NUM}",
            "-nopw", "-listen", "127.0.0.1",
            "-rfbport", str(VNC_PORT),
            "-forever", "-shared", "-quiet",
        ],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    await asyncio.sleep(0.5)

    _browser_session = {
        "store": store,
        "token": token,
        "xvfb": xvfb_proc,
        "vnc": vnc_proc,
        "task": None,
    }

    async def _run():
        old_display = os.environ.get("DISPLAY")
        os.environ["DISPLAY"] = f":{_DISPLAY_NUM}"
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(
                    headless=False,
                    executable_path=chromium,
                    args=["--no-sandbox", "--disable-dev-shm-usage",
                          "--disable-blink-features=AutomationControlled"],
                )
                ctx = await browser.new_context(
                    user_agent=_UA,
                    viewport={"width": 1280, "height": 800},
                )
                page = await ctx.new_page()
                await page.goto(login_url, wait_until="domcontentloaded")

                deadline = asyncio.get_event_loop().time() + 300
                while asyncio.get_event_loop().time() < deadline:
                    try:
                        if await page.locator(logged_in_sel).count() > 0:
                            await ctx.storage_state(path=str(session_file))
                            log.info(f"browser_session: {store} sesión guardada")
                            break
                    except Exception:
                        pass
                    await asyncio.sleep(1.5)

                await browser.close()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error(f"browser_session error: {e}")
        finally:
            if old_display is None:
                os.environ.pop("DISPLAY", None)
            else:
                os.environ["DISPLAY"] = old_display
            global _browser_session
            current_token = (_browser_session or {}).get("token")
            if current_token == token:
                _kill_session_procs(xvfb_proc, vnc_proc)
                _browser_session = None

    task = asyncio.create_task(_run())
    _browser_session["task"] = task
    return token


def _kill_session_procs(*procs) -> None:
    for proc in procs:
        if proc and proc.poll() is None:
            try:
                proc.terminate()
            except Exception:
                pass


async def stop_browser_session() -> None:
    global _browser_session
    if _browser_session is None:
        return
    sess = _browser_session
    _browser_session = None

    task = sess.get("task")
    if task and not task.done():
        task.cancel()

    _kill_session_procs(sess.get("vnc"), sess.get("xvfb"))


def browser_session_token() -> str | None:
    return _browser_session["token"] if _browser_session else None
