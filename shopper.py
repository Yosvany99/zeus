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

import httpx

log = logging.getLogger("zeus.shopper")

SESSIONS_DIR = Path("/home/axel/.local/share/zeus/sessions")
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

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
# Mercadona exposes an undocumented but stable JSON API used by their own SPA.

_MERC_BASE = "https://tienda.mercadona.es/api"
_MERC_SESSION = SESSIONS_DIR / "mercadona_token.json"


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


async def _merc_login(client: httpx.AsyncClient) -> str:
    """Log in and return JWT token. Saves token to disk."""
    email = _env("MERCADONA_EMAIL", "Mercadona")
    password = _env("MERCADONA_PASSWORD", "Mercadona")
    postal = os.getenv("MERCADONA_POSTAL", "28001")

    # First, set the warehouse (postal code)
    r = await client.get(f"{_MERC_BASE}/postal-codes/{postal}/", headers=_merc_headers())
    wh_id = None
    if r.status_code == 200:
        data = r.json()
        wh_id = data.get("id") or data.get("warehouse_id")

    # Login
    payload = {"username": email, "password": password}
    if wh_id:
        payload["warehouse_id"] = wh_id

    r = await client.post(
        f"{_MERC_BASE}/auth/",
        json=payload,
        headers=_merc_headers(),
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Mercadona login falló ({r.status_code}): {r.text[:200]}")

    data = r.json()
    token = data.get("token") or data.get("access") or data.get("access_token")
    if not token:
        raise RuntimeError(f"Mercadona: token no encontrado en respuesta: {list(data.keys())}")

    _MERC_SESSION.write_text(json.dumps({"token": token}))
    return token


async def _merc_get_token(client: httpx.AsyncClient) -> str:
    """Return cached token or re-login."""
    if _MERC_SESSION.exists():
        try:
            return json.loads(_MERC_SESSION.read_text())["token"]
        except Exception:
            pass
    return await _merc_login(client)


async def _merc_search(client: httpx.AsyncClient, token: str, query: str) -> dict | None:
    """Search Mercadona API and return first product."""
    from urllib.parse import quote
    r = await client.get(
        f"{_MERC_BASE}/search/",
        params={"query": query, "lang": "es"},
        headers=_merc_headers(token),
    )
    if r.status_code == 401:
        return None  # token expired
    if r.status_code != 200:
        log.warning(f"Mercadona search '{query}' status {r.status_code}")
        return None

    results = r.json()
    # Response can be {"results": [...]} or a list directly
    if isinstance(results, dict):
        products = results.get("results", [])
    else:
        products = results

    if not products:
        return None

    # Flatten nested structure: products can have sub-lists
    flat = []
    for item in products:
        if isinstance(item, dict):
            if "products" in item:
                flat.extend(item["products"])
            else:
                flat.append(item)

    return flat[0] if flat else None


async def _merc_add_to_cart(client: httpx.AsyncClient, token: str, product: dict) -> bool:
    """Add product to Mercadona cart via API."""
    pid = product.get("id")
    if not pid:
        return False
    r = await client.post(
        f"{_MERC_BASE}/cart/",
        json={"id": pid, "amount": 1},
        headers=_merc_headers(token),
    )
    if r.status_code == 401:
        return False
    return r.status_code in (200, 201, 204)


def _merc_extract_price(product: dict) -> float:
    """Extract price from Mercadona product dict."""
    price_data = product.get("price_instructions") or product.get("retail_price") or {}
    if isinstance(price_data, dict):
        unit = price_data.get("unit_price") or price_data.get("retail_price")
        if unit:
            return float(unit)
    raw = product.get("price") or product.get("retail_price") or "0"
    return _parse_price(str(raw))


async def _build_cart_mercadona(items: list[str]) -> Cart:
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        token = await _merc_get_token(client)

        found: list[CartItem] = []
        not_found: list[str] = []

        for query in items:
            product = await _merc_search(client, token, query)
            if product is None:
                # Try re-login once
                try:
                    _MERC_SESSION.unlink(missing_ok=True)
                    token = await _merc_login(client)
                    product = await _merc_search(client, token, query)
                except Exception:
                    pass

            if product:
                name = product.get("display_name") or product.get("name") or query
                price = _merc_extract_price(product)
                added = await _merc_add_to_cart(client, token, product)
                if added:
                    found.append(CartItem(name=name, price=price))
                    log.info(f"Mercadona: añadido '{name}' @ {price}€")
                else:
                    not_found.append(query)
                    log.warning(f"Mercadona: no se pudo añadir '{name}' al carrito")
            else:
                not_found.append(query)
                log.warning(f"Mercadona: '{query}' no encontrado")

        total = round(sum(i.price * i.qty for i in found), 2)
        return Cart(store="mercadona", items=found, total=total, not_found=not_found)


async def _confirm_mercadona() -> bool:
    """Place the order via Mercadona API."""
    if not _MERC_SESSION.exists():
        raise RuntimeError("Sin sesión de Mercadona. Construye el carrito primero.")

    token = json.loads(_MERC_SESSION.read_text())["token"]
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        # Get cart to confirm it has items
        r = await client.get(f"{_MERC_BASE}/cart/", headers=_merc_headers(token))
        if r.status_code != 200:
            return False

        # Confirm order (checkout endpoint)
        r2 = await client.post(
            f"{_MERC_BASE}/orders/",
            json={},
            headers=_merc_headers(token),
        )
        return r2.status_code in (200, 201)


# ── Playwright-based stores (Amazon, Carrefour) ────────────────────────────────

def _chromium_executable() -> str | None:
    """Find system Chromium. Returns None if unavailable."""
    candidates = [
        "/usr/bin/chromium-browser",
        "/usr/bin/chromium",
        "/snap/bin/chromium",
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
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx = await browser.new_context(
            storage_state=str(session_file) if session_file.exists() else None,
            user_agent=_UA,
            viewport={"width": 1280, "height": 720},
        )
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
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx = await browser.new_context(
            storage_state=str(session_file) if session_file.exists() else None,
            user_agent=_UA,
            viewport={"width": 1280, "height": 720},
        )
        page = await ctx.new_page()
        try:
            ok = await checkout_fn(page)
            await ctx.storage_state(path=str(session_file))
            return ok
        finally:
            await browser.close()


# ── Amazon ─────────────────────────────────────────────────────────────────────

async def _amazon_login(page) -> None:
    session_file = SESSIONS_DIR / "amazon.json"
    if session_file.exists():
        return  # session loaded via storage_state
    email = _env("AMAZON_EMAIL", "Amazon")
    password = _env("AMAZON_PASSWORD", "Amazon")
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


# ── Carrefour ──────────────────────────────────────────────────────────────────

async def _carrefour_login(page) -> None:
    session_file = SESSIONS_DIR / "carrefour.json"
    if session_file.exists():
        return  # session loaded via storage_state
    email = _env("CARREFOUR_EMAIL", "Carrefour")
    password = _env("CARREFOUR_PASSWORD", "Carrefour")
    await page.goto("https://www.carrefour.es/login", wait_until="domcontentloaded")
    await page.wait_for_timeout(1500)
    await page.locator('input[type="email"], input[name="email"]').first.fill(email)
    await page.locator('input[type="password"], input[name="password"]').first.fill(password)
    await page.locator('button[type="submit"]').first.click()
    await page.wait_for_load_state("networkidle")


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
}

STORE_NAMES = ["mercadona", "amazon", "carrefour"]


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


def auth_status() -> dict[str, bool]:
    """Return which stores have a saved session."""
    return {
        "mercadona": _MERC_SESSION.exists(),
        "amazon":    (SESSIONS_DIR / "amazon.json").exists(),
        "carrefour": (SESSIONS_DIR / "carrefour.json").exists(),
    }


# Login URLs shown to user in the visible browser window
_STORE_LOGIN_URLS = {
    "amazon": (
        "https://www.amazon.es/ap/signin"
        "?openid.return_to=https://www.amazon.es"
        "&openid.mode=checkid_setup"
        "&openid.ns=http://specs.openid.net/auth/2.0"
    ),
    "carrefour": "https://www.carrefour.es/login",
}

# Selectors that confirm a successful login per store
_STORE_LOGGED_IN_SELECTOR = {
    "amazon":    "#nav-link-accountList-nav-line-1",
    "carrefour": '[class*="header__user"], [href*="mi-cuenta"], [href*="cuenta"]',
}


async def connect_store_browser(store: str) -> None:
    """
    Headless auto-login using .env credentials. Saves session cookies.
    Raises RuntimeError if Chromium missing, credentials absent, or login fails.
    """
    store = store.lower()
    if store not in _STORE_LOGIN_URLS:
        raise ValueError(f"connect_store_browser: tienda desconocida '{store}'")

    chromium = _chromium_executable()
    if not chromium:
        raise RuntimeError(
            "Chromium no disponible. Instalar con: sudo apt install chromium-browser"
        )

    from playwright.async_api import async_playwright

    session_file = SESSIONS_DIR / f"{store}.json"
    logged_in_sel = _STORE_LOGGED_IN_SELECTOR[store]

    _login_fns = {
        "amazon":    _amazon_login,
        "carrefour": _carrefour_login,
    }

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            executable_path=chromium,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx = await browser.new_context(
            user_agent=_UA,
            viewport={"width": 1280, "height": 800},
        )
        page = await ctx.new_page()

        login_fn = _login_fns.get(store)
        if login_fn:
            await login_fn(page)

        try:
            await page.wait_for_selector(logged_in_sel, timeout=30_000)
        except Exception:
            await browser.close()
            raise RuntimeError(
                f"Login fallido para {store}. Verifica credenciales en .env "
                f"({store.upper()}_EMAIL / {store.upper()}_PASSWORD)."
            )

        await ctx.storage_state(path=str(session_file))
        log.info(f"connect_store_browser: sesión {store} guardada en {session_file}")
        await browser.close()


async def connect_mercadona() -> None:
    """
    Mercadona uses REST API — login with credentials from .env and cache JWT.
    Raises RuntimeError if credentials missing or login fails.
    """
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        _MERC_SESSION.unlink(missing_ok=True)
        await _merc_login(client)


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
