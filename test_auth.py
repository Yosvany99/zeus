#!/usr/bin/env python3
"""
Test auth for Mercadona (REST) and optionally Amazon/Carrefour (Playwright).
Reads .env from project root. Never adds to cart or checks out.
"""
import asyncio
import os
import sys
from pathlib import Path

# Load .env
env_file = Path(__file__).parent / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

import httpx

_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_MERC_BASE = "https://tienda.mercadona.es/api"


def _env(key: str) -> str | None:
    return os.getenv(key, "").strip() or None


def ok(msg: str):
    print(f"  [OK]  {msg}")


def fail(msg: str):
    print(f"  [FAIL] {msg}")


def skip(msg: str):
    print(f"  [SKIP] {msg}")


# ── Mercadona ──────────────────────────────────────────────────────────────────

async def test_mercadona():
    print("\n=== Mercadona (REST API) ===")
    email = _env("MERCADONA_EMAIL")
    password = _env("MERCADONA_PASSWORD")
    postal = os.getenv("MERCADONA_POSTAL", "28001")

    if not email or not password:
        skip("MERCADONA_EMAIL / MERCADONA_PASSWORD no configurados en .env")
        return

    headers = {
        "User-Agent": _UA,
        "Accept": "application/json",
        "Referer": "https://tienda.mercadona.es/",
        "Origin": "https://tienda.mercadona.es",
        "Accept-Language": "es-ES,es;q=0.9",
    }

    async with httpx.AsyncClient(timeout=15) as client:
        # 1. Postal code
        try:
            r = await client.get(f"{_MERC_BASE}/postal-codes/{postal}/", headers=headers)
            if r.status_code == 200:
                ok(f"Código postal {postal} válido")
                wh_id = r.json().get("id") or r.json().get("warehouse_id")
            else:
                fail(f"Postal code lookup: HTTP {r.status_code}")
                wh_id = None
        except Exception as e:
            fail(f"Postal code lookup error: {e}")
            wh_id = None

        # 2. Login
        payload = {"username": email, "password": password}
        if wh_id:
            payload["warehouse_id"] = wh_id
        try:
            r = await client.post(f"{_MERC_BASE}/auth/", json=payload, headers=headers)
            if r.status_code in (200, 201):
                data = r.json()
                token = data.get("token") or data.get("access") or data.get("access_token")
                if token:
                    ok(f"Login OK — token obtenido ({token[:20]}...)")
                else:
                    fail(f"Login HTTP {r.status_code} pero sin token. Keys: {list(data.keys())}")
            else:
                fail(f"Login HTTP {r.status_code}: {r.text[:300]}")
        except Exception as e:
            fail(f"Login error: {e}")


# ── Amazon & Carrefour (Playwright) ───────────────────────────────────────────

async def test_playwright_store(name: str, url: str, email_key: str, pass_key: str,
                                fill_login, check_logged_in):
    print(f"\n=== {name} (Playwright) ===")

    email = _env(email_key)
    password = _env(pass_key)
    if not email or not password:
        skip(f"{email_key} / {pass_key} no configurados en .env")
        return

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        skip("playwright no instalado — `pip install playwright && playwright install chromium`")
        return

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(user_agent=_UA)
        page = await ctx.new_page()
        try:
            await fill_login(page, email, password)
            logged_in = await check_logged_in(page)
            if logged_in:
                ok("Login OK — sesión autenticada")
            else:
                fail("Login completado pero no se detectó sesión autenticada")
        except Exception as e:
            fail(f"Error durante login: {e}")
        finally:
            await browser.close()


async def amazon_fill(page, email: str, password: str):
    await page.goto(
        "https://www.amazon.es/ap/signin"
        "?openid.return_to=https://www.amazon.es"
        "&openid.mode=checkid_setup"
        "&openid.ns=http://specs.openid.net/auth/2.0",
        wait_until="domcontentloaded",
    )
    await page.locator("#ap_email").fill(email)
    await page.locator("#continue").click()
    await page.wait_for_selector("#ap_password", timeout=8000)
    await page.locator("#ap_password").fill(password)
    await page.locator("#signInSubmit").click()
    await page.wait_for_load_state("networkidle")


async def amazon_check(page) -> bool:
    # Logged-in Amazon shows nav-link-accountList with the user's name
    try:
        text = await page.locator("#nav-link-accountList").inner_text(timeout=5000)
        return "Hola" in text or "cuenta" in text.lower()
    except Exception:
        return False


async def carrefour_fill(page, email: str, password: str):
    await page.goto("https://www.carrefour.es/login", wait_until="domcontentloaded")
    await page.wait_for_timeout(1500)
    await page.locator('input[type="email"], input[name="email"]').first.fill(email)
    await page.locator('input[type="password"], input[name="password"]').first.fill(password)
    await page.locator('button[type="submit"]').first.click()
    await page.wait_for_load_state("networkidle")


async def carrefour_check(page) -> bool:
    # After login, URL leaves /login and page shows account indicators
    url = page.url
    if "/login" in url:
        return False
    try:
        # Look for account/profile link
        await page.wait_for_selector(
            '[class*="account"], [href*="cuenta"], [href*="mi-cuenta"]',
            timeout=5000,
        )
        return True
    except Exception:
        return "/login" not in page.url


# ── Main ───────────────────────────────────────────────────────────────────────

async def main():
    await test_mercadona()

    await test_playwright_store(
        name="Amazon",
        url="https://www.amazon.es",
        email_key="AMAZON_EMAIL",
        pass_key="AMAZON_PASSWORD",
        fill_login=amazon_fill,
        check_logged_in=amazon_check,
    )

    await test_playwright_store(
        name="Carrefour",
        url="https://www.carrefour.es",
        email_key="CARREFOUR_EMAIL",
        pass_key="CARREFOUR_PASSWORD",
        fill_login=carrefour_fill,
        check_logged_in=carrefour_check,
    )

    print()


if __name__ == "__main__":
    asyncio.run(main())
