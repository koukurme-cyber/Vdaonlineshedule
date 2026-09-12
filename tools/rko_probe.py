#!/usr/bin/env python3
import asyncio
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

PAGES = {
    "online": "https://adultchildren.ru/groups/online_list/",
    "offline": "https://adultchildren.ru/groups/offline_list/",
}
OUT = Path("rko_probe_output.json")


def html_cell(value, base_url):
    if value is None:
        value = ""
    if not isinstance(value, str):
        value = json.dumps(value, ensure_ascii=False, sort_keys=True)
    soup = BeautifulSoup(value, "lxml")
    text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True)).strip()
    links = []
    for a in soup.find_all("a", href=True):
        href = urljoin(base_url, a["href"].strip())
        if href not in links:
            links.append(href)
    return {"text": text, "links": links}


def normalize_rows(raw_rows, headers, base_url):
    result = []
    for idx, row in enumerate(raw_rows, 1):
        if isinstance(row, dict):
            cells = {str(k): html_cell(v, base_url) for k, v in row.items()}
        elif isinstance(row, list):
            cells = {}
            for i, value in enumerate(row):
                key = headers[i] if i < len(headers) and headers[i] else f"col_{i+1}"
                if key in cells:
                    key = f"{key}_{i+1}"
                cells[key] = html_cell(value, base_url)
        else:
            cells = {"value": html_cell(row, base_url)}
        result.append({"row_number": idx, "cells": cells})
    return result


def shape(value, depth=0):
    """Return structure only, never cell values."""
    if depth > 4:
        return type(value).__name__
    if isinstance(value, dict):
        return {str(k): shape(v, depth + 1) for k, v in list(value.items())[:30]}
    if isinstance(value, list):
        return {
            "type": "list",
            "length": len(value),
            "first": shape(value[0], depth + 1) if value else None,
        }
    return type(value).__name__


def largest_row_list(value):
    best = None
    if isinstance(value, list):
        if value and all(isinstance(x, (dict, list)) for x in value[: min(10, len(value))]):
            best = value
        for item in value[:5]:
            candidate = largest_row_list(item)
            if candidate is not None and (best is None or len(candidate) > len(best)):
                best = candidate
    elif isinstance(value, dict):
        for item in value.values():
            candidate = largest_row_list(item)
            if candidate is not None and (best is None or len(candidate) > len(best)):
                best = candidate
    return best


async def scrape_page(page, kind, url):
    xhr_urls = []

    def on_response(response):
        try:
            if response.request.resource_type in {"xhr", "fetch"} and response.url not in xhr_urls:
                xhr_urls.append(response.url)
        except Exception:
            pass

    page.on("response", on_response)
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    try:
        await page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    await page.wait_for_timeout(4000)

    title = await page.title()
    table_count = await page.locator("table").count()

    ninja_url = next((u for u in xhr_urls if "ninja_tables_public_action" in u and "get-all-data" in u), None)
    ajax_info = None
    ajax_row_count = None
    if ninja_url:
        resp = await page.request.get(ninja_url, timeout=60000)
        payload = await resp.json()
        rows_candidate = largest_row_list(payload)
        ajax_row_count = len(rows_candidate) if rows_candidate is not None else None
        ajax_info = {
            "status": resp.status,
            "payload_shape": shape(payload),
            "largest_row_list_count": ajax_row_count,
        }

    # DOM extraction is deliberately limited to what the public page renders.
    # We do not persist hidden raw AJAX values here because Ninja Tables may
    # include administrative columns that are not intended for our bot.
    dom = await page.evaluate("""
    () => {
      const tables = Array.from(document.querySelectorAll('table'));
      let best = null;
      for (const table of tables) {
        const rows = Array.from(table.querySelectorAll('tbody tr'))
          .filter(r => !r.classList.contains('child'));
        if (!best || rows.length > best.count) {
          best = {
            count: rows.length,
            id: table.id || null,
            headers: Array.from(table.querySelectorAll('thead th')).map(x => (x.innerText || x.textContent || '').trim()),
            data: rows.map(r => Array.from(r.querySelectorAll('td')).map(td => td.innerHTML))
          };
        }
      }
      return best;
    }
    """)
    if not dom:
        raise RuntimeError(f"No data table found on {url}")

    headers = dom.get("headers") or []
    rows = normalize_rows(dom.get("data") or [], headers, url)
    return {
        "kind": kind,
        "url": url,
        "title": title,
        "mode": "dom+ajax-probe",
        "html_table_count": table_count,
        "selected_table_id": dom.get("id"),
        "headers": headers,
        "rendered_row_count": len(rows),
        "ajax_row_count": ajax_row_count,
        "xhr_fetch_urls": xhr_urls,
        "ajax_info": ajax_info,
        "sample_rows": rows[:3],
    }


async def main():
    result = {"generated_at": datetime.now(timezone.utc).isoformat(), "pages": {}}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(locale="ru-RU")
        for kind, url in PAGES.items():
            page = await context.new_page()
            try:
                data = await scrape_page(page, kind, url)
                result["pages"][kind] = data
                print(f"{kind}: rendered={data['rendered_row_count']}; ajax={data['ajax_row_count']}; table={data['selected_table_id']}")
                print("  headers:", data["headers"])
                print("  ajax shape:", json.dumps(data["ajax_info"], ensure_ascii=False)[:4000])
            except Exception as exc:
                result["pages"][kind] = {"kind": kind, "url": url, "error": repr(exc)}
                print(f"{kind}: ERROR: {exc!r}")
            finally:
                await page.close()
        await browser.close()

    OUT.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    errors = [x for x in result["pages"].values() if "error" in x]
    if errors:
        raise SystemExit(2)


if __name__ == "__main__":
    asyncio.run(main())
