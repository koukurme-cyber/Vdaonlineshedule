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
    return {"text": text, "links": links, "html": value}


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


async def scrape_page(page, kind, url):
    xhr_urls = []

    def on_response(response):
        try:
            if response.request.resource_type in {"xhr", "fetch"}:
                if response.url not in xhr_urls:
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

    # Prefer DataTables' own API: it exposes all rows, including columns hidden
    # behind the responsive '+' control, without clicking every record.
    dt = await page.evaluate("""
    () => {
      const jq = window.jQuery;
      if (!jq || !jq.fn || !jq.fn.dataTable) return null;
      const tables = Array.from(jq.fn.dataTable.tables());
      let best = null;
      for (const table of tables) {
        try {
          const api = jq(table).DataTable();
          const count = api.rows().count();
          const headers = api.columns().header().toArray().map(h => (h.innerText || h.textContent || '').trim());
          const data = api.rows().data().toArray().map(row => {
            if (Array.isArray(row)) return row.map(v => v == null ? '' : String(v));
            if (row && typeof row === 'object') {
              const out = {};
              for (const [k,v] of Object.entries(row)) out[k] = v == null ? '' : String(v);
              return out;
            }
            return String(row ?? '');
          });
          if (!best || count > best.count) best = {count, headers, data, id: table.id || null};
        } catch (e) {}
      }
      return best;
    }
    """)

    mode = "datatables"
    if dt and dt.get("data"):
        headers = dt.get("headers") or []
        raw_rows = dt["data"]
        table_id = dt.get("id")
    else:
        mode = "dom"
        # Generic fallback: choose the table with the most body rows.
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
        raw_rows = dom.get("data") or []
        table_id = dom.get("id")

    rows = normalize_rows(raw_rows, headers, url)
    return {
        "kind": kind,
        "url": url,
        "title": title,
        "mode": mode,
        "html_table_count": table_count,
        "selected_table_id": table_id,
        "headers": headers,
        "row_count": len(rows),
        "xhr_fetch_urls": xhr_urls,
        "rows": rows,
    }


async def main():
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pages": {},
    }
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            locale="ru-RU",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "Chrome/140.0 Safari/537.36"
            ),
        )
        for kind, url in PAGES.items():
            page = await context.new_page()
            try:
                data = await scrape_page(page, kind, url)
                result["pages"][kind] = data
                print(f"{kind}: {data['row_count']} rows; mode={data['mode']}; table={data['selected_table_id']}")
                print("  headers:", data["headers"])
                print("  xhr/fetch:", data["xhr_fetch_urls"][:10])
                for sample in data["rows"][:2]:
                    preview = {k: v["text"] for k, v in sample["cells"].items()}
                    print("  sample:", json.dumps(preview, ensure_ascii=False))
            except Exception as exc:
                result["pages"][kind] = {"kind": kind, "url": url, "error": repr(exc)}
                print(f"{kind}: ERROR: {exc!r}")
            finally:
                await page.close()
        await browser.close()

    OUT.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved: {OUT}")

    errors = [x for x in result["pages"].values() if "error" in x]
    empty = [x for x in result["pages"].values() if x.get("row_count", 0) == 0 and "error" not in x]
    if errors or empty:
        raise SystemExit(2)


if __name__ == "__main__":
    asyncio.run(main())
