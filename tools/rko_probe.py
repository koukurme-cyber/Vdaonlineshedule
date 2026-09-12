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

FIELD_MAP = {
    "online": {
        "название_группы": "name",
        "дата_регистрации": "registration_date",
        "расписание_группы": "schedule",
        "как_попасть": "how_to_join",
        "примечание": "note",
        "публичный_e_mail": "public_email",
        "дата_рождения_группы": "group_birthday",
        "дата_обновления_группы": "updated_date",
    },
    "offline": {
        "название_группы": "name",
        "страна": "country",
        "город": "city",
        "дата_регистрации": "registration_date",
        "адрес_проведения": "address",
        "расписание": "schedule",
        "пояснение": "note",
        "способ_связи": "web",
        "дата_рождения": "group_birthday",
        "телефон_группы": "phone",
        "дополнительный_e_mail": "public_email",
        "координата_на_карте": "coordinates",
    },
}

URL_RE = re.compile(r"https?://[^\s<>\"]+", re.I)


def clean_text(value):
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    soup = BeautifulSoup(value, "lxml")
    return re.sub(r"\s+", " ", soup.get_text(" ", strip=True)).strip()


def extract_links(value, base_url):
    if not isinstance(value, str):
        return []
    soup = BeautifulSoup(value, "lxml")
    links = []
    for a in soup.find_all("a", href=True):
        href = urljoin(base_url, a["href"].strip())
        if href not in links:
            links.append(href)
    for found in URL_RE.findall(clean_text(value)):
        found = found.rstrip(".,);]")
        if found not in links:
            links.append(found)
    return links


def sanitize_record(kind, item, base_url):
    values = item.get("value", {}) if isinstance(item, dict) else {}
    if not isinstance(values, dict):
        return None
    record = {}
    source_id = values.get("___id___")
    if source_id not in (None, ""):
        record["source_id"] = source_id
    all_links = []
    for source_key, target_key in FIELD_MAP[kind].items():
        if source_key not in values:
            continue
        raw = values.get(source_key)
        text = clean_text(raw)
        if text:
            record[target_key] = text
        for link in extract_links(raw, base_url):
            if link not in all_links:
                all_links.append(link)
    if all_links:
        record["links"] = all_links
    return record if len(record) > (1 if "source_id" in record else 0) else None


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
    await page.wait_for_timeout(3000)

    ninja_url = next(
        (u for u in xhr_urls if "ninja_tables_public_action" in u and "get-all-data" in u),
        None,
    )
    if not ninja_url:
        raise RuntimeError("Ninja Tables data request not found")

    response = await page.request.get(ninja_url, timeout=60000)
    if not response.ok:
        raise RuntimeError(f"Ninja Tables request failed: HTTP {response.status}")
    payload = await response.json()
    if not isinstance(payload, list):
        raise RuntimeError(f"Unexpected Ninja Tables payload: {type(payload).__name__}")

    records = []
    for item in payload:
        record = sanitize_record(kind, item, url)
        if record:
            records.append(record)

    return {
        "kind": kind,
        "source_url": url,
        "table_data_url": ninja_url,
        "raw_row_count": len(payload),
        "record_count": len(records),
        "records": records,
    }


async def main():
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "privacy_note": (
            "Only explicitly public/useful fields are retained. Registration e-mails, "
            "contact-person fields and internal display flags from the raw Ninja Tables payload are discarded."
        ),
        "pages": {},
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(locale="ru-RU")
        for kind, url in PAGES.items():
            page = await context.new_page()
            try:
                data = await scrape_page(page, kind, url)
                result["pages"][kind] = data
                print(f"{kind}: {data['record_count']} records from {data['raw_row_count']} raw rows")
                for sample in data["records"][:2]:
                    print("  sample:", json.dumps(sample, ensure_ascii=False))
            except Exception as exc:
                result["pages"][kind] = {"kind": kind, "source_url": url, "error": repr(exc)}
                print(f"{kind}: ERROR: {exc!r}")
            finally:
                await page.close()
        await browser.close()

    OUT.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    errors = [page for page in result["pages"].values() if "error" in page]
    if errors:
        raise SystemExit(2)


if __name__ == "__main__":
    asyncio.run(main())
