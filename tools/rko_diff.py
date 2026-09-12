#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

KINDS = ("online", "offline")
FIELD_LABELS = {
    "name": "Название",
    "country": "Страна",
    "city": "Город",
    "registration_date": "Дата регистрации",
    "updated_date": "Дата обновления",
    "address": "Адрес",
    "schedule": "Расписание",
    "how_to_join": "Как попасть",
    "note": "Примечание",
    "web": "Способ связи",
    "group_birthday": "Дата рождения группы",
    "phone": "Телефон",
    "public_email": "Публичный e-mail",
    "coordinates": "Координаты",
    "links": "Ссылки",
}
IGNORED_FIELDS = {"registration_date", "updated_date"}


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def record_key(record: Dict[str, Any]) -> str:
    source_id = record.get("source_id")
    if source_id not in (None, ""):
        return str(source_id)
    # Fallback only if the source ever stops returning IDs.
    return "fallback:" + "|".join(
        str(record.get(k, "")).strip().lower() for k in ("name", "city", "country")
    )


def canonical_records(current: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {}
    for kind in KINDS:
        page = current.get("pages", {}).get(kind, {})
        records = page.get("records", [])
        if not isinstance(records, list):
            raise ValueError(f"{kind}: records is not a list")
        cleaned = []
        seen = set()
        for record in records:
            if not isinstance(record, dict):
                continue
            key = record_key(record)
            if key in seen:
                raise ValueError(f"{kind}: duplicate source id/key {key}")
            seen.add(key)
            cleaned.append(record)
        cleaned.sort(key=lambda r: record_key(r))
        result[kind] = cleaned
    return result


def baseline_payload(records: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    return {
        "format": 1,
        "source": "adultchildren.ru RKO group tables",
        "pages": records,
    }


def as_map(records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {record_key(r): r for r in records}


def short_identity(kind: str, record: Dict[str, Any]) -> str:
    name = str(record.get("name", "Без названия")).strip()
    if kind == "offline":
        city = str(record.get("city", "")).strip()
        if city:
            return f"{name} — {city}"
    return name


def fmt(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(x) for x in value) or "—"
    text = str(value or "").strip()
    return text if text else "—"


def compare_kind(kind: str, old: List[Dict[str, Any]], new: List[Dict[str, Any]]) -> Tuple[List, List, List]:
    old_map = as_map(old)
    new_map = as_map(new)
    added = [new_map[k] for k in sorted(new_map.keys() - old_map.keys())]
    removed = [old_map[k] for k in sorted(old_map.keys() - new_map.keys())]
    modified = []

    for key in sorted(old_map.keys() & new_map.keys()):
        before = old_map[key]
        after = new_map[key]
        changes = []
        fields = sorted((set(before) | set(after)) - {"source_id"} - IGNORED_FIELDS)
        for field in fields:
            if before.get(field) != after.get(field):
                changes.append((field, before.get(field), after.get(field)))
        if changes:
            modified.append((after, changes))
    return added, removed, modified


def render_report(old_pages, new_pages, initialized=False) -> Tuple[str, int]:
    lines = ["# Изменения расписания групп РКО", ""]
    if initialized:
        lines += [
            "Создан исходный снимок. Сравнивать пока не с чем.",
            "",
            f"- Онлайн-групп: **{len(new_pages['online'])}**",
            f"- Очных групп: **{len(new_pages['offline'])}**",
            "",
        ]
        return "\n".join(lines), 0

    total_changes = 0
    summaries = []
    sections = []
    for kind, title in (("online", "Онлайн-группы"), ("offline", "Очные группы")):
        added, removed, modified = compare_kind(kind, old_pages.get(kind, []), new_pages.get(kind, []))
        count = len(added) + len(removed) + len(modified)
        total_changes += count
        summaries.append(
            f"- {title}: +{len(added)} новых, -{len(removed)} исчезнувших, {len(modified)} изменённых"
        )
        if not count:
            continue
        block = [f"## {title}", ""]
        if added:
            block += ["### Новые", ""]
            for record in added:
                block.append(f"- **{short_identity(kind, record)}** (ID {record_key(record)})")
            block.append("")
        if removed:
            block += ["### Исчезли из списка РКО", ""]
            for record in removed:
                block.append(f"- **{short_identity(kind, record)}** (ID {record_key(record)})")
            block += ["", "> Исчезновение из списка само по себе не означает закрытие группы; это требует проверки.", ""]
        if modified:
            block += ["### Изменились", ""]
            for record, changes in modified:
                block.append(f"#### {short_identity(kind, record)} (ID {record_key(record)})")
                for field, before, after in changes:
                    label = FIELD_LABELS.get(field, field)
                    block.append(f"- **{label}:** `{fmt(before)}` → `{fmt(after)}`")
                block.append("")
        sections.extend(block)

    lines += summaries + [""]
    if total_changes == 0:
        lines += ["Изменений в значимых полях не найдено.", ""]
    else:
        lines += [f"Всего событий: **{total_changes}**", ""] + sections
    return "\n".join(lines), total_changes


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--current", default="rko_probe_output.json")
    parser.add_argument("--baseline", default="data/rko_snapshot.json")
    parser.add_argument("--report", default="rko_changes.md")
    parser.add_argument("--status", default="rko_diff_status.env")
    args = parser.parse_args()

    current_path = Path(args.current)
    baseline_path = Path(args.baseline)
    report_path = Path(args.report)
    status_path = Path(args.status)

    current = load_json(current_path)
    new_pages = canonical_records(current)

    initialized = not baseline_path.exists()
    if initialized:
        old_pages = {kind: [] for kind in KINDS}
    else:
        baseline = load_json(baseline_path)
        old_pages = baseline.get("pages", {})

    report, change_count = render_report(old_pages, new_pages, initialized=initialized)
    report_path.write_text(report, encoding="utf-8")

    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text(
        json.dumps(baseline_payload(new_pages), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    status_path.write_text(
        f"CHANGES_FOUND={1 if change_count else 0}\nCHANGE_COUNT={change_count}\nINITIALIZED={1 if initialized else 0}\n",
        encoding="utf-8",
    )
    print(report)


if __name__ == "__main__":
    main()
