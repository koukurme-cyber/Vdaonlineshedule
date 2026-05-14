import asyncio
import csv
import hashlib
import io
import json
import os
import random
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

BOT_TOKEN = os.getenv("BOTTOKEN") or os.getenv("BOT_TOKEN")
SUBSCRIBERS_FILE = Path(os.getenv("SUBSCRIBERS_FILE", "vdasubscribers.json"))
LIVE_GROUPS_FILE = Path(os.getenv("LIVE_GROUPS_FILE", "livegroups.tsv"))
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "30"))
DEFAULT_DAILY_HOUR = 7
DEFAULT_REMIND_BEFORE = [60]
HTML_MODE = "HTML"
DAY_HOUR_CHOICES = [5, 6, 7, 8, 9]
MAX_MESSAGE_LEN = 3800
CITY_PAGE_SIZE = 40

DAYS = [
    "Понедельник",
    "Вторник",
    "Среда",
    "Четверг",
    "Пятница",
    "Суббота",
    "Воскресенье",
]

SLOGANS_AND_AFFIRMATIONS = [
    "Программа простая, но не лёгкая",
    "Жизнь больше, чем просто выживание",
    "Можно жить по-другому",
    "Только сегодня",
    "Не суетись",
    "Не усложняй",
    "Прогресс, а не совершенство",
    "Первым делом — главное",
    "И эта боль тоже пройдёт",
    "Отпусти. Пусти Бога",
    "Возвращайтесь снова и снова",
]

SPB_SUBURBS = [
    "Санкт-Петербург",
    "Пушкин",
    "Петергоф",
    "Колпино",
    "Пушкин (СПб)",
    "Петергоф (СПб)",
    "Колпино (СПб)",
    "Всеволожск",
    "Выборг",
    "Кириши",
]

POPULAR_CITIES = [
    "Москва",
    "Санкт-Петербург",
    "Казань",
    "Новосибирск",
    "Екатеринбург",
    "Нижний Новгород",
    "Краснодар",
    "Самара",
    "Ростов-на-Дону",
]

ONLINE_SCHEDULE = {
    0: [
        ("05:00", "Восход", "https://t.me/+gdi_B_ctmVJkMTAy"),
        ("07:00", "ВДА Утро", "https://t.me/+KBt9VaElvMA4NTcy"),
        ("07:00", "Маяк ВДА", "https://t.me/+1XGQ4SDkR8M0N2Yy"),
        ("08:00", "Единство утро", "https://t.me/ACAgroupUnityMoscow"),
        ("08:00", "Говори Доверяй Чувствуй", "https://t.me/govori_vda"),
        ("09:00", "Доверие", "https://t.me/VDADoverie"),
        ("12:00", "День за днём", "https://t.me/+BwAsiX1KsGljZjQy"),
        ("17:00", "Шаг за шагом", "https://t.me/joinchat/4SFNdPrxumNkYzky"),
        ("18:00", "Весна", "https://t.me/vdavesna_2021"),
        ("19:00", "Рассвет", "https://t.me/+OOw9IMnM5x1hNDJi"),
        ("19:00", "Маяк ВДА", "https://t.me/+1XGQ4SDkR8M0N2Yy"),
        ("21:00", "ДЫШИ!", "https://t.me/breathelivebe"),
        ("21:00", "ВДА ВЕЧЕР", "https://t.me/vda_vecher"),
        ("21:00", "Свобода", "https://t.me/vda_svoboda"),
    ],
    1: [
        ("05:00", "Восход", "https://t.me/+gdi_B_ctmVJkMTAy"),
        ("07:00", "ВДА Утро", "https://t.me/+KBt9VaElvMA4NTcy"),
        ("07:00", "Маяк ВДА", "https://t.me/+1XGQ4SDkR8M0N2Yy"),
        ("09:00", "Доверие", "https://t.me/VDADoverie"),
        ("12:00", "День за днём", "https://t.me/+BwAsiX1KsGljZjQy"),
        ("15:00", "ВДА вокруг света", "https://t.me/+nFn14RqYkyozZmUy"),
        ("18:00", "Весна", "https://t.me/vdavesna_2021"),
        ("19:00", "Артплей", "https://t.me/VDAartPlay"),
        ("19:00", "Рассвет", "https://t.me/+OOw9IMnM5x1hNDJi"),
        ("19:00", "Маяк ВДА", "https://t.me/+1XGQ4SDkR8M0N2Yy"),
        ("20:00", "Феникс", "https://t.me/+1GAp8vi4hyNmMzUy"),
        ("20:00", "По шагам Тони А.", "https://t.me/+ajasg4oH0SU3MjFi"),
        ("21:00", "ДЫШИ!", "https://t.me/breathelivebe"),
        ("21:00", "ВДА ВЕЧЕР", "https://t.me/vda_vecher"),
        ("21:00", "Свобода", "https://t.me/vda_svoboda"),
    ],
    2: [
        ("05:00", "Восход", "https://t.me/+gdi_B_ctmVJkMTAy"),
        ("07:00", "ВДА Утро", "https://t.me/+KBt9VaElvMA4NTcy"),
        ("07:00", "Маяк ВДА", "https://t.me/+1XGQ4SDkR8M0N2Yy"),
        ("08:00", "Единство утро", "https://t.me/ACAgroupUnityMoscow"),
        ("09:00", "Доверие", "https://t.me/VDADoverie"),
        ("12:00", "День за днём", "https://t.me/+BwAsiX1KsGljZjQy"),
        ("14:00", "Венеция", "https://t.me/joinchat/AocB9y6QC_k2ZjJi"),
        ("18:00", "Весна", "https://t.me/vdavesna_2021"),
        ("19:00", "Рассвет", "https://t.me/+OOw9IMnM5x1hNDJi"),
        ("19:00", "Вместе", "https://chat.whatsapp.com/0CvEyMffhB60ZHQcShdva7"),
        ("19:00", "Точка Опоры", "https://us06web.zoom.us/j/88678026186?pwd=QYiLNtlro6gEZ3f6eVZdwu7CAHbVF3.1"),
        ("19:00", "Светский круг (жен.)", "https://t.me/+Pyarr0R7MSEyMGIy"),
        ("19:00", "ВДА-ВЕРА", "https://t.me/+J2m1MAbQ818zNTFi"),
        ("19:00", "Маяк ВДА", "https://t.me/+1XGQ4SDkR8M0N2Yy"),
        ("19:30", "Эффект бабочки", "https://t.me/+FcaUkHDOuMpkMTI8"),
        ("20:00", "Мужская ВДА", "https://t.me/+ewtjezZaCtM5YTdi"),
        ("20:00", "Доверие (вопросы)", "https://t.me/VDADoverie"),
        ("21:00", "ДЫШИ!", "https://t.me/breathelivebe"),
        ("21:00", "ВДА ВЕЧЕР", "https://t.me/vda_vecher"),
        ("21:00", "Свобода", "https://t.me/vda_svoboda"),
        ("22:00", "Восст. Люб. Род.", "https://us02web.zoom.us/j/86893102645?pwd=d2N1UWFDY3Y5RXBpTUdQcWpDdEZVUT09UT09"),
    ],
    3: [
        ("05:00", "Восход", "https://t.me/+gdi_B_ctmVJkMTAy"),
        ("07:00", "Маяк ВДА", "https://t.me/+1XGQ4SDkR8M0N2Yy"),
        ("07:00", "ВДА Утро", "https://t.me/+KBt9VaElvMA4NTcy"),
        ("09:00", "Доверие", "https://t.me/VDADoverie"),
        ("12:00", "День за днём", "https://t.me/+BwAsiX1KsGljZjQy"),
        ("14:00", "Венеция", "https://t.me/joinchat/AocB9y6QC_k2ZjJi"),
        ("18:00", "Весна", "https://t.me/vdavesna_2021"),
        ("19:00", "праВДА", "https://t.me/+ZYfdfXWBRltjZGEy"),
        ("19:00", "ВДА в Рязани", "https://t.me/+MHSRTpkJliw5YzUy"),
        ("19:00", "Артплей (онлайн)", "https://t.me/VDAartPlay"),
        ("19:00", "Рассвет", "https://t.me/+OOw9IMnM5x1hNDJi"),
        ("19:00", "Маяк ВДА", "https://t.me/+1XGQ4SDkR8M0N2Yy"),
        ("21:00", "ДЫШИ!", "https://t.me/breathelivebe"),
        ("21:00", "ВДА ВЕЧЕР", "https://t.me/vda_vecher"),
        ("21:00", "Свобода", "https://t.me/vda_svoboda"),
    ],
    4: [
        ("05:00", "Восход", "https://t.me/+gdi_B_ctmVJkMTAy"),
        ("07:00", "ВДА Утро", "https://t.me/+KBt9VaElvMA4NTcy"),
        ("07:00", "Маяк ВДА", "https://t.me/+1XGQ4SDkR8M0N2Yy"),
        ("08:00", "Говори Доверяй Чувствуй", "https://t.me/govori_vda"),
        ("08:00", "Единство утро", "https://t.me/ACAgroupUnityMoscow"),
        ("09:00", "Доверие", "https://t.me/VDADoverie"),
        ("12:00", "День за днём", "https://t.me/+BwAsiX1KsGljZjQy"),
        ("14:00", "Венеция", "https://t.me/joinchat/AocB9y6QC_k2ZjJi"),
        ("14:00", "ВДА вокруг света", "https://t.me/+nFn14RqYkyozZmUy"),
        ("18:00", "Весна", "https://t.me/vdavesna_2021"),
        ("19:00", "Братский Круг", "https://t.me/+uEG2E5FVndA0YTc6"),
        ("19:00", "Рассвет", "https://t.me/+OOw9IMnM5x1hNDJi"),
        ("19:00", "Маяк ВДА", "https://t.me/+1XGQ4SDkR8M0N2Yy"),
        ("20:00", "Феникс", "https://t.me/+1GAp8vi4hyNmMzUy"),
        ("20:00", "Доверие (Любящий Родитель)", "https://t.me/VDADoverie"),
        ("21:00", "ДЫШИ!", "https://t.me/breathelivebe"),
        ("21:00", "ВДА ВЕЧЕР", "https://t.me/vda_vecher"),
        ("21:00", "Свобода", "https://t.me/vda_svoboda"),
    ],
    5: [
        ("05:00", "Восход", "https://t.me/+gdi_B_ctmVJkMTAy"),
        ("08:00", "ВДА Утро", "https://t.me/+KBt9VaElvMA4NTcy"),
        ("09:00", "Доверие", "https://t.me/VDADoverie"),
        ("12:00", "День за днём", "https://t.me/+BwAsiX1KsGljZjQy"),
        ("14:00", "Венеция", "https://t.me/joinchat/AocB9y6QC_k2ZjJi"),
        ("18:00", "ВДА «Весь мир»", "https://t.me/+zfYoUgHPiVRhN2Iy"),
        ("18:00", "Весна", "https://t.me/vdavesna_2021"),
        ("19:00", "Девчата", "https://t.me/+FKs5HqhF711iZTli"),
        ("19:00", "ВДА-ВЕРА", "https://t.me/+J2m1MAbQ818zNTFi"),
        ("19:00", "Рассвет", "https://t.me/+OOw9IMnM5x1hNDJi"),
        ("20:00", "Восст. Люб. Род.", "https://us02web.zoom.us/j/86893102645?pwd=d2N1UWFDY3Y5RXBpTUdQcWpDdEZVUT09UT09"),
        ("21:00", "ВДА ВЕЧЕР", "https://t.me/vda_vecher"),
        ("21:00", "Свобода", "https://t.me/vda_svoboda"),
    ],
    6: [
        ("05:00", "Восход", "https://t.me/+gdi_B_ctmVJkMTAy"),
        ("07:00", "Маяк ВДА", "https://t.me/+1XGQ4SDkR8M0N2Yy"),
        ("08:00", "ВДА Утро", "https://t.me/+KBt9VaElvMA4NTcy"),
        ("10:00", "ВДА НСК онлайн", "https://t.me/VDANsk"),
        ("12:00", "Только сегодня (MAX)", "https://max.ru/join/y25EwyRl_K_F1OeJv5JbewpRpww71IKfWS-gwtTB65Q"),
        ("12:00", "День за днём", "https://t.me/+BwAsiX1KsGljZjQy"),
        ("12:30", "Мужская ВДА", "https://t.me/+ewtjezZaCtM5YTdi"),
        ("14:00", "Венеция", "https://t.me/joinchat/AocB9y6QC_k2ZjJi"),
        ("18:00", "Ежедневник ВДА", "https://t.me/VDAOXOTNIRYAD"),
        ("18:00", "Весна", "https://t.me/vdavesna_2021"),
        ("19:00", "Сила и Надежда (Watsup)", "https://chat.whatsapp.com/CUc0VVemIvl7Aoe2cuYCav"),
        ("19:00", "Рассвет", "https://t.me/+OOw9IMnM5x1hNDJi"),
        ("19:30", "Эффект бабочки", "https://t.me/+FcaUkHDOuMpkMTI8"),
        ("20:00", "Огоньки", "https://t.me/ogonki2025"),
        ("20:00", "Маяк ВДА", "https://t.me/+1XGQ4SDkR8M0N2Yy"),
        ("21:00", "ВДА ВЕЧЕР", "https://t.me/vda_vecher"),
        ("21:00", "Свобода", "https://t.me/vda_svoboda"),
    ],
}


def add_online_group(day_indices: Iterable[int], time_str: str, name: str, url: str):
    for day_index in day_indices:
        day_groups = ONLINE_SCHEDULE.setdefault(day_index, [])
        if not any(t == time_str and n == name for t, n, _ in day_groups):
            day_groups.append((time_str, name, url))


# Дополнительные онлайн-группы, добавленные вручную.
# ONLINE_SCHEDULE хранит только время начала, поэтому окончание собрания здесь не указывается.
add_online_group([3], "20:15", "ВДА онлайн Минск", "https://t.me/+iIAYtReKyd04YTdi")
for _time in ["09:00", "10:00", "11:00", "13:00", "14:00", "15:00", "19:00", "20:00", "21:00"]:
    add_online_group(range(7), _time, "БШН", "https://t.me/bshnvda")
add_online_group([2, 4, 5], "13:00", "Начало", "https://max.ru/join/K1vR_TmHfgSBKnKR9DT04dX1vO81a3GuyBP3kc0fsio")


class LiveGroupSearch(StatesGroup):
    waiting_for_city = State()


class SubCitySearch(StatesGroup):
    waiting_for_city = State()


class GroupNameSearch(StatesGroup):
    waiting_for_name = State()


REPLY_MAIN_MENU = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="🌐 Онлайн-встречи"),
            KeyboardButton(text="🏙 Живые встречи"),
        ],
        [
            KeyboardButton(text="🔔 Мои подписки"),
            KeyboardButton(text="Ещё"),
        ],
        [
            KeyboardButton(text="🔍 Найти группу"),
        ],
    ],
    resize_keyboard=True,
    is_persistent=True,
)

def moscow_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=3)


def escape_html(text: str) -> str:
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def split_long_message(text: str, limit: int = MAX_MESSAGE_LEN) -> List[str]:
    parts = []
    while len(text) > limit:
        cut = text.rfind("\n", 0, limit)
        if cut == -1:
            cut = limit
        parts.append(text[:cut].strip())
        text = text[cut:].lstrip()
    if text.strip():
        parts.append(text.strip())
    return parts


def time_to_minutes(hhmm: str) -> int:
    h, m = map(int, hhmm.split(":"))
    return h * 60 + m


def normalize_remind_before(value) -> List[int]:
    if isinstance(value, int):
        value = [value]
    if not isinstance(value, list):
        return DEFAULT_REMIND_BEFORE.copy()
    result = []
    for item in value:
        try:
            iv = int(item)
        except Exception:
            continue
        if iv in {60, 120} and iv not in result:
            result.append(iv)
    return sorted(result) or DEFAULT_REMIND_BEFORE.copy()


def normalize_group_type(value) -> Optional[str]:
    return value if value in {"online", "live"} else None


def normalize_group_payload(groups) -> Dict[str, dict]:
    if not isinstance(groups, dict):
        return {}
    normalized = {}
    for name, payload in groups.items():
        if not isinstance(name, str) or not isinstance(payload, dict):
            continue
        group_type = normalize_group_type(payload.get("type"))
        if not group_type:
            continue
        info = payload.get("info") if isinstance(payload.get("info"), dict) else {}
        normalized[name] = {"type": group_type, "info": info}
    return normalized


def normalize_settings(settings: Optional[dict], fallback_hour: int, fallback_remind) -> dict:
    settings = settings if isinstance(settings, dict) else {}
    try:
        daily_hour = int(settings.get("daily_hour", fallback_hour))
    except Exception:
        daily_hour = fallback_hour
    if daily_hour not in DAY_HOUR_CHOICES:
        daily_hour = DEFAULT_DAILY_HOUR
    return {
        "daily_hour": daily_hour,
        "remind_before": normalize_remind_before(settings.get("remind_before", fallback_remind)),
    }


def normalize_user_sub(data: Optional[dict]) -> dict:
    data = data if isinstance(data, dict) else {}
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    last_reminders = meta.get("last_reminders") if isinstance(meta.get("last_reminders"), dict) else {}

    legacy_hour = data.get("dailyhour", data.get("daily_hour", DEFAULT_DAILY_HOUR))
    legacy_remind = data.get("remindbefore", data.get("remind_before", DEFAULT_REMIND_BEFORE))

    online_settings_raw = data.get("online_settings") if isinstance(data.get("online_settings"), dict) else data.get("onlinesettings")
    live_settings_raw = data.get("live_settings") if isinstance(data.get("live_settings"), dict) else data.get("livesettings")

    return {
        "country": data.get("country") if isinstance(data.get("country"), str) else None,
        "city": data.get("city") if isinstance(data.get("city"), str) else None,
        "all_online": bool(data.get("all_online", data.get("allonline", False))),
        "all_live": bool(data.get("all_live", data.get("alllive", False))),
        "groups": normalize_group_payload(data.get("groups")),
        "meta": {
            "last_daily_sent": meta.get("last_daily_sent", meta.get("lastdailysent")),
            "last_reminders": {str(k): str(v) for k, v in last_reminders.items()},
        },
        "online_settings": normalize_settings(online_settings_raw, legacy_hour, legacy_remind),
        "live_settings": normalize_settings(live_settings_raw, legacy_hour, legacy_remind),
    }


class SubscriberStore:
    def __init__(self, path: Path):
        self.path = path

    def load_all(self) -> dict:
        try:
            with self.path.open("r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def save_all(self, data: dict):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        with temp_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        temp_path.replace(self.path)

    def get_user(self, uid: str) -> dict:
        return normalize_user_sub(self.load_all().get(uid))

    def set_user(self, uid: str, data: dict):
        subs = self.load_all()
        subs[uid] = normalize_user_sub(data)
        self.save_all(subs)

    def remove_user(self, uid: str):
        subs = self.load_all()
        subs.pop(uid, None)
        self.save_all(subs)


STORE = SubscriberStore(SUBSCRIBERS_FILE)


def read_live_source() -> str:
    for path in [LIVE_GROUPS_FILE, Path("live_groups.tsv"), Path("livegroups.tsv")]:
        if path.exists():
            return path.read_text(encoding="utf-8")
    return ""


def clean_live_cell(value: str, schedule: bool = False) -> str:
    value = "" if value is None else str(value)
    value = value.replace("\ufeff", "")
    value = re.sub(
        r"(?i)([А-ЯЁA-Z])\s*(?:<br\s*/?>|\r\n|\n|\r)\s*([а-яёa-z])",
        r"\1\2",
        value,
    )
    separator = "; " if schedule else " "
    value = re.sub(r"(?i)<br\s*/?>", separator, value)
    value = value.replace("\r\n", separator).replace("\n", separator).replace("\r", separator).replace("\t", " ")
    value = re.sub(r"\s+", " ", value)
    value = re.sub(r"\s+([,.;:])", r"\1", value)
    value = re.sub(r"([,;])(?=\S)", r"\1 ", value)
    return value.strip(" \t\n\r\"'")


def normalize_live_time_token(value: str) -> str:
    value = value.strip().lower().replace("ч", "").replace(":", ".").replace("-", ".")
    if re.fullmatch(r"\d{1,2}", value):
        value = f"{int(value):02d}.00"
    match = re.fullmatch(r"(\d{1,2})\.(\d{1,2})", value)
    if match:
        return f"{int(match.group(1)):02d}:{int(match.group(2)):02d}"
    return value.replace(".", ":")


def parse_live_schedule(raw_lines: str) -> List[dict]:
    groups = []
    short_map = {
        "пн": 0, "пон": 0, "вт": 1, "вто": 1, "ср": 2, "сре": 2,
        "чт": 3, "чет": 3, "пт": 4, "пят": 4, "сб": 5, "суб": 5,
        "вс": 6, "вос": 6,
    }
    reader = csv.reader(io.StringIO(raw_lines), delimiter="\t")
    for parts in reader:
        parts = [clean_live_cell(c) for c in parts]
        if len(parts) < 5:
            continue
        country, city, name, address, time_str = parts[:5]
        if not country or country.lower() == "страна":
            continue
        time_str = clean_live_cell(time_str, schedule=True).replace('"', '')
        entry_parts = re.split(r";|\\n|\n|(?<=\d)\s+(?=(?:Понедельник|Вторник|Среда|Четверг|Пятница|Суббота|Воскресенье)\b)| и (?=[А-ЯЁа-яё]+\s+(?:с|в|\d))", time_str)
        days = []
        for entry in entry_parts:
            entry = entry.strip()
            if not entry:
                continue
            lower = entry.lower().replace("ё", "е")
            day_found = next(
                (
                    i for i, day_name in enumerate(DAYS)
                    if day_name.lower().replace("ё", "е") in lower
                    or day_name[:3].lower().replace("ё", "е") in lower
                ),
                None,
            )
            if day_found is None:
                for key, idx in short_map.items():
                    if re.search(rf"(?<![а-яё]){re.escape(key)}(?![а-яё])", lower):
                        day_found = idx
                        break
            if day_found is None:
                continue
            occurrence = None
            if "последн" in lower:
                occurrence = "last"
            elif "перв" in lower:
                occurrence = 1
            elif "втор" in lower and "вторник" not in lower:
                occurrence = 2
            elif "треть" in lower:
                occurrence = 3
            elif "четверт" in lower and "четверг" not in lower:
                occurrence = 4
            is_work_meeting = "рабоч" in lower or "рабочка" in lower
            time_pattern = r"(\d{1,2}(?:[.:\-]\d{2})?)"
            times = []
            for match in re.finditer(rf"{time_pattern}\s*(?:[-–—]|до)\s*{time_pattern}", entry, flags=re.I):
                times.append((match.group(1), match.group(2)))
            if not times:
                for match in re.finditer(rf"с\s*{time_pattern}\s*до\s*{time_pattern}", entry, flags=re.I):
                    times.append((match.group(1), match.group(2)))
            if not times:
                single = re.findall(rf"(?:в\s*)?{time_pattern}", entry, flags=re.I)
                if single:
                    start = normalize_live_time_token(single[0])
                    h, m = start.split(":")
                    times = [(start, f"{(int(h) + 1):02d}:{m}")]
            for start, end in times:
                days.append({
                    "day": day_found,
                    "start": normalize_live_time_token(start),
                    "end": normalize_live_time_token(end),
                    "occurrence": occurrence,
                    "is_work_meeting": is_work_meeting,
                })
        if days:
            groups.append({
                "country": country.strip(),
                "city": city.strip(),
                "name": name.strip(),
                "address": address.strip(),
                "days": days,
            })
    return groups

RAW_LIVE = read_live_source()
LIVE_GROUPS = parse_live_schedule(RAW_LIVE) if RAW_LIVE else []
COUNTRY_TO_ID: Dict[str, str] = {}
ID_TO_COUNTRY: Dict[str, str] = {}
LOCATION_TO_ID: Dict[Tuple[str, str], str] = {}
ID_TO_LOCATION: Dict[str, Tuple[str, str]] = {}
CITY_TO_ID: Dict[str, str] = {}  # backward compatibility for old city-only callbacks
ID_TO_CITY: Dict[str, str] = {}  # backward compatibility for old city-only callbacks

for country in sorted({g["country"] for g in LIVE_GROUPS}):
    country_id = str(len(COUNTRY_TO_ID))
    COUNTRY_TO_ID[country] = country_id
    ID_TO_COUNTRY[country_id] = country

for country, city in sorted({(g["country"], g["city"]) for g in LIVE_GROUPS}, key=lambda x: (x[0].lower(), x[1].lower())):
    location_id = str(len(LOCATION_TO_ID))
    LOCATION_TO_ID[(country, city)] = location_id
    ID_TO_LOCATION[location_id] = (country, city)
    CITY_TO_ID.setdefault(city, location_id)
    ID_TO_CITY.setdefault(location_id, city)


def resolve_location_id(location_id: str) -> Tuple[Optional[str], str]:
    if location_id in ID_TO_LOCATION:
        return ID_TO_LOCATION[location_id]
    return None, ID_TO_CITY.get(location_id, location_id)


def get_location_id(city: str, country: Optional[str] = None) -> str:
    if country and (country, city) in LOCATION_TO_ID:
        return LOCATION_TO_ID[(country, city)]
    for (known_country, known_city), location_id in LOCATION_TO_ID.items():
        if known_city == city:
            return location_id
    return CITY_TO_ID.get(city, city)


def get_country_city_label(country: Optional[str], city: str) -> str:
    return f"{country}, {city}" if country and country != "Россия" else city


def get_countries_with_live_groups() -> List[str]:
    return sorted({g["country"] for g in LIVE_GROUPS}, key=lambda x: (x != "Россия", x.lower()))


def city_sort_key(city: str) -> str:
    normalized = re.sub(r"^г\.?\s+", "", city.strip(), flags=re.IGNORECASE)
    normalized = re.sub(r"\s*\(СПб\)\s*$", "", normalized, flags=re.IGNORECASE)
    return normalized.lower()


def get_cities_for_country(country: str) -> List[str]:
    return sorted({g["city"] for g in LIVE_GROUPS if g["country"] == country}, key=city_sort_key)


def make_short_id(prefix: str, name: str) -> str:
    return prefix + hashlib.md5(name.encode("utf-8")).hexdigest()[:10]


ONLINE_GROUP_ID_TO_NAME = {}
ONLINE_GROUP_INFO = {}
for day_groups in ONLINE_SCHEDULE.values():
    for _, name, url in day_groups:
        ONLINE_GROUP_ID_TO_NAME.setdefault(make_short_id("o", name), name)
        ONLINE_GROUP_INFO.setdefault(name, {"url": url})

LIVE_GROUP_ID_TO_NAME = {}
LIVE_GROUP_INFO = {}
for group in LIVE_GROUPS:
    LIVE_GROUP_ID_TO_NAME.setdefault(make_short_id("l", group["name"]), group["name"])
    LIVE_GROUP_INFO.setdefault(group["name"], {"country": group["country"], "city": group["city"], "address": group["address"]})
    
def normalize_city_name(city: str) -> str:
    aliases = {
        "мск": "москва",
        "москва": "москва",
        "спб": "санкт-петербург",
        "питер": "санкт-петербург",
        "петербург": "санкт-петербург",
        "санкт-петербург": "санкт-петербург",
    }
    return aliases.get(city.lower().strip(), city.lower().strip())


def get_searchable_cities(query: str, country: Optional[str] = None) -> List[Tuple[str, str]]:
    query_lower = query.lower().strip()
    is_spb_search = query_lower in {"санкт-петербург", "спб", "питер", "петербург"}
    matched: List[Tuple[str, str]] = []
    seen = set()
    for group in LIVE_GROUPS:
        if country and group["country"] != country:
            continue
        group_country = group["country"]
        city = group["city"]
        city_lower = city.lower()
        country_lower = group_country.lower()
        label_lower = f"{country_lower} {city_lower}"
        if query_lower in city_lower or query_lower in label_lower:
            if is_spb_search and any(suburb.lower() in city_lower and suburb.lower() != "санкт-петербург" for suburb in SPB_SUBURBS):
                continue
            key = (group_country, city)
            if key not in seen:
                matched.append(key)
                seen.add(key)
    if not matched and not is_spb_search:
        for group in LIVE_GROUPS:
            if country and group["country"] != country:
                continue
            group_country = group["country"]
            city = group["city"]
            city_lower = city.lower()
            if any(query_lower in suburb.lower() and query_lower in city_lower for suburb in SPB_SUBURBS):
                key = (group_country, city)
                if key not in seen:
                    matched.append(key)
                    seen.add(key)
    return matched


def week_of_month(dt):
    return ((dt.day - 1) // 7) + 1


def is_last_weekday_of_month(dt):
    return (dt + timedelta(days=7)).month != dt.month


def day_entry_matches_date(day_entry, target_date):
    if day_entry["day"] != target_date.weekday():
        return False
    occurrence = day_entry.get("occurrence")
    if occurrence is None:
        return True
    if occurrence == "last":
        return is_last_weekday_of_month(target_date)
    return week_of_month(target_date) == occurrence


def get_live_groups_for_city(city: str, country: Optional[str] = None) -> List[dict]:
    city_norm = normalize_city_name(city)
    result = []
    for group in LIVE_GROUPS:
        if country and group.get("country") != country:
            continue
        group_city = normalize_city_name(group["city"])
        if city_norm in {"москва", "санкт-петербург"}:
            if group_city == city_norm or group_city.startswith(f"{city_norm},") or group_city.startswith(f"{city_norm} "):
                result.append(group)
        elif city_norm in group_city:
            result.append(group)
    return result


def get_live_groups_for_day(city: str, day_index: int, country: Optional[str] = None):
    now = moscow_now()
    target_date = now.date() + timedelta(days=(day_index - now.weekday()) % 7)
    result = []
    for group in get_live_groups_for_city(city, country):
        for entry in group["days"]:
            if day_entry_matches_date(entry, target_date):
                result.append((group["name"], group["address"], entry["start"], entry["end"], entry.get("is_work_meeting", False)))
    return sorted(result, key=lambda x: x[2])


def get_live_week(city: str, country: Optional[str] = None) -> str:
    city_groups = get_live_groups_for_city(city, country)
    if not city_groups:
        return f"В городе «{escape_html(city)}» живых групп не найдено."
    parts = [f"🏙 Живые группы: {escape_html(get_country_city_label(country, city))}:"]
    today = moscow_now().date()
    monday = today - timedelta(days=today.weekday())
    for offset in range(7):
        target_date = monday + timedelta(days=offset)
        day_name = DAYS[target_date.weekday()]
        items = {
            (
                group["name"],
                group["address"],
                entry["start"],
                entry["end"],
                entry.get("is_work_meeting", False),
            )
            for group in city_groups
            for entry in group["days"]
            if day_entry_matches_date(entry, target_date)
        }
        if items:
            parts.append(format_day_header(day_name, target_date.strftime("%d.%m")))
            parts.extend(format_live_group(name, address, start, end, is_work_meeting) for name, address, start, end, is_work_meeting in sorted(items, key=lambda x: x[2]))
            parts.append("")
    if parts and parts[-1] == "":
        parts.pop()
    return "\n".join(parts)


def get_online_by_day(day_index: int):
    return sorted(ONLINE_SCHEDULE.get(day_index, []), key=lambda x: x[0])


def get_online_full() -> str:
    parts = []
    for i, day_name in enumerate(DAYS):
        groups = get_online_by_day(i)
        if groups:
            lines = [format_day_header(day_name)]
            lines.extend(format_online_group(t, n, u) for t, n, u in groups)
            parts.append("\n".join(lines))
    return "\n\n".join(parts) if parts else "Онлайн-групп нет."



def normalize_search_query(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def collect_online_group_matches(query: str) -> List[Tuple[str, List[Tuple[int, str, str]]]]:
    q = normalize_search_query(query)
    found: Dict[str, List[Tuple[int, str, str]]] = {}
    for day_index, day_groups in ONLINE_SCHEDULE.items():
        for time_str, name, url in day_groups:
            if q in normalize_search_query(name):
                found.setdefault(name, []).append((day_index, time_str, url))
    return sorted(found.items(), key=lambda x: x[0].lower())


def collect_live_group_matches(query: str) -> List[dict]:
    q = normalize_search_query(query)
    result = []
    seen = set()
    for group in LIVE_GROUPS:
        if q not in normalize_search_query(group.get("name", "")):
            continue
        key = (group.get("country"), group.get("city"), group.get("name"), group.get("address"))
        if key in seen:
            continue
        seen.add(key)
        result.append(group)
    return sorted(result, key=lambda g: (g.get("country") != "Россия", g.get("country", "").lower(), city_sort_key(g.get("city", "")), g.get("name", "").lower()))


def format_search_results(query: str) -> str:
    online_matches = collect_online_group_matches(query)
    live_matches = collect_live_group_matches(query)
    if not online_matches and not live_matches:
        return f"По запросу «{escape_html(query)}» группы не найдены."

    lines = [f"🔍 <b>Поиск группы:</b> {escape_html(query)}"]

    if online_matches:
        lines.append("\n🌐 <b>Онлайн</b>")
        for name, occurrences in online_matches[:20]:
            parts = []
            for day_index, time_str, url in sorted(occurrences, key=lambda x: (x[0], x[1]))[:10]:
                parts.append(f"{DAYS[day_index][:2]} {time_str}")
            url = occurrences[0][2]
            extra = "…" if len(occurrences) > 10 else ""
            lines.append(f'• <a href="{url}">{escape_html(name)}</a> — {escape_html(", ".join(parts) + extra)}')
        if len(online_matches) > 20:
            lines.append(f"…и ещё {len(online_matches) - 20} онлайн-групп.")

    if live_matches:
        lines.append("\n🏙 <b>Живые</b>")
        for group in live_matches[:20]:
            day_parts = []
            for entry in sorted(group.get("days", []), key=lambda e: (e.get("day", 0), e.get("start", "")))[:7]:
                day_parts.append(f"{DAYS[entry['day']][:2]} {entry['start']}")
            days_text = ", ".join(day_parts) if day_parts else "расписание не распознано"
            lines.append(
                f"• <b>{escape_html(group['name'])}</b> — "
                f"{escape_html(get_country_city_label(group.get('country'), group.get('city', '')))}; "
                f"{escape_html(group.get('address', ''))}; {escape_html(days_text)}"
            )
        if len(live_matches) > 20:
            lines.append(f"…и ещё {len(live_matches) - 20} живых групп. Уточните запрос.")

    return "\n".join(lines)


def get_user_sub(uid: str) -> dict:
    return STORE.get_user(uid)


def set_user_sub(uid: str, data: dict):
    STORE.set_user(uid, data)


def remove_subscriber(uid: str):
    STORE.remove_user(uid)


def is_user_subscribed_to_online(user_data: dict, group_name: str) -> bool:
    payload = user_data.get("groups", {}).get(group_name)
    return user_data.get("all_online", False) or (payload and payload.get("type") == "online")


def is_user_subscribed_to_live(user_data: dict, group_name: str) -> bool:
    payload = user_data.get("groups", {}).get(group_name)
    return user_data.get("all_live", False) or (payload and payload.get("type") == "live")


def has_online_subscriptions(user_data: dict) -> bool:
    return user_data.get("all_online", False) or any(v.get("type") == "online" for v in user_data.get("groups", {}).values())


def has_live_subscriptions(user_data: dict) -> bool:
    return user_data.get("all_live", False) or any(v.get("type") == "live" for v in user_data.get("groups", {}).values())


def format_online_group(time_str: str, name: str, url: str) -> str:
    return f'🟠 <b>{time_str}</b> — <a href="{url}">{escape_html(name)}</a>'


def format_online_group_with_sub(time_str: str, name: str, url: str, user_data: dict) -> str:
    bell = " 🔔" if is_user_subscribed_to_online(user_data, name) else ""
    return f'🟠 <b>{time_str}</b> — <a href="{url}">{escape_html(name)}</a>{bell}'


def format_live_group(name: str, address: str, start: str, end: str, is_work_meeting: bool = False) -> str:
    label = " 🔧" if is_work_meeting else ""
    return f"• {start}–{end} — {escape_html(name)}{label} — {escape_html(address)}"


def format_live_group_with_sub(name: str, address: str, start: str, end: str, is_work_meeting: bool, user_data: dict) -> str:
    bell = " 🔔" if is_user_subscribed_to_live(user_data, name) else ""
    label = " 🔧" if is_work_meeting else ""
    return f"• {start}–{end} — {escape_html(name)}{label}{bell} — {escape_html(address)}"


def format_day_header(day_name: str, date_label: Optional[str] = None) -> str:
    if date_label:
        return f"━━━ <b>{escape_html(day_name)}</b> · <code>{escape_html(date_label)}</code> ━━━"
    return f"━━━ <b>{escape_html(day_name)}</b> ━━━"


def back_markup(text: str, callback_data: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=text, callback_data=callback_data)]])


def build_main_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🌐 Онлайн-встречи", callback_data="mainonline"),
        InlineKeyboardButton(text="🏙 Живые встречи", callback_data="mainlive"),
    )
    builder.row(
        InlineKeyboardButton(text="🔔 Мои подписки", callback_data="mainsub"),
        InlineKeyboardButton(text="Ещё", callback_data="mainmore"),
    )
    builder.row(InlineKeyboardButton(text="🔍 Найти группу", callback_data="searchgroup"))
    return builder.as_markup()




CONTACTS_TEXT = (
    'По вопросам актуализации информации о группах пишите '
    '<a href="https://t.me/koukurme2022">сюда</a> '
    'либо на <a href="https://adultchildren.ru/">официальный сайт РКО ВДА</a>.'
)


HELP_TEXT = (
    '<b>Помощь</b>\n\n'
    '<b>Главные разделы</b>\n'
    '🌐 Онлайн-встречи — расписание онлайн-групп по дням.\n'
    '🏙 Живые встречи — очные группы по стране, городу и дню недели.\n'
    '🔔 Мои подписки — выбранные группы, утреннее расписание и напоминания.\n'
    'Ещё — фраза поддержки, контакты и справка.\n\n'
    '<b>Команды</b>\n'
    '/start — открыть главное меню.\n'
    '/help — показать эту справку.\n'
    '/slogan — получить случайную фразу поддержки.\n\n'
    '<b>Подписки</b>\n'
    'Можно подписаться на все онлайн-группы, все живые группы выбранного города или отдельные группы. '
    'Напоминания настраиваются отдельно для онлайн- и живых встреч.'
)


def build_more_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="💫 Фраза поддержки", callback_data="mainslogan"),
        InlineKeyboardButton(text="Контакты", callback_data="maincontacts"),
    )
    builder.row(InlineKeyboardButton(text="Помощь", callback_data="mainhelp"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    return builder.as_markup()


def build_live_root_keyboard(user_data: Optional[dict] = None) -> InlineKeyboardMarkup:
    user_data = user_data or {}
    builder = InlineKeyboardBuilder()
    if user_data.get("city"):
        builder.row(InlineKeyboardButton(
            text=f"🏙 Мой город: {get_country_city_label(user_data.get('country'), user_data['city'])}",
            callback_data="livemycity",
        ))
    builder.row(InlineKeyboardButton(text="🌍 Выбрать страну", callback_data="livechoosecountry"))
    builder.row(InlineKeyboardButton(text="🔍 Найти город", callback_data="livesearchcity"))
    builder.row(InlineKeyboardButton(text="🔍 Найти группу", callback_data="searchgroup"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    return builder.as_markup()

def build_online_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📅 Сегодня", callback_data="onlinetoday"),
        InlineKeyboardButton(text="📋 Вся неделя", callback_data="onlinefull"),
    )
    builder.row(InlineKeyboardButton(text="📆 Выбрать день", callback_data="onlinechooseday"))
    builder.row(InlineKeyboardButton(text="🔍 Найти группу", callback_data="searchgroup"))
    builder.row(InlineKeyboardButton(text="🔔 Настроить подписки", callback_data="subonline"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    return builder.as_markup()

def build_live_country_keyboard(prefix: str = "livecountry", back_callback: str = "modelive") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for country in get_countries_with_live_groups():
        builder.button(text=country, callback_data=f"{prefix}{COUNTRY_TO_ID[country]}")
    builder.adjust(2)
    builder.row(InlineKeyboardButton(text="🔍 Найти город", callback_data="livesearchcity"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu" if back_callback == "modelive" else back_callback))
    return builder.as_markup()


def clamp_page(page: int, total_items: int, page_size: int = CITY_PAGE_SIZE) -> int:
    if total_items <= 0:
        return 0
    max_page = (total_items - 1) // page_size
    return max(0, min(page, max_page))


def parse_country_page(raw: str) -> Tuple[str, int]:
    if ":p" not in raw:
        return raw, 0
    country_id, page_raw = raw.rsplit(":p", 1)
    try:
        page = int(page_raw)
    except Exception:
        page = 0
    return country_id, max(0, page)


def city_page_title(country: str, page: int, total_items: int) -> str:
    if total_items <= CITY_PAGE_SIZE:
        return f"🏙 Выберите город: <b>{escape_html(country)}</b>"
    max_page = (total_items - 1) // CITY_PAGE_SIZE
    return f"🏙 Выберите город: <b>{escape_html(country)}</b> · стр. {page + 1}/{max_page + 1}"


def add_city_page_buttons(builder: InlineKeyboardBuilder, prefix: str, country: str, page: int) -> Tuple[int, int]:
    cities = get_cities_for_country(country)
    page = clamp_page(page, len(cities))
    start = page * CITY_PAGE_SIZE
    end = start + CITY_PAGE_SIZE
    for city in cities[start:end]:
        builder.button(text=city, callback_data=f"{prefix}{get_location_id(city, country)}")
    builder.adjust(2)
    return page, len(cities)


def add_city_pagination_buttons(builder: InlineKeyboardBuilder, country: str, page: int, country_callback_prefix: str) -> None:
    cities = get_cities_for_country(country)
    if len(cities) <= CITY_PAGE_SIZE:
        return
    country_id = COUNTRY_TO_ID.get(country, country)
    max_page = (len(cities) - 1) // CITY_PAGE_SIZE
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="← Предыдущие", callback_data=f"{country_callback_prefix}{country_id}:p{page - 1}"))
    if page < max_page:
        nav.append(InlineKeyboardButton(text="Следующие →", callback_data=f"{country_callback_prefix}{country_id}:p{page + 1}"))
    if nav:
        builder.row(*nav)


def build_live_city_keyboard(country: str, page: int = 0) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    page, _ = add_city_page_buttons(builder, "livecity", country, page)
    add_city_pagination_buttons(builder, country, page, "livecountry")
    builder.row(InlineKeyboardButton(text="← К странам", callback_data="livechoosecountry"))
    builder.row(InlineKeyboardButton(text="🔍 Найти город", callback_data="livesearchcity"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    return builder.as_markup()


def live_period_keyboard(city: str, country: Optional[str] = None) -> InlineKeyboardMarkup:
    cid = get_location_id(city, country)
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📅 Сегодня", callback_data=f"livetoday{cid}"),
        InlineKeyboardButton(text="📋 Вся неделя", callback_data=f"liveweek{cid}"),
    )
    builder.row(InlineKeyboardButton(text="📆 Выбрать день", callback_data=f"livechooseday{cid}"))
    builder.row(InlineKeyboardButton(text="🔔 Подписки по городу", callback_data=f"sublivecity{cid}"))
    builder.row(InlineKeyboardButton(text="← К городам", callback_data=f"livecountry{COUNTRY_TO_ID[country]}" if country in COUNTRY_TO_ID else "livechoosecountry"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    return builder.as_markup()


def get_days_keyboard(prefix: str, back_callback: str, back_text: str = "← Назад") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for i, day_name in enumerate(DAYS):
        builder.button(text=day_name, callback_data=f"{prefix}{i}")
    builder.adjust(2)
    builder.row(InlineKeyboardButton(text=back_text, callback_data=back_callback))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    return builder.as_markup()


def build_subscriptions_menu() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="⭐ Список моих групп", callback_data="mainmygroups"))
    builder.row(InlineKeyboardButton(text="⚙️ Настройки уведомлений", callback_data="settingsroot"))
    builder.row(
        InlineKeyboardButton(text="🌐 Онлайн-подписки", callback_data="subonline"),
        InlineKeyboardButton(text="🏙 Живые подписки", callback_data="sublive"),
    )
    builder.row(InlineKeyboardButton(text="🔕 Отписаться от всего", callback_data="mainunsubscribe"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    return builder.as_markup()


def build_settings_root_menu() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🌐 Онлайн", callback_data="subsettingsonline"),
        InlineKeyboardButton(text="🏙 Живые", callback_data="subsettingslive"),
    )
    builder.row(InlineKeyboardButton(text="← К подпискам", callback_data="submainback"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    return builder.as_markup()


def format_remind_label(remind_before: Iterable[int]) -> str:
    remind_set = set(remind_before)
    if remind_set == {60, 120}:
        return "за 1 и 2 часа"
    if remind_set == {120}:
        return "за 2 часа"
    return "за 1 час"


def render_my_groups_text(user_data: dict) -> str:
    lines = ["<b>Мои группы</b>"]
    if user_data.get("all_online"):
        lines.append("\n🌐 Все онлайн")
    if user_data.get("all_live"):
        city_text = f" ({escape_html(user_data['city'])})" if user_data.get("city") else ""
        lines.append(f"\n🏙 Все живые{city_text}")
    groups = user_data.get("groups", {})
    if groups:
        lines.append("")
        for name, payload in sorted(groups.items(), key=lambda x: x[0].lower()):
            emoji = "🌐" if payload.get("type") == "online" else "🏙"
            lines.append(f"{emoji} {escape_html(name)}")
    if len(lines) == 1:
        lines.append("\nПодписок пока нет.")
    return "\n".join(lines)


async def safe_callback_answer(callback: CallbackQuery, text: str = ""):
    try:
        await callback.answer(text)
    except Exception:
        pass


def markup_signature(markup: Optional[InlineKeyboardMarkup]) -> str:
    if markup is None:
        return ""
    try:
        rows = [[(btn.text, btn.callback_data, btn.url) for btn in row] for row in markup.inline_keyboard]
        return json.dumps(rows, ensure_ascii=False, sort_keys=True)
    except Exception:
        return repr(markup)


async def safe_edit_text(message: Message, text: str, **kwargs):
    if message is None:
        return
    try:
        current_text = message.html_text or message.text or ""
    except Exception:
        current_text = ""
    current_markup = markup_signature(getattr(message, "reply_markup", None))
    new_markup = markup_signature(kwargs.get("reply_markup"))
    if current_text == text and current_markup == new_markup:
        return
    try:
        await message.edit_text(text, **kwargs)
    except TelegramBadRequest as e:
        err = str(e).lower()
        if "message is not modified" in err:
            return
        if any(fragment in err for fragment in [
            "there is no text in the message to edit",
            "message can't be edited",
            "message to edit not found",
        ]):
            await message.answer(text, **kwargs)
            return
        raise


async def send_or_edit(target: CallbackQuery | Message, text: str, **kwargs):
    if isinstance(target, CallbackQuery):
        await safe_edit_text(target.message, text, **kwargs)
        await safe_callback_answer(target)
    else:
        await target.answer(text, **kwargs)


async def send_long_text(target: CallbackQuery | Message, header_text: Optional[str], full_text: str, final_markup: Optional[InlineKeyboardMarkup] = None, **kwargs):
    parts = split_long_message(full_text)
    if isinstance(target, CallbackQuery):
        if header_text:
            await safe_edit_text(target.message, header_text, **kwargs)
        else:
            await safe_edit_text(target.message, parts[0], reply_markup=final_markup if len(parts) == 1 else None, **kwargs)
            parts = parts[1:]
        for idx, part in enumerate(parts):
            markup = final_markup if idx == len(parts) - 1 else None
            await target.message.answer(part, reply_markup=markup, **kwargs)
        await safe_callback_answer(target)
    else:
        if header_text:
            await target.answer(header_text, **kwargs)
        for idx, part in enumerate(parts):
            markup = final_markup if idx == len(parts) - 1 else None
            await target.answer(part, reply_markup=markup, **kwargs)


def get_today_online_subscriptions(user_data: dict):
    return [(t, n, u) for t, n, u in get_online_by_day(moscow_now().weekday()) if is_user_subscribed_to_online(user_data, n)]


def get_today_live_subscriptions(user_data: dict):
    city = user_data.get("city")
    if not city:
        return []
    return [item for item in get_live_groups_for_day(city, moscow_now().weekday(), user_data.get("country")) if is_user_subscribed_to_live(user_data, item[0])]


def build_daily_message(user_data: dict) -> Optional[str]:
    day_index = moscow_now().weekday()
    online_groups = get_today_online_subscriptions(user_data)
    live_groups = get_today_live_subscriptions(user_data)
    if not online_groups and not live_groups:
        return None
    parts = [f"☀ Доброе утро. Вот ваши группы на сегодня, <b>{DAYS[day_index]}</b>:"]
    if online_groups:
        parts.append("\n🌐 <b>Онлайн</b>")
        parts.extend(format_online_group(t, n, u) for t, n, u in online_groups)
    if live_groups:
        parts.append("\n🏙 <b>Живые</b>")
        parts.extend(format_live_group(n, a, s, e, w) for n, a, s, e, w in live_groups)
    return "\n".join(parts)


def build_reminder_key(group_type: str, group_name: str, date_str: str, time_str: str, minutes_before: int) -> str:
    return f"{group_type}|{group_name}|{date_str}|{time_str}|{minutes_before}"


def build_online_single_reminder(name: str, url: str, time_str: str, minutes_before: int) -> str:
    before_text = "через час" if minutes_before == 60 else "через два часа"
    return (
        f"Напоминание: {before_text} начнётся онлайн-группа.\n\n"
        f"🌐 <b>{escape_html(name)}</b>\n"
        f"Начало: <b>{time_str}</b>\n"
        f"Ссылка: <a href=\"{url}\">перейти в группу</a>"
    )


def build_online_multi_reminder(time_str: str, items: List[Tuple[str, str]], minutes_before: int) -> str:
    before_text = "через час" if minutes_before == 60 else "через два часа"
    lines = [f"Напоминание: {before_text} начнутся онлайн-группы.", "", f"Начало: <b>{time_str}</b>", ""]
    lines.extend(f"• <a href=\"{url}\"><b>{escape_html(name)}</b></a>" for name, url in sorted(items, key=lambda x: x[0].lower()))
    return "\n".join(lines)


def build_live_single_reminder(name: str, address: str, start: str, is_work_meeting: bool, minutes_before: int) -> str:
    before_text = "через час" if minutes_before == 60 else "через два часа"
    label = " 🔧" if is_work_meeting else ""
    return (
        f"Напоминание: {before_text} начнётся живая группа.\n\n"
        f"🏙 <b>{escape_html(name)}</b>{label}\n"
        f"Адрес: {escape_html(address)}\n"
        f"Начало: <b>{start}</b>"
    )


def build_live_multi_reminder(start: str, items: List[Tuple[str, str, bool]], minutes_before: int) -> str:
    before_text = "за час" if minutes_before == 60 else "за два часа"
    lines = [f"Напоминание: {before_text} начнутся живые группы.", "", f"Начало: <b>{start}</b>", ""]
    for name, address, is_work_meeting in sorted(items, key=lambda x: x[0].lower()):
        label = " 🔧" if is_work_meeting else ""
        lines.append(f"• <b>{escape_html(name)}</b>{label} — {escape_html(address)}")
    return "\n".join(lines)


def get_online_settings(user_data: dict) -> dict:
    return user_data["online_settings"]


def get_live_settings(user_data: dict) -> dict:
    return user_data["live_settings"]


def collect_due_reminders(user_data: dict, now_dt: datetime):
    due = []
    today_str = now_dt.strftime("%Y-%m-%d")
    now_minutes = now_dt.hour * 60 + now_dt.minute
    reminders_meta = user_data.setdefault("meta", {}).setdefault("last_reminders", {})

    online_due: Dict[Tuple[str, int], List[Tuple[str, str]]] = {}
    for time_str, name, url in get_online_by_day(now_dt.weekday()):
        if not is_user_subscribed_to_online(user_data, name):
            continue
        for minutes_before in get_online_settings(user_data)["remind_before"]:
            if time_to_minutes(time_str) - now_minutes == minutes_before:
                online_due.setdefault((time_str, minutes_before), []).append((name, url))

    for (time_str, minutes_before), items in online_due.items():
        if len(items) == 1:
            name, url = items[0]
            key = build_reminder_key("online", name, today_str, time_str, minutes_before)
            if reminders_meta.get(key) != today_str:
                due.append((key, build_online_single_reminder(name, url, time_str, minutes_before), True))
        else:
            key = build_reminder_key("online_multi", "|".join(sorted(name for name, _ in items)), today_str, time_str, minutes_before)
            if reminders_meta.get(key) != today_str:
                due.append((key, build_online_multi_reminder(time_str, items, minutes_before), True))

    city = user_data.get("city")
    if city:
        live_due: Dict[Tuple[str, int], List[Tuple[str, str, bool]]] = {}
        for name, address, start, _, is_work_meeting in get_live_groups_for_day(city, now_dt.weekday(), user_data.get("country")):
            if not is_user_subscribed_to_live(user_data, name):
                continue
            for minutes_before in get_live_settings(user_data)["remind_before"]:
                if time_to_minutes(start) - now_minutes == minutes_before:
                    live_due.setdefault((start, minutes_before), []).append((name, address, is_work_meeting))

        for (start, minutes_before), items in live_due.items():
            if len(items) == 1:
                name, address, is_work_meeting = items[0]
                key = build_reminder_key("live", name, today_str, start, minutes_before)
                if reminders_meta.get(key) != today_str:
                    due.append((key, build_live_single_reminder(name, address, start, is_work_meeting, minutes_before), False))
            else:
                key = build_reminder_key("live_multi", "|".join(sorted(name for name, _, _ in items)), today_str, start, minutes_before)
                if reminders_meta.get(key) != today_str:
                    due.append((key, build_live_multi_reminder(start, items, minutes_before), False))
    return due


def cleanup_old_reminders(user_data: dict, now_dt: datetime):
    cutoff = (now_dt.date() - timedelta(days=3)).strftime("%Y-%m-%d")
    reminders = user_data.setdefault("meta", {}).setdefault("last_reminders", {})
    for key in [k for k, v in reminders.items() if isinstance(v, str) and v < cutoff]:
        del reminders[key]


async def send_daily_notifications(bot: Bot, now_dt: datetime):
    subs = STORE.load_all()
    changed = False
    today_str = now_dt.strftime("%Y-%m-%d")
    for uid, raw_data in subs.items():
        user_data = normalize_user_sub(raw_data)
        should_send_online = has_online_subscriptions(user_data) and now_dt.hour == get_online_settings(user_data)["daily_hour"]
        should_send_live = has_live_subscriptions(user_data) and now_dt.hour == get_live_settings(user_data)["daily_hour"]
        if not (should_send_online or should_send_live):
            subs[uid] = user_data
            continue
        if user_data["meta"].get("last_daily_sent") == today_str:
            subs[uid] = user_data
            continue
        text = build_daily_message(user_data)
        if not text:
            subs[uid] = user_data
            continue
        try:
            await bot.send_message(int(uid), text, parse_mode=HTML_MODE, disable_web_page_preview=True)
            user_data["meta"]["last_daily_sent"] = today_str
            changed = True
        except Exception as e:
            print(f"daily send failed for {uid}: {e}")
        subs[uid] = user_data
    if changed:
        STORE.save_all(subs)


async def send_hourly_reminders(bot: Bot, now_dt: datetime):
    subs = STORE.load_all()
    changed = False
    for uid, raw_data in subs.items():
        user_data = normalize_user_sub(raw_data)
        cleanup_old_reminders(user_data, now_dt)
        due = collect_due_reminders(user_data, now_dt)
        for key, text, disable_preview in due:
            try:
                await bot.send_message(int(uid), text, parse_mode=HTML_MODE, disable_web_page_preview=disable_preview)
                user_data["meta"]["last_reminders"][key] = now_dt.strftime("%Y-%m-%d")
                changed = True
            except Exception as e:
                print(f"reminder send failed for {uid}: {e}")
        subs[uid] = user_data
    if changed:
        STORE.save_all(subs)


async def notifications_worker(bot: Bot):
    print("notifications worker started")
    while True:
        try:
            now_dt = moscow_now().replace(second=0, microsecond=0)
            await send_daily_notifications(bot, now_dt)
            await send_hourly_reminders(bot, now_dt)
        except Exception as e:
            print(f"notifications worker error: {e}")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


async def show_sub_main(target: CallbackQuery | Message):
    await send_or_edit(target, "🔔 <b>Подписки</b>", parse_mode=HTML_MODE, reply_markup=build_subscriptions_menu())


async def show_sub_online_list(target: CallbackQuery | Message):
    user_data = get_user_sub(str(target.from_user.id))
    builder = InlineKeyboardBuilder()
    all_prefix = "🔔" if user_data.get("all_online") else "🔕"
    builder.row(InlineKeyboardButton(text=f"{all_prefix} Все онлайн", callback_data="subtoggleonlineall"))
    for gid, name in sorted(ONLINE_GROUP_ID_TO_NAME.items(), key=lambda x: x[1].lower()):
        prefix = "🔔" if is_user_subscribed_to_online(user_data, name) else "🔕"
        builder.row(InlineKeyboardButton(text=f"{prefix} {name}", callback_data=f"subtoggleonline{gid}"))
    builder.row(InlineKeyboardButton(text="⚙️ Настройки онлайн", callback_data="subsettingsonline"))
    builder.row(InlineKeyboardButton(text="← К подпискам", callback_data="submainback"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    await send_or_edit(target, "🌐 Онлайн-подписки\n\n🔔 — включено\n🔕 — выключено", parse_mode=HTML_MODE, reply_markup=builder.as_markup())


async def show_sub_live_country_selector(target: CallbackQuery | Message):
    builder = InlineKeyboardBuilder()
    for country in get_countries_with_live_groups():
        builder.button(text=country, callback_data=f"sublivecountry{COUNTRY_TO_ID[country]}")
    builder.adjust(2)
    builder.row(InlineKeyboardButton(text="🔍 Найти город", callback_data="sublivecitysearch"))
    builder.row(InlineKeyboardButton(text="← К подпискам", callback_data="submainback"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    await send_or_edit(target, "🌍 Выберите страну для живых подписок:", reply_markup=builder.as_markup())


async def show_sub_live_city_selector(target: CallbackQuery | Message, country: str, page: int = 0):
    builder = InlineKeyboardBuilder()
    page, total = add_city_page_buttons(builder, "sublivecity", country, page)
    add_city_pagination_buttons(builder, country, page, "sublivecountry")
    builder.row(InlineKeyboardButton(text="← К странам", callback_data="sublivecitychange"))
    builder.row(InlineKeyboardButton(text="🔍 Найти город", callback_data="sublivecitysearch"))
    builder.row(InlineKeyboardButton(text="← К подпискам", callback_data="submainback"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    await send_or_edit(target, city_page_title(country, page, total), parse_mode=HTML_MODE, reply_markup=builder.as_markup())


async def show_sub_live_list(target: CallbackQuery | Message, city: str, country: Optional[str] = None):
    user_data = get_user_sub(str(target.from_user.id))
    builder = InlineKeyboardBuilder()
    all_prefix = "🔔" if user_data.get("all_live") else "🔕"
    builder.row(InlineKeyboardButton(text=f"{all_prefix} Все живые в этом городе", callback_data="subtoggleliveall"))
    group_names = sorted({g["name"] for g in get_live_groups_for_city(city, country)}, key=str.lower)
    if not group_names:
        builder.row(InlineKeyboardButton(text="🏙 Сменить город", callback_data="sublivecitychange"))
        builder.row(InlineKeyboardButton(text="← К подпискам", callback_data="submainback"))
        builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
        await send_or_edit(target, f"🏙 <b>{escape_html(city)}</b>\n\nЖивые группы не найдены.", parse_mode=HTML_MODE, reply_markup=builder.as_markup())
        return
    for name in group_names:
        gid = make_short_id("l", name)
        prefix = "🔔" if is_user_subscribed_to_live(user_data, name) else "🔕"
        builder.row(InlineKeyboardButton(text=f"{prefix} {name}", callback_data=f"subtogglelive{gid}"))
    builder.row(InlineKeyboardButton(text="🏙 Сменить город", callback_data="sublivecitychange"))
    builder.row(InlineKeyboardButton(text="⚙️ Настройки живых", callback_data="subsettingslive"))
    builder.row(InlineKeyboardButton(text="← К подпискам", callback_data="submainback"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    await send_or_edit(target, f"🏙 Живые подписки: <b>{escape_html(city)}</b>\n\n🔔 — включено\n🔕 — выключено", parse_mode=HTML_MODE, reply_markup=builder.as_markup())


async def settings_menu(target: CallbackQuery | Message, group_type: str):
    user_data = get_user_sub(str(target.from_user.id))
    settings = get_online_settings(user_data) if group_type == "online" else get_live_settings(user_data)
    title = "🌐 Настройки онлайн" if group_type == "online" else "🏙 Настройки живых"
    prefix = "online" if group_type == "online" else "live"
    builder = InlineKeyboardBuilder()
    for hour in DAY_HOUR_CHOICES:
        checked = "✅ " if settings["daily_hour"] == hour else ""
        builder.button(text=f"{checked}{hour:02d}:00", callback_data=f"setdailyhour:{prefix}:{hour}")
    builder.adjust(len(DAY_HOUR_CHOICES))
    remind_set = set(settings["remind_before"])
    builder.row(
        InlineKeyboardButton(text="✅ За 1 час" if remind_set == {60} else "За 1 час", callback_data=f"setremind:{prefix}:1"),
        InlineKeyboardButton(text="✅ За 2 часа" if remind_set == {120} else "За 2 часа", callback_data=f"setremind:{prefix}:2"),
        InlineKeyboardButton(text="✅ Оба" if remind_set == {60, 120} else "Оба", callback_data=f"setremind:{prefix}:both"),
    )
    builder.row(InlineKeyboardButton(text="← К подпискам", callback_data="submainback"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    text = (
        f"<b>{title}</b>\n\n"
        f"🕖 Утреннее расписание: <b>{settings['daily_hour']:02d}:00</b>\n"
        f"⏰ Напоминания: <b>{format_remind_label(settings['remind_before'])}</b>"
    )
    await send_or_edit(target, text, parse_mode=HTML_MODE, reply_markup=builder.as_markup())


DP = Dispatcher(storage=MemoryStorage())


@DP.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer("Выберите раздел ниже.", reply_markup=REPLY_MAIN_MENU)
    await message.answer("🏠 <b>Главное меню</b>\n\n🌐 Онлайн-встречи — Telegram / Zoom / MAX\n🏙 Живые встречи — очно в выбранном городе", parse_mode=HTML_MODE, reply_markup=build_main_menu_keyboard())


@DP.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        HELP_TEXT,
        parse_mode=HTML_MODE,
        reply_markup=back_markup("⬅️ Главное меню", "mainmenu"),
    )


@DP.message(Command("slogan"))
async def cmd_slogan(message: Message):
    await message.answer(
        f"<b>Фраза поддержки</b>\n<i>{escape_html(random.choice(SLOGANS_AND_AFFIRMATIONS))}</i>",
        parse_mode=HTML_MODE,
        reply_markup=back_markup("⬅️ Главное меню", "mainmenu"),
    )


@DP.callback_query(F.data == "mainmenu")
async def main_menu_callback(callback: CallbackQuery):
    await send_or_edit(callback, "🏠 <b>Главное меню</b>\n\n🌐 Онлайн-встречи — Telegram / Zoom / MAX\n🏙 Живые встречи — очно в выбранном городе", parse_mode=HTML_MODE, reply_markup=build_main_menu_keyboard())


@DP.callback_query(F.data == "mainonline")
async def main_online_callback(callback: CallbackQuery):
    await send_or_edit(callback, "🌐 <b>Онлайн-встречи</b>\n\nПроходят в Telegram / Zoom / MAX.", parse_mode=HTML_MODE, reply_markup=build_online_menu_keyboard())


@DP.callback_query(F.data == "mainlive")
async def main_live_callback(callback: CallbackQuery):
    await send_or_edit(callback, "🏙 <b>Живые встречи</b>\n\nПроходят очно в выбранном городе.", parse_mode=HTML_MODE, reply_markup=build_live_root_keyboard(get_user_sub(str(callback.from_user.id))))


@DP.callback_query(F.data == "searchgroup")
async def search_group_callback(callback: CallbackQuery, state: FSMContext):
    await state.set_state(GroupNameSearch.waiting_for_name)
    await send_or_edit(callback, "Введите часть названия группы:", reply_markup=back_markup("⬅️ Главное меню", "mainmenu"))


@DP.message(StateFilter(GroupNameSearch.waiting_for_name))
async def search_group_input(message: Message, state: FSMContext):
    await state.clear()
    query = (message.text or "").strip()
    if len(query) < 2:
        await message.answer("Введите минимум 2 символа.", reply_markup=back_markup("⬅️ Главное меню", "mainmenu"))
        return
    await message.answer(
        format_search_results(query),
        parse_mode=HTML_MODE,
        disable_web_page_preview=True,
        reply_markup=back_markup("⬅️ Главное меню", "mainmenu"),
    )


@DP.callback_query(F.data == "livechoosecountry")
async def live_choose_country_callback(callback: CallbackQuery):
    await send_or_edit(callback, "🌍 Выберите страну:", parse_mode=HTML_MODE, reply_markup=build_live_country_keyboard())


@DP.callback_query(F.data.startswith("livecountry"))
async def live_choose_city_callback(callback: CallbackQuery):
    country_id, page = parse_country_page(callback.data[len("livecountry"):])
    country = ID_TO_COUNTRY.get(country_id, country_id)
    total = len(get_cities_for_country(country))
    page = clamp_page(page, total)
    await send_or_edit(callback, city_page_title(country, page, total), parse_mode=HTML_MODE, reply_markup=build_live_city_keyboard(country, page))


@DP.callback_query(F.data == "livemycity")
async def live_my_city_callback(callback: CallbackQuery):
    user_data = get_user_sub(str(callback.from_user.id))
    city = user_data.get("city")
    if not city:
        await send_or_edit(callback, "🏙 Город ещё не выбран.", parse_mode=HTML_MODE, reply_markup=build_live_country_keyboard())
        return
    await send_or_edit(callback, f"🏙 <b>{escape_html(get_country_city_label(user_data.get('country'), city))}</b>", parse_mode=HTML_MODE, reply_markup=live_period_keyboard(city, user_data.get("country")))


@DP.callback_query(F.data == "mainmore")
async def main_more_callback(callback: CallbackQuery):
    await send_or_edit(callback, "<b>Ещё</b>", parse_mode=HTML_MODE, reply_markup=build_more_menu_keyboard())


@DP.callback_query(F.data == "mainhelp")
async def main_help_callback(callback: CallbackQuery):
    await send_or_edit(
        callback,
        HELP_TEXT,
        parse_mode=HTML_MODE,
        reply_markup=back_markup("← Ещё", "mainmore"),
    )


@DP.callback_query(F.data == "maincontacts")
async def main_contacts_callback(callback: CallbackQuery):
    await send_or_edit(
        callback,
        CONTACTS_TEXT,
        parse_mode=HTML_MODE,
        disable_web_page_preview=True,
        reply_markup=back_markup("← Ещё", "mainmore"),
    )


@DP.callback_query(F.data == "mainslogan")
async def main_slogan_callback(callback: CallbackQuery):
    await send_or_edit(
        callback,
        f"<b>Фраза поддержки</b>\n<i>{escape_html(random.choice(SLOGANS_AND_AFFIRMATIONS))}</i>",
        parse_mode=HTML_MODE,
        reply_markup=back_markup("← Ещё", "mainmore"),
    )


@DP.callback_query(F.data == "mainsub")
async def main_sub_callback(callback: CallbackQuery):
    await show_sub_main(callback)


@DP.callback_query(F.data == "mainsettings")
async def main_settings_callback(callback: CallbackQuery):
    await send_or_edit(callback, "⚙️ <b>Настройки уведомлений</b>", parse_mode=HTML_MODE, reply_markup=build_settings_root_menu())


@DP.callback_query(F.data == "mainmygroups")
async def main_my_groups_callback(callback: CallbackQuery):
    user_data = get_user_sub(str(callback.from_user.id))
    await send_or_edit(callback, render_my_groups_text(user_data), parse_mode=HTML_MODE, reply_markup=build_subscriptions_menu())


@DP.callback_query(F.data == "mainunsubscribe")
async def main_unsubscribe_callback(callback: CallbackQuery):
    remove_subscriber(str(callback.from_user.id))
    await send_or_edit(callback, "🔕 Вы отписались от всех уведомлений.", reply_markup=build_main_menu_keyboard())


@DP.callback_query(F.data == "submainback")
async def sub_main_back(callback: CallbackQuery):
    await show_sub_main(callback)


@DP.callback_query(F.data == "subonline")
async def sub_online_list(callback: CallbackQuery):
    await show_sub_online_list(callback)


@DP.callback_query(F.data == "sublive")
async def sub_live_start(callback: CallbackQuery):
    user_data = get_user_sub(str(callback.from_user.id))
    city = user_data.get("city")
    country = user_data.get("country")
    if city:
        await show_sub_live_list(callback, city, country)
    else:
        await show_sub_live_country_selector(callback)


@DP.callback_query(F.data == "sublivecitychange")
async def sub_live_city_change(callback: CallbackQuery):
    await show_sub_live_country_selector(callback)


@DP.callback_query(F.data.startswith("sublivecountry"))
async def sub_live_country_selected(callback: CallbackQuery):
    country_id, page = parse_country_page(callback.data[len("sublivecountry"):])
    country = ID_TO_COUNTRY.get(country_id, country_id)
    await show_sub_live_city_selector(callback, country, page)


@DP.callback_query(F.data == "sublivecitysearch")
async def sub_live_city_search(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SubCitySearch.waiting_for_city)
    await send_or_edit(callback, "Введите город для живых подписок:", reply_markup=back_markup("← К подпискам", "submainback"))


@DP.message(StateFilter(SubCitySearch.waiting_for_city))
async def sub_live_city_input(message: Message, state: FSMContext):
    await state.clear()
    matched = get_searchable_cities((message.text or "").strip())
    if not matched:
        await message.answer("Город не найден.", reply_markup=back_markup("← К подпискам", "submainback"))
        return
    country, city = matched[0]
    uid = str(message.from_user.id)
    user_data = get_user_sub(uid)
    user_data["country"] = country
    user_data["city"] = city
    set_user_sub(uid, user_data)
    await message.answer(f"Выбран город: <b>{escape_html(get_country_city_label(country, city))}</b>", parse_mode=HTML_MODE)
    await show_sub_live_list(message, city, country)


@DP.callback_query(F.data.startswith("sublivecity"))
async def sub_live_city_selected(callback: CallbackQuery):
    cid = callback.data[len("sublivecity"):]
    if cid == "search":
        return
    country, city = resolve_location_id(cid)
    uid = str(callback.from_user.id)
    user_data = get_user_sub(uid)
    user_data["country"] = country
    user_data["city"] = city
    set_user_sub(uid, user_data)
    await show_sub_live_list(callback, city, country)


@DP.callback_query(F.data == "subtoggleonlineall")
async def sub_toggle_online_all(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    user_data = get_user_sub(uid)
    user_data["all_online"] = not user_data.get("all_online", False)
    if user_data["all_online"]:
        user_data["groups"] = {k: v for k, v in user_data["groups"].items() if v.get("type") != "online"}
    set_user_sub(uid, user_data)
    await safe_callback_answer(callback, "Готово")
    await show_sub_online_list(callback)


@DP.callback_query(F.data.startswith("subtoggleonline"))
async def sub_toggle_online(callback: CallbackQuery):
    gid = callback.data[len("subtoggleonline"):]
    group_name = ONLINE_GROUP_ID_TO_NAME.get(gid)
    if not group_name:
        await safe_callback_answer(callback, "Ошибка ID")
        return
    uid = str(callback.from_user.id)
    user_data = get_user_sub(uid)
    groups = user_data.setdefault("groups", {})

    if group_name in groups and groups[group_name].get("type") == "online":
        del groups[group_name]
    else:
        groups[group_name] = {"type": "online", "info": ONLINE_GROUP_INFO.get(group_name, {})}
        user_data["all_online"] = False
    set_user_sub(uid, user_data)
    await safe_callback_answer(callback, "Готово")
    await show_sub_online_list(callback)


@DP.callback_query(F.data == "subtoggleliveall")
async def sub_toggle_live_all(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    user_data = get_user_sub(uid)
    city = user_data.get("city")
    if not city:
        await safe_callback_answer(callback, "Сначала выберите город")
        await show_sub_live_country_selector(callback)
        return
    user_data["all_live"] = not user_data.get("all_live", False)
    if user_data["all_live"]:
        user_data["groups"] = {k: v for k, v in user_data["groups"].items() if v.get("type") != "live"}
    set_user_sub(uid, user_data)
    await safe_callback_answer(callback, "Готово")
    await show_sub_live_list(callback, city, user_data.get("country"))


@DP.callback_query(F.data.startswith("subtogglelive"))
async def sub_toggle_live(callback: CallbackQuery):
    gid = callback.data[len("subtogglelive"):]
    group_name = LIVE_GROUP_ID_TO_NAME.get(gid)
    if not group_name:
        await safe_callback_answer(callback, "Ошибка ID")
        return
    uid = str(callback.from_user.id)
    user_data = get_user_sub(uid)
    groups = user_data.setdefault("groups", {})
    if group_name in groups and groups[group_name].get("type") == "live":
        del groups[group_name]
    else:
        groups[group_name] = {"type": "live", "info": LIVE_GROUP_INFO.get(group_name, {})}
        user_data["all_live"] = False
    set_user_sub(uid, user_data)
    await safe_callback_answer(callback, "Готово")
    city = user_data.get("city")
    if city:
        await show_sub_live_list(callback, city, user_data.get("country"))
    else:
        await show_sub_live_country_selector(callback)


@DP.callback_query(F.data == "settingsroot")
async def settings_root_callback(callback: CallbackQuery):
    await send_or_edit(callback, "⚙️ <b>Настройки уведомлений</b>", parse_mode=HTML_MODE, reply_markup=build_settings_root_menu())


@DP.callback_query(F.data == "subsettingsonline")
async def sub_settings_online(callback: CallbackQuery):
    await settings_menu(callback, "online")


@DP.callback_query(F.data == "subsettingslive")
async def sub_settings_live(callback: CallbackQuery):
    await settings_menu(callback, "live")


@DP.callback_query(F.data.startswith("setdailyhour:"))
async def set_daily_hour(callback: CallbackQuery):
    _, group_type, hour = callback.data.split(":")
    uid = str(callback.from_user.id)
    user_data = get_user_sub(uid)
    user_data[f"{group_type}_settings"]["daily_hour"] = int(hour)
    set_user_sub(uid, user_data)
    await settings_menu(callback, group_type)


@DP.callback_query(F.data.startswith("setremind:"))
async def set_remind(callback: CallbackQuery):
    _, group_type, option = callback.data.split(":")
    mapping = {"1": [60], "2": [120], "both": [60, 120]}
    uid = str(callback.from_user.id)
    user_data = get_user_sub(uid)
    user_data[f"{group_type}_settings"]["remind_before"] = mapping.get(option, [60])
    set_user_sub(uid, user_data)
    await settings_menu(callback, group_type)


@DP.callback_query(F.data == "onlinetoday")
async def online_today(callback: CallbackQuery):
    user_data = get_user_sub(str(callback.from_user.id))
    day_index = moscow_now().weekday()
    groups = get_online_by_day(day_index)
    text = f"{format_day_header(DAYS[day_index])}\n\n"
    text += "\n".join(format_online_group_with_sub(t, n, u, user_data) for t, n, u in groups) if groups else "Нет групп."
    await send_or_edit(callback, text, parse_mode=HTML_MODE, disable_web_page_preview=True, reply_markup=back_markup("← К онлайн", "mainonline"))


@DP.callback_query(F.data == "onlinefull")
async def online_full(callback: CallbackQuery):
    await send_long_text(
        callback,
        "📋 Онлайн на всю неделю:",
        get_online_full(),
        final_markup=back_markup("← К онлайн", "mainonline"),
        parse_mode=HTML_MODE,
        disable_web_page_preview=True,
    )


@DP.callback_query(F.data == "onlinechooseday")
async def online_choose_day(callback: CallbackQuery):
    await send_or_edit(callback, "📆 Выберите день:", reply_markup=get_days_keyboard("onlineday", "mainonline", "← К онлайн"))


@DP.callback_query(F.data.startswith("onlineday"))
async def online_show_day(callback: CallbackQuery):
    day_index = int(callback.data[len("onlineday"):])
    user_data = get_user_sub(str(callback.from_user.id))
    groups = get_online_by_day(day_index)
    text = f"{format_day_header(DAYS[day_index])}\n\n"
    text += "\n".join(format_online_group_with_sub(t, n, u, user_data) for t, n, u in groups) if groups else "Нет групп."
    await send_or_edit(callback, text, parse_mode=HTML_MODE, disable_web_page_preview=True, reply_markup=back_markup("← К дням", "onlinechooseday"))


@DP.callback_query(F.data == "modelive")
async def back_to_live(callback: CallbackQuery):
    await send_or_edit(callback, "🏙 <b>Живые встречи</b>\n\nПроходят очно в выбранном городе.", parse_mode=HTML_MODE, reply_markup=build_live_root_keyboard(get_user_sub(str(callback.from_user.id))))


@DP.callback_query(F.data == "livesearchcity")
async def live_search_city_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(LiveGroupSearch.waiting_for_city)
    await send_or_edit(callback, "🔍 Введите город:", reply_markup=back_markup("← К городам", "modelive"))


@DP.message(StateFilter(LiveGroupSearch.waiting_for_city))
async def live_search_city_handle(message: Message, state: FSMContext):
    await state.clear()
    matched = get_searchable_cities((message.text or "").strip())
    if not matched:
        await message.answer("Город не найден.", reply_markup=back_markup("← К странам", "livechoosecountry"))
        return
    if len(matched) == 1:
        country, city = matched[0]
        await message.answer(f"🏙 <b>{escape_html(get_country_city_label(country, city))}</b>", parse_mode=HTML_MODE, reply_markup=live_period_keyboard(city, country))
        return
    builder = InlineKeyboardBuilder()
    for country, city in matched[:20]:
        builder.button(text=get_country_city_label(country, city), callback_data=f"livecity{get_location_id(city, country)}")
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text="← К странам", callback_data="livechoosecountry"))
    await message.answer("🔍 Уточните город:", reply_markup=builder.as_markup())


@DP.callback_query(F.data.startswith("livecity"))
async def process_city(callback: CallbackQuery):
    cid = callback.data[len("livecity"):]
    country, city = resolve_location_id(cid)
    await send_or_edit(callback, f"🏙 <b>{escape_html(get_country_city_label(country, city))}</b>", parse_mode=HTML_MODE, reply_markup=live_period_keyboard(city, country))


@DP.callback_query(F.data.startswith("livetoday"))
async def live_today(callback: CallbackQuery):
    cid = callback.data[len("livetoday"):]
    country, city = resolve_location_id(cid)
    user_data = get_user_sub(str(callback.from_user.id))
    day_index = moscow_now().weekday()
    groups = get_live_groups_for_day(city, day_index, country)
    text = f"🏙 <b>{escape_html(get_country_city_label(country, city))}</b>\n\n{format_day_header(DAYS[day_index])}\n\n"
    text += "\n".join(format_live_group_with_sub(n, a, s, e, w, user_data) for n, a, s, e, w in groups) if groups else "Нет групп."
    await send_or_edit(callback, text, parse_mode=HTML_MODE, reply_markup=back_markup("← К городу", f"liveperiod{cid}"))


@DP.callback_query(F.data.startswith("liveweek"))
async def live_week(callback: CallbackQuery):
    cid = callback.data[len("liveweek"):]
    country, city = resolve_location_id(cid)
    await send_long_text(
        callback,
        "📋 Живые группы на неделю:",
        get_live_week(city, country),
        final_markup=back_markup("← К городу", f"liveperiod{cid}"),
        parse_mode=HTML_MODE,
    )


@DP.callback_query(F.data.startswith("liveperiod"))
async def live_period(callback: CallbackQuery):
    cid = callback.data[len("liveperiod"):]
    country, city = resolve_location_id(cid)
    await send_or_edit(callback, f"🏙 <b>{escape_html(get_country_city_label(country, city))}</b>", parse_mode=HTML_MODE, reply_markup=live_period_keyboard(city, country))


@DP.callback_query(F.data.startswith("livechooseday"))
async def live_choose_day(callback: CallbackQuery):
    cid = callback.data[len("livechooseday"):]
    country, city = resolve_location_id(cid)
    await send_or_edit(
        callback,
        f"📆 <b>{escape_html(get_country_city_label(country, city))}</b>\n\nВыберите день:",
        parse_mode=HTML_MODE,
        reply_markup=get_days_keyboard(f"liveday{cid}_", f"liveperiod{cid}", "← К городу"),
    )


@DP.callback_query(F.data.startswith("liveday"))
async def live_show_day(callback: CallbackQuery):
    payload = callback.data[len("liveday"):]
    cid, day_index_str = payload.rsplit("_", 1)
    country, city = resolve_location_id(cid)
    day_index = int(day_index_str)
    user_data = get_user_sub(str(callback.from_user.id))
    groups = get_live_groups_for_day(city, day_index, country)
    text = f"🏙 <b>{escape_html(get_country_city_label(country, city))}</b>\n\n{format_day_header(DAYS[day_index])}\n\n"
    text += "\n".join(format_live_group_with_sub(n, a, s, e, w, user_data) for n, a, s, e, w in groups) if groups else "Нет групп."
    await send_or_edit(callback, text, parse_mode=HTML_MODE, reply_markup=back_markup("← К дням", f"livechooseday{cid}"))


@DP.message(F.text == "🌐 Онлайн")
@DP.message(F.text == "🌐 Онлайн-встречи")
async def btn_online(message: Message):
    await message.answer("🌐 <b>Онлайн-встречи</b>\n\nПроходят в Telegram / Zoom / MAX.", parse_mode=HTML_MODE, reply_markup=build_online_menu_keyboard())


@DP.message(F.text == "🏙 Живые")
@DP.message(F.text == "🏙 Живые встречи")
async def btn_live(message: Message):
    await message.answer("🏙 <b>Живые встречи</b>\n\nПроходят очно в выбранном городе.", parse_mode=HTML_MODE, reply_markup=build_live_root_keyboard(get_user_sub(str(message.from_user.id))))


@DP.message(F.text == "🔔 Подписки")
@DP.message(F.text == "🔔 Мои подписки")
async def btn_subscriptions(message: Message):
    await show_sub_main(message)


@DP.message(F.text == "⭐ Мои группы")
async def btn_my_groups(message: Message):
    await message.answer(
        render_my_groups_text(get_user_sub(str(message.from_user.id))),
        parse_mode=HTML_MODE,
        reply_markup=build_subscriptions_menu(),
    )


@DP.message(F.text == "⚙️ Настройки")
async def btn_settings(message: Message):
    await message.answer("⚙️ <b>Настройки уведомлений</b>", parse_mode=HTML_MODE, reply_markup=build_settings_root_menu())


@DP.message(F.text == "Ещё")
async def btn_more(message: Message):
    await message.answer("<b>Ещё</b>", parse_mode=HTML_MODE, reply_markup=build_more_menu_keyboard())


@DP.message(F.text == "Помощь")
async def btn_help(message: Message):
    await message.answer(
        HELP_TEXT,
        parse_mode=HTML_MODE,
        reply_markup=back_markup("← Ещё", "mainmore"),
    )


@DP.message(F.text == "💫 Фраза поддержки")
async def btn_slogan(message: Message):
    await message.answer(
        f"<b>Фраза поддержки</b>\n<i>{escape_html(random.choice(SLOGANS_AND_AFFIRMATIONS))}</i>",
        parse_mode=HTML_MODE,
        reply_markup=back_markup("← Ещё", "mainmore"),
    )


@DP.message(F.text == "Контакты")
async def btn_contacts(message: Message):
    await message.answer(
        CONTACTS_TEXT,
        parse_mode=HTML_MODE,
        disable_web_page_preview=True,
        reply_markup=back_markup("← Ещё", "mainmore"),
    )


@DP.message(F.text == "❌ Отписаться от всего")
async def btn_unsubscribe_all(message: Message):
    remove_subscriber(str(message.from_user.id))
    await message.answer("🔕 Вы отписались от всех уведомлений.", reply_markup=build_main_menu_keyboard())


@DP.message(F.text == "🔍 Найти группу")
async def btn_search_group(message: Message, state: FSMContext):
    await state.set_state(GroupNameSearch.waiting_for_name)
    await message.answer("Введите часть названия группы:", reply_markup=back_markup("⬅️ Главное меню", "mainmenu"))


@DP.message()
async def fallback(message: Message, state: FSMContext):
    if await state.get_state() is None:
        await message.answer("Используйте кнопки меню 👇", reply_markup=REPLY_MAIN_MENU)


async def main():
    if not BOT_TOKEN:
        print("BOT_TOKEN/BOTTOKEN not set")
        return
    bot = Bot(token=BOT_TOKEN)
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(notifications_worker(bot))
    print("bot started")
    await DP.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
