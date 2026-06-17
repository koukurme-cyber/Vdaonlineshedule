import asyncio
import csv
import hashlib
import io
import json
import os
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
ONLINE_TIME_NOTE = "Время указано московское."
CITY_PAGE_SIZE = 40
DAILY_SUMMARY_LIMIT = 999

DAYS = [
    "Понедельник",
    "Вторник",
    "Среда",
    "Четверг",
    "Пятница",
    "Суббота",
    "Воскресенье",
]

DAY_SHORT = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


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
        ("19:00", "Вместе (WhatsApp)", "https://chat.whatsapp.com/0CvEyMffhB60ZHQcShdva7"),
        ("19:00", "Точка Опоры (Zoom)", "https://us06web.zoom.us/j/88678026186?pwd=QYiLNtlro6gEZ3f6eVZdwu7CAHbVF3.1"),
        ("19:00", "Светский круг (жен.)", "https://t.me/+Pyarr0R7MSEyMGIy"),
        ("19:00", "ВДА-ВЕРА", "https://t.me/+J2m1MAbQ818zNTFi"),
        ("19:00", "Маяк ВДА", "https://t.me/+1XGQ4SDkR8M0N2Yy"),
        ("19:30", "Эффект бабочки", "https://t.me/+FcaUkHDOuMpkMTI8"),
        ("20:00", "Мужская ВДА", "https://t.me/+ewtjezZaCtM5YTdi"),
        ("20:00", "Доверие (вопросы)", "https://t.me/VDADoverie"),
        ("21:00", "ДЫШИ!", "https://t.me/breathelivebe"),
        ("21:00", "ВДА ВЕЧЕР", "https://t.me/vda_vecher"),
        ("21:00", "Свобода", "https://t.me/vda_svoboda"),
        ("22:00", "Восст. Люб. Род. (Zoom)", "https://us02web.zoom.us/j/86893102645?pwd=d2N1UWFDY3Y5RXBpTUdQcWpDdEZVUT09UT09"),
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
        ("20:00", "Четвёртая черта онлайн", "https://t.me/chetvertayacherta"),
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
        ("19:00", "Рассвет", "https://t.me/+OOw9IMnM5x1hNDJi"),
        ("20:00", "Восст. Люб. Род. (Zoom)", "https://us02web.zoom.us/j/86893102645?pwd=d2N1UWFDY3Y5RXBpTUdQcWpDdEZVUT09UT09"),
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
        ("14:00", "Четвёртая черта онлайн", "https://t.me/chetvertayacherta"),
        ("18:00", "Ежедневник ВДА", "https://t.me/VDAOXOTNIRYAD"),
        ("18:00", "Весна", "https://t.me/vdavesna_2021"),
        ("19:00", "Сила и Надежда (WhatsApp)", "https://chat.whatsapp.com/CUc0VVemIvl7Aoe2cuYCav"),
        ("19:00", "ВДА-ВЕРА", "https://t.me/+J2m1MAbQ818zNTFi"),
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

# БШН: программная группа, основанная на традициях ВДА.
# Воскресное собрание 21:30 — ПЛР; в расписании оставляем общую группу «БШН»,
# чтобы не дробить подписку на отдельное название.
add_online_group([0], "18:00", "БШН", "https://t.me/bshnvda")
add_online_group([1], "18:00", "БШН", "https://t.me/bshnvda")
add_online_group([2], "09:00", "БШН", "https://t.me/bshnvda")
add_online_group([3], "12:00", "БШН", "https://t.me/bshnvda")
add_online_group([4], "10:00", "БШН", "https://t.me/bshnvda")
add_online_group([5], "07:00", "БШН", "https://t.me/bshnvda")
add_online_group([6], "21:30", "БШН", "https://t.me/bshnvda")

# Свобода теперь проводится ежедневно в 09:00 и 21:00.
add_online_group(range(7), "09:00", "Свобода", "https://t.me/vda_svoboda")

add_online_group([2, 4, 5], "13:00", "Начало (MAX)", "https://max.ru/join/K1vR_TmHfgSBKnKR9DT04dX1vO81a3GuyBP3kc0fsio")

# «По шагам Тони А.» теперь также проводится по пятницам в 19:00.
add_online_group([4], "19:00", "По шагам Тони А.", "https://t.me/+ajasg4oH0SU3MjFi")


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
            KeyboardButton(text="🔔 Подписки"),
            KeyboardButton(text="🔎 Найти группу или город"),
        ],
        [
            KeyboardButton(text="📩 Контакты"),
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
        if iv in {15, 60, 120} and iv not in result:
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
    daily_enabled = settings.get("daily_enabled", settings.get("dailyenabled", True))
    remind_enabled = settings.get("remind_enabled", settings.get("remindenabled", True))
    return {
        "daily_enabled": bool(daily_enabled),
        "daily_hour": daily_hour,
        "remind_enabled": bool(remind_enabled),
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

# Runtime guard against duplicate daily-summary checks inside one running process.
# Key format: YYYY-MM-DD|HH
DAILY_CHECKED_SLOTS = set()


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


def normalize_city_for_sort(city: str) -> str:
    normalized = re.sub(r"^г\.?\s+", "", city.strip(), flags=re.IGNORECASE)
    normalized = re.sub(r"\s*\((СПб|Мск)\)\s*$", "", normalized, flags=re.IGNORECASE)
    return normalized.lower()


def city_priority(city: str) -> int:
    normalized = normalize_city_for_sort(city)
    if normalized == "москва":
        return 0
    if normalized == "санкт-петербург":
        return 1
    return 2


def city_sort_key(city: str) -> tuple:
    return (city_priority(city), normalize_city_for_sort(city))


def get_cities_for_country(country: str) -> List[str]:
    return sorted({g["city"] for g in LIVE_GROUPS if g["country"] == country}, key=city_sort_key)


def compact_address_hint(address: str, max_len: int = 42) -> str:
    text = re.sub(r"<[^>]+>", " ", str(address or ""))
    text = re.sub(r"\s+", " ", text).strip(" .;,")
    if not text:
        return "адрес не указан"

    metro = re.search(r"(?:^|[\s,;(])(?:м\.|метро)\s*([А-ЯЁA-Z0-9][^,;.()]{1,32})", text, flags=re.IGNORECASE)
    if metro:
        station = metro.group(1).strip(" .;,-")
        hint = f"м. {station}"
        return hint[:max_len].rstrip(" .,;-")

    street_markers = (
        "ул", "улица", "просп", "пр-т", "проспект", "пер", "переулок",
        "пл", "площад", "шоссе", "наб", "набереж", "бульв", "проезд",
        "линия", "аллея", "тракт", "дорога", "д.", "дом"
    )
    parts = [p.strip(" .;,") for p in re.split(r"[,;]", text) if p.strip(" .;,")]
    for part in parts:
        lower = part.lower()
        if any(marker in lower for marker in street_markers):
            hint = part
            break
    else:
        hint = parts[0] if parts else text

    if len(hint) > max_len:
        hint = hint[:max_len].rsplit(" ", 1)[0] or hint[:max_len]
    return hint.rstrip(" .,;-")


def abbreviate_place_hint(text: str, max_len: int = 22) -> str:
    """Сжимает ориентир для кнопки: название группы важнее топонима."""
    text = re.sub(r"\s+", " ", str(text or "")).strip(" .;,\"'«»")
    if not text:
        return ""

    replacements = [
        (r"\bТехнологический\s+институт\b", "Технол. ин-т"),
        (r"\bТехнологический\s+Институт\b", "Технол. ин-т"),
        (r"\bПроспект\s+Просвещения\b", "Пр. Просв."),
        (r"\bпроспект\s+Просвещения\b", "Пр. Просв."),
        (r"\bПроспект\b", "Пр."),
        (r"\bпроспект\b", "пр."),
        (r"\bулица\b", "ул."),
        (r"\bУлица\b", "ул."),
        (r"\bпереулок\b", "пер."),
        (r"\bПереулок\b", "пер."),
        (r"\bплощадь\b", "пл."),
        (r"\bПлощадь\b", "пл."),
        (r"\bнабережная\b", "наб."),
        (r"\bНабережная\b", "наб."),
        (r"\bшоссе\b", "ш."),
        (r"\bШоссе\b", "ш."),
        (r"\bВасилеостровская\b", "В.О."),
        (r"\bСенная\b", "Сенн."),
        (r"\bСадовая\b", "Сад."),
        (r"\bСпасская\b", "Спасс."),
        (r"\bВладимирская\b", "Владим."),
        (r"\bДостоевская\b", "Дост."),
        (r"\bНовочеркасская\b", "Новоч."),
        (r"\bЧернышевская\b", "Черн."),
    ]
    for pattern, repl in replacements:
        text = re.sub(pattern, repl, text)

    text = re.sub(r"^м\.\s*Проспект\s+Просвещения\b", "м. Пр. Просвещения", text, flags=re.IGNORECASE)
    text = re.sub(r"^м\.\s*", "м. ", text)
    if len(text) > max_len:
        text = text[:max_len].rsplit(" ", 1)[0] or text[:max_len]
        text = text.rstrip(" .;,-") + "…"
    return text


def short_group_name_for_button(name: str, max_len: int = 34) -> str:
    """Короткое название только для кнопок. Полное название остаётся в карточке группы."""
    text = re.sub(r"\s+", " ", str(name or "").strip())
    replacements = {
        'Говори, доверяй, чувствуй': 'Говори, доверяй…',
        'Говори, доверяй, чувствуй”': 'Говори, доверяй…',
        'Говори Доверяй Чувствуй': 'Говори, доверяй…',
        'Тепло («Азария»)': 'Тепло/Азария',
        'Тепло ("Азария")': 'Тепло/Азария',
        'Доверие (Любящий Родитель)': 'Доверие (ЛР)',
        'Восстановление связи с Любящим Родителем': 'Любящий Родитель',
        'Практика применения пособия «Любящий родитель»': 'Практика ЛР',
        'Практика применения пособия Любящий родитель': 'Практика ЛР',
        'Практика применения пособия Любящий Родитель': 'Практика ЛР',
        'Практика применения пособия "Любящий родитель"': 'Практика ЛР',
        'Практика применения пособия «Любящий Родитель»': 'Практика ЛР',
    }
    if text in replacements:
        return replacements[text]

    text = re.sub(r"Восст\.?\s*Люб\.?\s*Род\.?", "Любящий Родитель", text, flags=re.IGNORECASE)
    text = re.sub(r"Любящ(?:ий|им|его|ему)\s+Родител(?:ь|ем|я|ю)", "ЛР", text, flags=re.IGNORECASE)
    text = re.sub(r"любящ(?:ий|им|его|ему)\s+родител(?:ь|ем|я|ю)", "ЛР", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    return trim_button_text(text, max_len=max_len)


def live_subscription_list_button_text(group: dict, bell: str, max_len: int = 48) -> str:
    """Кнопка списка подписок: полное название в приоритете, затем время, затем короткий ориентир."""
    full_name = re.sub(r"\s+", " ", str(group.get("name", "")).strip())
    name = short_group_name_for_button(full_name, max_len=34)
    schedule = compact_live_schedule_for_button(group, max_len=18)
    hint = abbreviate_place_hint(compact_address_hint(group.get("address", ""), max_len=28), max_len=14)

    parts = [f"{bell} {name}"]
    if schedule:
        parts.append(schedule)
    if hint:
        parts.append(hint)
    text = " · ".join(parts)
    if len(text) <= max_len:
        return text

    # Сначала режем ориентир: он наименее важен.
    if hint:
        for hint_len in (14, 12, 10, 8):
            short_hint = abbreviate_place_hint(hint, max_len=hint_len)
            text = " · ".join([p for p in [f"{bell} {name}", schedule, short_hint] if p])
            if len(text) <= max_len:
                return text

    # Потом режем расписание.
    if schedule:
        short_schedule = compact_live_schedule_for_button(group, max_len=16)
        text = " · ".join([p for p in [f"{bell} {name}", short_schedule, hint] if p])
        if len(text) <= max_len:
            return text
        text = " · ".join([p for p in [f"{bell} {name}", short_schedule] if p])
        if len(text) <= max_len:
            return text

    # Название режем только в последнюю очередь.
    return trim_button_text(text, max_len=max_len)


def trim_button_text(text: str, max_len: int = 64) -> str:
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(text) <= max_len:
        return text
    return text[:max_len - 1].rstrip(" .;,-") + "…"


def short_time_for_button(value: str) -> str:
    value = str(value or "").strip()
    return re.sub(r":00$", "", value)


def format_group_start_days_for_button(group: dict, limit: int = 3) -> str:
    """Краткое расписание для кнопок подписок: только дни и время начала, без времени окончания.
    Для экономии места ровные часы выводятся как 18, а не 18:00.
    """
    entries = [
        e for e in group.get("days", [])
        if isinstance(e.get("day"), int) and 0 <= e.get("day") < len(DAY_SHORT)
    ]
    if not entries:
        return "расписание не указано"

    by_start: Dict[Tuple[str, bool], List[int]] = {}
    for entry in entries:
        start = str(entry.get("start", "")).strip()
        if not start:
            continue
        key = (short_time_for_button(start), bool(entry.get("is_work_meeting")))
        by_start.setdefault(key, []).append(entry.get("day"))

    parts = []
    for (start, is_work_meeting), days in sorted(by_start.items(), key=lambda x: (min(x[1]), x[0][0])):
        if limit and len(parts) >= limit:
            parts.append("…")
            break
        day_text = format_day_list(days)
        if not day_text:
            continue
        label = " 🔧" if is_work_meeting else ""
        parts.append(f"{day_text} {start}{label}")
    return "; ".join(parts) if parts else "расписание не указано"


def compact_live_schedule_for_button(group: dict, max_len: int = 28) -> str:
    schedule = format_group_start_days_for_button(group, limit=3)
    schedule = re.sub(r"\s+", " ", str(schedule or "")).strip()
    if not schedule:
        return "расписание не указано"
    if len(schedule) > max_len:
        cut = schedule[:max_len].rsplit(";", 1)[0].strip()
        if not cut or len(cut) < 8:
            cut = schedule[:max_len].rsplit(" ", 1)[0].strip() or schedule[:max_len].strip()
        schedule = cut.rstrip(" .;,-") + "…"
    return schedule


def live_subscription_button_text(group: dict, max_len: int = 64) -> str:
    name = group.get("name", "")
    return trim_button_text(name, max_len=max_len)


def format_live_subscription_hint_line(index: int, group: dict) -> str:
    name = group.get("name", "")
    schedule = compact_live_schedule_for_button(group, max_len=38)
    hint = compact_address_hint(group.get("address", ""), max_len=42)
    return f"{index}. <b>{escape_html(name)}</b> — {escape_html(schedule)} — {escape_html(hint)}"


def format_online_subscription_hint_line(index: int, name: str) -> str:
    schedule = compact_online_schedule_for_button(name, max_len=52)
    return f"{index}. <b>{escape_html(name)}</b> — {escape_html(schedule)}"


def collect_online_occurrences_for_group(name: str) -> List[Tuple[int, str, str]]:
    occurrences: List[Tuple[int, str, str]] = []
    for day_index, day_groups in ONLINE_SCHEDULE.items():
        for time_str, group_name, url in day_groups:
            if group_name == name:
                occurrences.append((day_index, time_str, url))
    return sorted(occurrences, key=lambda x: (x[0], x[1]))


def format_day_list(day_indexes: List[int]) -> str:
    days = sorted(set(d for d in day_indexes if isinstance(d, int) and 0 <= d < len(DAY_SHORT)))
    if not days:
        return ""
    if days == list(range(7)):
        return "ежедневно"
    if days == list(range(6)):
        return "Пн–Сб"
    if days == [0, 1, 2, 3, 4]:
        return "Пн–Пт"

    ranges = []
    start = prev = days[0]
    for day in days[1:]:
        if day == prev + 1:
            prev = day
            continue
        ranges.append((start, prev))
        start = prev = day
    ranges.append((start, prev))

    parts = []
    for a, b in ranges:
        if a == b:
            parts.append(DAY_SHORT[a])
        elif b == a + 1:
            parts.extend([DAY_SHORT[a], DAY_SHORT[b]])
        else:
            parts.append(f"{DAY_SHORT[a]}–{DAY_SHORT[b]}")
    return ", ".join(parts)


def format_online_schedule(name: str, max_len: Optional[int] = None) -> str:
    occurrences = collect_online_occurrences_for_group(name)
    if not occurrences:
        return "расписание не указано"

    # Если по всем дням один и тот же набор времени — показываем компактно:
    # ежедневно 09:00, 21:00
    times_by_day: Dict[int, List[str]] = {}
    for day_index, time_str, _ in occurrences:
        times_by_day.setdefault(day_index, []).append(time_str)
    for day_index in times_by_day:
        times_by_day[day_index] = sorted(set(times_by_day[day_index]))

    if set(times_by_day.keys()) == set(range(7)):
        first_times = times_by_day.get(0, [])
        if first_times and all(times_by_day.get(day) == first_times for day in range(7)):
            text = f"ежедневно {', '.join(first_times)}"
            if max_len and len(text) > max_len:
                text = text[:max_len].rsplit(";", 1)[0].rstrip() or text[:max_len].rstrip()
                text += "…"
            return text

    # В остальных случаях порядок важнее группировки по времени:
    # Пн 18:00; Вт 18:00; Ср 09:00; ...
    # Но подряд идущие дни с одинаковым временем склеиваем:
    # Пн, Вт 18:00
    slots = sorted(
        ((day_index, time_str) for day_index, time_str, _ in occurrences),
        key=lambda x: (x[0], x[1]),
    )

    groups: List[Tuple[List[int], str]] = []
    for day_index, time_str in slots:
        if groups and groups[-1][1] == time_str and day_index == groups[-1][0][-1] + 1:
            groups[-1][0].append(day_index)
        else:
            groups.append(([day_index], time_str))

    parts = []
    for days, time_str in groups:
        day_text = format_day_list(days)
        if day_text:
            parts.append(f"{day_text} {time_str}")

    text = "; ".join(parts) if parts else "расписание не указано"
    if max_len and len(text) > max_len:
        text = text[:max_len].rsplit(";", 1)[0].rstrip() or text[:max_len].rstrip()
        text += "…"
    return text


def compact_online_schedule_for_button(name: str, max_len: int = 42) -> str:
    return format_online_schedule(name, max_len=max_len)


def online_subscription_button_text(name: str, max_len: int = 64) -> str:
    return trim_button_text(name, max_len=max_len)


def format_online_full_schedule(name: str) -> str:
    return format_online_schedule(name)


def format_online_subscription_info(name: str, user_data: dict) -> str:
    occurrences = collect_online_occurrences_for_group(name)
    url = occurrences[0][2] if occurrences else ONLINE_GROUP_INFO.get(name, {}).get("url", "")
    subscribed = is_user_subscribed_to_online(user_data, name)
    status = "включена" if subscribed else "выключена"
    lines = [
        f"🌐 <b>{escape_html(name)}</b>",
        "",
        f"Расписание: {escape_html(format_online_full_schedule(name))}",
        f"Время: {escape_html(ONLINE_TIME_NOTE.lower())}",
    ]
    if url:
        lines.append(f'Ссылка: <a href="{url}">перейти</a>')
    lines.extend(["", f"Подписка: <b>{status}</b>"])
    return "\n".join(lines)


def find_live_group_by_gid_for_user(gid: str, user_data: dict) -> Optional[dict]:
    group_name = LIVE_GROUP_ID_TO_NAME.get(gid)
    if not group_name:
        return None
    city = user_data.get("city")
    country = user_data.get("country")
    if city:
        for group in get_live_groups_for_city(city, country):
            if group.get("name") == group_name:
                return group
    for group in LIVE_GROUPS:
        if group.get("name") == group_name:
            return group
    return None


def format_live_subscription_info(group: dict, user_data: dict) -> str:
    name = group.get("name", "")
    country = group.get("country")
    city = group.get("city", "")
    address = group.get("address", "")
    days = format_group_days_for_search(group, limit=30)
    subscribed = is_user_subscribed_to_live(user_data, name)
    status = "включена" if subscribed else "выключена"
    return (
        f"🏙 <b>{escape_html(name)}</b>\n\n"
        f"Город: <b>{escape_html(get_country_city_label(country, city))}</b>\n"
        f"Адрес: {escape_html(address) if address else 'не указан'}\n"
        f"Расписание: {escape_html(days)}\n\n"
        f"Подписка: <b>{status}</b>"
    )


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


def city_search_terms(query: str) -> List[str]:
    q = normalize_search_query(query)
    if q in {"мск", "msk"}:
        return ["москва", "мск"]
    if q in {"спб", "spb", "питер", "петербург"}:
        return ["санкт петербург", "санкт-петербург", "спб", "питер", "петербург"]
    return [q]


def get_searchable_cities(query: str, country: Optional[str] = None) -> List[Tuple[str, str]]:
    query_terms = [t for t in city_search_terms(query) if t]
    matched, seen = [], set()
    for group in LIVE_GROUPS:
        if country and group["country"] != country:
            continue
        group_country = group["country"]
        city = group["city"]
        city_text = normalize_search_query(city)
        country_text = normalize_search_query(group_country)
        label_text = f"{country_text} {city_text}"
        aliases = []
        city_norm = normalize_city_for_sort(city)
        if city_norm == "москва" or "(мск)" in city.lower():
            aliases.append("мск")
        if city_norm == "санкт-петербург" or "(спб)" in city.lower():
            aliases.extend(["спб", "питер", "петербург"])

        if any(term in city_text or term in label_text or term in aliases for term in query_terms):
            key = (group_country, city)
            if key not in seen:
                matched.append(key)
                seen.add(key)
    return sorted(matched, key=lambda x: (x[0] != "Россия", x[0].lower(), city_sort_key(x[1])))


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
            parts.append(render_live_group_list(sorted(items, key=lambda x: x[2])))
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


def format_group_days_for_search(group: dict, limit: int = 7) -> str:
    entries = [e for e in group.get("days", []) if isinstance(e.get("day"), int) and 0 <= e.get("day") < len(DAY_SHORT)]
    if not entries:
        return "расписание не распознано"

    by_slot: Dict[Tuple[str, str, bool], List[int]] = {}
    for entry in entries:
        key = (entry.get("start", ""), entry.get("end", ""), bool(entry.get("is_work_meeting")))
        by_slot.setdefault(key, []).append(entry.get("day"))

    parts = []
    for (start, end, is_work_meeting), days in sorted(by_slot.items(), key=lambda x: (min(x[1]), x[0][0], x[0][1])):
        if limit and len(parts) >= limit:
            parts.append("…")
            break
        day_text = format_day_list(days)
        if not day_text:
            continue
        label = " 🔧" if is_work_meeting else ""
        if start and end:
            parts.append(f"{day_text} {start}–{end}{label}")
        elif start:
            parts.append(f"{day_text} {start}{label}")
    return "; ".join(parts) if parts else "расписание не распознано"


def format_live_search_line(group: dict, include_city: bool = True, indent: bool = False) -> str:
    prefix = "  •" if indent else "•"
    city_part = ""
    if include_city:
        city_part = f" — {escape_html(get_country_city_label(group.get('country'), group.get('city', '')))}"
    address = escape_html(group.get("address", ""))
    days_text = escape_html(format_group_days_for_search(group))
    if address:
        return f"{prefix} <b>{escape_html(group['name'])}</b>{city_part}; {address}; {days_text}"
    return f"{prefix} <b>{escape_html(group['name'])}</b>{city_part}; {days_text}"


def format_live_group_card_for_city_list(index: int, group: dict) -> str:
    """Readable card for full city search results."""
    name = escape_html(group.get("name", ""))
    schedule = escape_html(format_group_days_for_search(group, limit=30))
    address = escape_html(group.get("address", "") or "адрес не указан")
    return (
        f"<b>{index}. {name}</b>\n"
        f"🕒 {schedule}\n"
        f"📍 {address}"
    )


def format_city_preview_line(index: int, group: dict) -> str:
    """Short line for search result previews inside a city match."""
    name = escape_html(group.get("name", ""))
    schedule = escape_html(format_group_days_for_search(group, limit=3))
    hint = escape_html(compact_address_hint(group.get("address", ""), max_len=46))
    if hint and hint != "адрес не указан":
        return f"  {index}. <b>{name}</b> — {schedule} — {hint}"
    return f"  {index}. <b>{name}</b> — {schedule}"


def plural_ru(number: int, one: str, few: str, many: str) -> str:
    number_abs = abs(int(number))
    if 11 <= number_abs % 100 <= 14:
        return many
    last = number_abs % 10
    if last == 1:
        return one
    if 2 <= last <= 4:
        return few
    return many


def group_count_text(count: int) -> str:
    return f"{count} {plural_ru(count, 'группа', 'группы', 'групп')}"

def is_spb_query(query: str) -> bool:
    q = normalize_search_query(query).replace("ё", "е")
    return q in {"спб", "spb", "питер", "петербург", "санкт петербург", "санкт-петербург"}


def is_spb_main_city(city: str) -> bool:
    return normalize_city_for_sort(city).replace("ё", "е") == "санкт-петербург"


def is_spb_area_city(city: str) -> bool:
    city_norm = normalize_city_for_sort(city).replace("ё", "е")
    spb_area = {
        "пушкин", "петергоф", "колпино", "всеволожск", "выборг", "кириши",
        "пушкин (спб)", "петергоф (спб)", "колпино (спб)",
    }
    return city_norm in spb_area or "(спб)" in city.lower()


def format_city_header_for_search(country: Optional[str], city: str, groups: List[dict]) -> List[str]:
    return [
        f"📍 <b>{escape_html(get_country_city_label(country, city))}</b>",
        f"Групп: <b>{len(groups)}</b>",
        "",
    ]


def format_city_cards_for_search(country: Optional[str], city: str, start_index: int = 1) -> str:
    groups = sorted(get_live_groups_for_city(city, country), key=lambda g: g.get("name", "").lower())
    if not groups:
        return f"В городе «{escape_html(get_country_city_label(country, city))}» живых групп не найдено."
    lines = format_city_header_for_search(country, city, groups)
    lines.extend(
        format_live_group_card_for_city_list(i, group)
        for i, group in enumerate(groups, start=start_index)
    )
    return "\n\n".join(lines)


def format_city_compact_line(country: Optional[str], city: str) -> str:
    groups = get_live_groups_for_city(city, country)
    return f"• <b>{escape_html(get_country_city_label(country, city))}</b> — {escape_html(group_count_text(len(groups)))}"


def split_primary_city_matches(query: str, city_matches: List[Tuple[str, str]]) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    # При запросах «СПб», «Питер», «Санкт-Петербург» показываем только сам Санкт-Петербург.
    # Колпино, Пушкин, Петергоф и другие города с пометкой «(СПб)» не подмешиваем:
    # пользователь сможет найти их отдельным прямым запросом по названию.
    if is_spb_query(query):
        primary = [(country, city) for country, city in city_matches if is_spb_main_city(city)]
        return primary[:1], []
    if len(city_matches) == 1:
        return city_matches, []
    return [], city_matches


def format_search_results(query: str) -> str:
    city_matches = get_searchable_cities(query)
    online_matches = collect_online_group_matches(query)
    live_matches = collect_live_group_matches(query)
    if not city_matches and not online_matches and not live_matches:
        return f"По запросу «{escape_html(query)}» ничего не найдено."

    lines = [f"🔎 <b>Поиск:</b> {escape_html(query)}"]

    if city_matches:
        city_matches = sorted(city_matches, key=lambda x: (x[0] != "Россия", x[0].lower(), city_sort_key(x[1])))
        primary_cities, secondary_cities = split_primary_city_matches(query, city_matches)

        if primary_cities:
            country, city = primary_cities[0]
            lines.append("")
            lines.append(format_city_compact_line(country, city))
            lines.append("Откройте расписание по кнопке ниже.")

        if secondary_cities:
            title = "🏘 <b>Отдельно рядом</b>" if primary_cities else "📍 <b>Найденные города</b>"
            lines.append(f"\n{title}")
            for country, city in secondary_cities[:12]:
                lines.append(format_city_compact_line(country, city))
            if len(secondary_cities) > 12:
                extra_cities = len(secondary_cities) - 12
                lines.append(f"…и ещё {extra_cities} {plural_ru(extra_cities, 'город', 'города', 'городов')}. Уточните запрос.")

    if online_matches:
        lines.append("\n🌐 <b>Онлайн</b>")
        for name, occurrences in online_matches[:20]:
            parts = []
            for day_index, time_str, url in sorted(occurrences, key=lambda x: (x[0], x[1]))[:10]:
                parts.append(f"{DAY_SHORT[day_index]} {time_str}")
            url = occurrences[0][2]
            extra = "…" if len(occurrences) > 10 else ""
            lines.append(f'• <a href="{url}">{escape_html(name)}</a> — {escape_html(", ".join(parts) + extra)}')
        if len(online_matches) > 20:
            extra_online = len(online_matches) - 20
            lines.append(f"…и ещё {extra_online} онлайн-{plural_ru(extra_online, 'группа', 'группы', 'групп')}.")

    if city_matches:
        matched_city_keys = set(city_matches)
        live_matches = [
            group for group in live_matches
            if (group.get("country"), group.get("city")) not in matched_city_keys
        ]

    if live_matches:
        lines.append("\n🏙 <b>Живые группы по названию</b>")
        for idx, group in enumerate(live_matches[:20], start=1):
            lines.append(format_live_group_card_for_city_list(idx, group))
        if len(live_matches) > 20:
            extra_live = len(live_matches) - 20
            lines.append(f"…и ещё {extra_live} {plural_ru(extra_live, 'живая группа', 'живые группы', 'живых групп')}. Уточните запрос.")

    lines.append("\nДоступные действия — кнопками ниже.")
    return "\n".join(lines)

def build_search_results_keyboard(query: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    city_matches = sorted(
        get_searchable_cities(query),
        key=lambda x: (x[0] != "Россия", x[0].lower(), city_sort_key(x[1])),
    )[:12]
    online_matches = collect_online_group_matches(query)
    live_matches = collect_live_group_matches(query)

    if city_matches:
        primary_cities, secondary_cities = split_primary_city_matches(query, city_matches)
        visible_cities = primary_cities + secondary_cities
        for country, city in visible_cities:
            location_id = get_location_id(city, country)
            label = get_country_city_label(country, city)
            builder.row(InlineKeyboardButton(text=f"Расписание: {label}", callback_data=f"searchshowcity{location_id}"))
        if len(visible_cities) == 1:
            country, city = visible_cities[0]
            location_id = get_location_id(city, country)
            builder.row(InlineKeyboardButton(text="Выбрать как мой город", callback_data=f"searchsetcity{location_id}"))

    if online_matches:
        builder.row(InlineKeyboardButton(text="Перейти к онлайн-подпискам", callback_data="subonline"))

    if live_matches and not city_matches:
        first = live_matches[0]
        country = first.get("country")
        city = first.get("city", "")
        location_id = get_location_id(city, country)
        label = get_country_city_label(country, city)
        builder.row(InlineKeyboardButton(text=f"Расписание: {label}", callback_data=f"searchshowcity{location_id}"))

    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    return builder.as_markup()

def build_search_retry_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🔎 Ввести группу или город", callback_data="searchgroup"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    return builder.as_markup()


def format_full_city_search_results(country: Optional[str], city: str) -> str:
    groups = sorted(get_live_groups_for_city(city, country), key=lambda g: g.get("name", "").lower())
    if not groups:
        return f"В городе «{escape_html(get_country_city_label(country, city))}» живых групп не найдено."

    lines = [
        f"📍 <b>{escape_html(get_country_city_label(country, city))}</b>",
        f"Групп: <b>{len(groups)}</b>",
        "",
    ]
    lines.extend(format_live_group_card_for_city_list(i, group) for i, group in enumerate(groups, start=1))
    return "\n\n".join(lines)

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
    return f'• <b>{time_str}</b> — <a href="{url}">{escape_html(name)}</a>'


def format_online_group_with_sub(time_str: str, name: str, url: str, user_data: dict) -> str:
    bell = " 🔔" if is_user_subscribed_to_online(user_data, name) else ""
    return f'• <b>{time_str}</b> — <a href="{url}">{escape_html(name)}</a>{bell}'


def format_live_group(name: str, address: str, start: str, end: str, is_work_meeting: bool = False) -> str:
    label = " 🔧" if is_work_meeting else ""
    return (
        f"🕒 <b>{escape_html(start)}–{escape_html(end)}</b>{label}\n"
        f"<b>{escape_html(name)}</b>\n"
        f"📍 {escape_html(address)}"
    )


def format_live_group_with_sub(name: str, address: str, start: str, end: str, is_work_meeting: bool, user_data: dict) -> str:
    bell = " 🔔" if is_user_subscribed_to_live(user_data, name) else ""
    label = " 🔧" if is_work_meeting else ""
    return (
        f"🕒 <b>{escape_html(start)}–{escape_html(end)}</b>{label}{bell}\n"
        f"<b>{escape_html(name)}</b>\n"
        f"📍 {escape_html(address)}"
    )


def format_live_group_compact(name: str, address: str, start: str, end: str, is_work_meeting: bool = False) -> str:
    label = " 🔧" if is_work_meeting else ""
    return f"• <b>{escape_html(start)}–{escape_html(end)}</b> — {escape_html(name)}{label} — {escape_html(address)}"


def render_live_group_list(items) -> str:
    return "\n\n".join(
        format_live_group(name, address, start, end, is_work_meeting)
        for name, address, start, end, is_work_meeting in items
    )


def format_day_header(day_name: str, date_label: Optional[str] = None) -> str:
    if date_label:
        return f"━━━ <b>{escape_html(day_name)}</b> · <code>{escape_html(date_label)}</code> ━━━"
    return f"━━━ <b>{escape_html(day_name)}</b> ━━━"


def back_markup(text: str, callback_data: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=text, callback_data=callback_data)]])


CONTACTS_TEXT = (
    'По вопросам актуализации информации о группах пишите '
    '<a href="https://t.me/koukurme2022">сюда</a> '
    'либо на <a href="https://adultchildren.ru/">официальный сайт РКО ВДА</a>.'
)


HELP_TEXT = (
    '<b>Помощь</b>\n\n'
    '<b>Главные разделы</b>\n'
    '🌐 Онлайн-встречи — расписание онлайн-групп по дням.\n'
    '🏙 Живые встречи — очные группы выбранного города по дням и на неделю.\n'
    '🔔 Мои подписки — выбранные группы, утреннее расписание и напоминания.\n'
    '🔎 Найти группу или город — поиск по онлайн-группам, живым группам и городам.\n📩 Контакты — куда писать по вопросам актуализации информации о группах.\n\n'
    '<b>Команды</b>\n'
    '/start — открыть главное меню.\n'
    '/help — показать эту справку.\n'
    '\n'
    '<b>Подписки</b>\n'
    'Можно подписаться на все онлайн-группы, все живые группы выбранного города или отдельные группы. '
    'Утренняя сводка и напоминания за 1/2 часа настраиваются отдельно для онлайн- и живых встреч. '
    'Сводку можно отключить, оставив только напоминания перед началом групп.'
)


def build_live_root_keyboard(user_data: Optional[dict] = None) -> InlineKeyboardMarkup:
    user_data = user_data or {}
    builder = InlineKeyboardBuilder()
    if user_data.get("city"):
        builder.row(InlineKeyboardButton(
            text=f"🏙 Мой город: {get_country_city_label(user_data.get('country'), user_data['city'])}",
            callback_data="livemycity",
        ))
    builder.row(InlineKeyboardButton(text="🔎 Найти город или группу", callback_data="searchgroup"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    return builder.as_markup()

def build_online_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📅 Сегодня", callback_data="onlinetoday"),
        InlineKeyboardButton(text="📋 Неделя", callback_data="onlinefull"),
    )
    builder.row(InlineKeyboardButton(text="📆 Выбрать день", callback_data="onlinechooseday"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    return builder.as_markup()

def build_live_country_keyboard(prefix: str = "livecountry", back_callback: str = "modelive") -> InlineKeyboardMarkup:
    # Списки стран и городов убраны: город выбирается только через поиск.
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🔎 Найти город", callback_data="searchgroup"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
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
    return "Список городов убран. Найдите город через поиск."


def add_city_page_buttons(builder: InlineKeyboardBuilder, prefix: str, country: str, page: int) -> Tuple[int, int]:
    # Списки городов больше не строим: город выбирается только через поиск.
    return 0, 0

def add_city_pagination_buttons(builder: InlineKeyboardBuilder, country: str, page: int, country_callback_prefix: str) -> None:
    return None

def build_live_city_keyboard(country: str, page: int = 0) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🔎 Найти город", callback_data="searchgroup"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    return builder.as_markup()


def all_live_locations_page_title(page: int, total_items: int) -> str:
    return "Список городов убран. Найдите город через поиск."


def build_all_live_locations_keyboard(page: int = 0) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🔎 Найти город", callback_data="searchgroup"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    return builder.as_markup()

def build_removed_section_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    return builder.as_markup()

def get_all_live_locations() -> List[Tuple[str, str]]:
    locations = {(g["country"], g["city"]) for g in LIVE_GROUPS}
    return sorted(
        locations,
        key=lambda x: (x[0] != "Россия", city_sort_key(x[1]), x[0].lower()),
    )


def remaining_locations_page_title(page: int, total_items: int) -> str:
    return "Раздел убран. Используйте расписание по дням или неделю."


def add_remaining_location_page_buttons(builder: InlineKeyboardBuilder, page: int) -> Tuple[int, int]:
    return 0, 0

def add_remaining_location_pagination_buttons(builder: InlineKeyboardBuilder, page: int, total_items: int) -> None:
    return None

def build_remaining_live_root_keyboard(user_data: Optional[dict] = None, page: int = 0) -> InlineKeyboardMarkup:
    return build_removed_section_keyboard()

def build_remaining_live_city_keyboard(country: str, page: int = 0) -> InlineKeyboardMarkup:
    return build_removed_section_keyboard()

def live_period_keyboard(city: str, country: Optional[str] = None) -> InlineKeyboardMarkup:
    cid = get_location_id(city, country)
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📅 Сегодня", callback_data=f"livetoday{cid}"),
        InlineKeyboardButton(text="📋 Неделя", callback_data=f"liveweek{cid}"),
    )
    builder.row(InlineKeyboardButton(text="📆 Выбрать день", callback_data=f"livechooseday{cid}"))
    builder.row(InlineKeyboardButton(text="🔎 Найти другой город", callback_data="searchgroup"))
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
    builder.row(InlineKeyboardButton(text="Выбрать онлайн-группы", callback_data="subonline"))
    builder.row(InlineKeyboardButton(text="Выбрать живые группы", callback_data="sublive"))
    builder.row(InlineKeyboardButton(text="Настройки подписок", callback_data="settingsroot"))
    builder.row(InlineKeyboardButton(text="🔕 Отписаться от всего", callback_data="mainunsubscribe"))
    builder.row(InlineKeyboardButton(text="← Главное меню", callback_data="mainmenu"))
    return builder.as_markup()


def build_my_groups_menu() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="Изменить онлайн", callback_data="subonline"),
        InlineKeyboardButton(text="Изменить живые", callback_data="sublive"),
    )
    builder.row(InlineKeyboardButton(text="← К подпискам", callback_data="submainback"))
    builder.row(InlineKeyboardButton(text="← Главное меню", callback_data="mainmenu"))
    return builder.as_markup()


def _remind_option_label(settings: dict, values: List[int], label: str) -> str:
    return f"✅ {label}" if set(settings.get("remind_before", [])) == set(values) else label


def build_settings_root_menu(user_data: Optional[dict] = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🌐 Настроить онлайн", callback_data="subsettingsonline"))
    builder.row(InlineKeyboardButton(text="🏙 Настроить живые", callback_data="subsettingslive"))
    builder.row(InlineKeyboardButton(text="← К подпискам", callback_data="submainback"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    return builder.as_markup()


def format_remind_label(remind_before: Iterable[int]) -> str:
    remind_set = set(remind_before)
    if remind_set == {15}:
        return "за 15 минут"
    if remind_set == {60}:
        return "за 1 час"
    if remind_set == {120}:
        return "за 2 часа"
    if remind_set == {60, 120}:
        return "за 1 и 2 часа"
    if remind_set == {15, 60}:
        return "за 15 минут и 1 час"
    if remind_set == {15, 120}:
        return "за 15 минут и 2 часа"
    if remind_set == {15, 60, 120}:
        return "за 15 минут, 1 и 2 часа"
    return "за 1 час"


def format_settings_line(label: str, settings: dict) -> str:
    daily_enabled = settings.get("daily_enabled", True)
    if daily_enabled:
        daily_text = f"сводка в {int(settings.get('daily_hour', DEFAULT_DAILY_HOUR)):02d}:00"
    else:
        daily_text = "сводка выключена"
    if settings.get("remind_enabled", True):
        remind_text = f"напоминания {format_remind_label(settings.get('remind_before', DEFAULT_REMIND_BEFORE))}"
    else:
        remind_text = "напоминания выключены"
    return f"{label} — {daily_text}, {remind_text}"


def render_my_groups_text(user_data: dict) -> str:
    lines = ["🔔 <b>Мои подписки</b>"]

    online_items = []
    live_items = []
    if user_data.get("all_online"):
        online_items.append("Все онлайн-группы")
    if user_data.get("all_live"):
        city = user_data.get("city")
        country = user_data.get("country")
        city_text = get_country_city_label(country, city) if city else "выбранного города"
        live_items.append(f"Все живые группы: {escape_html(city_text)}")

    for name, payload in sorted(user_data.get("groups", {}).items(), key=lambda x: x[0].lower()):
        if payload.get("type") == "online":
            online_items.append(escape_html(name))
        elif payload.get("type") == "live":
            live_items.append(escape_html(name))

    lines.append("\n🌐 <b>Онлайн</b>")
    if online_items:
        lines.extend(f"• {item}" for item in online_items)
    else:
        lines.append("Нет выбранных онлайн-групп.")

    lines.append("\n🏙 <b>Живые</b>")
    if live_items:
        lines.extend(f"• {item}" for item in live_items)
    else:
        lines.append("Нет выбранных живых групп.")

    lines.append("\n⚙️ <b>Уведомления</b>")
    lines.append(format_settings_line("🌐 Онлайн", get_online_settings(user_data)))
    lines.append(format_settings_line("🏙 Живые", get_live_settings(user_data)))
    return "\n".join(lines)


def _daily_settings_text(settings: dict) -> str:
    if settings.get("daily_enabled", True):
        return f"сводка включена, {int(settings.get('daily_hour', DEFAULT_DAILY_HOUR)):02d}:00"
    return "сводка выключена"


def render_settings_root_text(user_data: dict) -> str:
    online_settings = get_online_settings(user_data)
    live_settings = get_live_settings(user_data)
    return (
        "⚙️ <b>Настройки подписок</b>\n\n"
        "🌐 <b>Онлайн</b>\n"
        f"Утренняя сводка: <b>{_daily_settings_text(online_settings)}</b>\n"
        f"Напоминания: <b>{format_remind_label(online_settings.get('remind_before', DEFAULT_REMIND_BEFORE)) if online_settings.get('remind_enabled', True) else 'выключены'}</b>\n\n"
        "🏙 <b>Живые</b>\n"
        f"Утренняя сводка: <b>{_daily_settings_text(live_settings)}</b>\n"
        f"Напоминания: <b>{format_remind_label(live_settings.get('remind_before', DEFAULT_REMIND_BEFORE)) if live_settings.get('remind_enabled', True) else 'выключены'}</b>"
    )


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


def get_remaining_online_today(now_dt: Optional[datetime] = None):
    now_dt = now_dt or moscow_now()
    now_minutes = now_dt.hour * 60 + now_dt.minute
    return [
        (time_str, name, url)
        for time_str, name, url in get_online_by_day(now_dt.weekday())
        if time_to_minutes(time_str) >= now_minutes
    ]


def get_remaining_live_today(city: str, country: Optional[str] = None, now_dt: Optional[datetime] = None):
    now_dt = now_dt or moscow_now()
    now_minutes = now_dt.hour * 60 + now_dt.minute
    return [
        item
        for item in get_live_groups_for_day(city, now_dt.weekday(), country)
        if time_to_minutes(item[2]) >= now_minutes
    ]


def format_remaining_live_line(name: str, address: str, start: str, end: str, is_work_meeting: bool = False) -> str:
    label = " 🔧" if is_work_meeting else ""
    hint = compact_address_hint(address, max_len=54)
    if hint and hint != "адрес не указан":
        return f"• <b>{escape_html(start)}–{escape_html(end)}</b> — {escape_html(name)}{label} — {escape_html(hint)}"
    return f"• <b>{escape_html(start)}–{escape_html(end)}</b> — {escape_html(name)}{label}"


def build_remaining_today_text(user_data: dict) -> str:
    return "Раздел убран. Используйте расписание по дням или неделю."


def build_remaining_online_text(now_dt: Optional[datetime] = None) -> str:
    return "Раздел убран. Используйте расписание по дням или неделю."


def build_remaining_live_text(city: str, country: Optional[str] = None, now_dt: Optional[datetime] = None) -> str:
    return "Раздел убран. Используйте расписание по дням или неделю."



def get_today_online_subscriptions(user_data: dict):
    return [(t, n, u) for t, n, u in get_online_by_day(moscow_now().weekday()) if is_user_subscribed_to_online(user_data, n)]


def get_today_live_subscriptions(user_data: dict):
    city = user_data.get("city")
    if not city:
        return []
    return [item for item in get_live_groups_for_day(city, moscow_now().weekday(), user_data.get("country")) if is_user_subscribed_to_live(user_data, item[0])]


def _trim_daily_items(items: list, limit: int = DAILY_SUMMARY_LIMIT):
    # Утренняя сводка больше не обрезается: показываем все группы на сегодня.
    return items, 0


def format_live_group_for_daily_summary(name: str, address: str, start: str, end: str, is_work_meeting: bool = False) -> str:
    """Compact two-line live-group block for morning summary."""
    label = " 🔧" if is_work_meeting else ""
    hint = compact_address_hint(address, max_len=54)
    first_line = f"• <b>{escape_html(start)}–{escape_html(end)}</b> — {escape_html(name)}{label}"
    if hint and hint != "адрес не указан":
        return f"{first_line}\n  {escape_html(hint)}"
    return first_line


def build_daily_message(
    user_data: dict,
    include_online: bool = True,
    include_live: bool = True,
    limit_per_section: int = DAILY_SUMMARY_LIMIT,
    test_mode: bool = False,
) -> Optional[str]:
    day_index = moscow_now().weekday()
    online_groups = get_today_online_subscriptions(user_data) if include_online else []
    live_groups = get_today_live_subscriptions(user_data) if include_live else []
    if not online_groups and not live_groups:
        return None

    header = "🧪 Тест утренней сводки" if test_mode else "☀ Доброе утро. Вот ваши группы на сегодня"
    parts = [f"{header}, <b>{DAYS[day_index]}</b>:"]

    if online_groups:
        parts.append("\n🌐 <b>Онлайн</b>")
        parts.extend(format_online_group(t, n, u) for t, n, u in online_groups)

    if live_groups:
        parts.append("\n🏙 <b>Живые</b>")
        parts.extend(
            format_live_group_for_daily_summary(n, a, s, e, w)
            for n, a, s, e, w in live_groups
        )

    return "\n".join(parts)


def build_reminder_key(group_type: str, group_name: str, date_str: str, time_str: str, minutes_before: int) -> str:
    return f"{group_type}|{group_name}|{date_str}|{time_str}|{minutes_before}"


def reminder_before_text(minutes_before: int) -> str:
    if minutes_before == 15:
        return "через 15 минут"
    if minutes_before == 60:
        return "через час"
    if minutes_before == 120:
        return "через два часа"
    return f"через {minutes_before} минут"


def build_online_single_reminder(name: str, url: str, time_str: str, minutes_before: int) -> str:
    before_text = reminder_before_text(minutes_before)
    return (
        f"Напоминание: {before_text} начнётся онлайн-группа.\n\n"
        f"🌐 <b>{escape_html(name)}</b>\n"
        f"Начало: <b>{time_str}</b>\n"
        f"Ссылка: <a href=\"{url}\">перейти в группу</a>"
    )


def build_online_multi_reminder(time_str: str, items: List[Tuple[str, str]], minutes_before: int) -> str:
    before_text = reminder_before_text(minutes_before)
    lines = [f"Напоминание: {before_text} начнутся онлайн-группы.", "", f"Начало: <b>{time_str}</b>", ""]
    lines.extend(f"• <a href=\"{url}\"><b>{escape_html(name)}</b></a>" for name, url in sorted(items, key=lambda x: x[0].lower()))
    return "\n".join(lines)


def build_live_single_reminder(name: str, address: str, start: str, is_work_meeting: bool, minutes_before: int) -> str:
    before_text = reminder_before_text(minutes_before)
    label = " 🔧" if is_work_meeting else ""
    return (
        f"Напоминание: {before_text} начнётся живая группа.\n\n"
        f"🏙 <b>{escape_html(name)}</b>{label}\n"
        f"Адрес: {escape_html(address)}\n"
        f"Начало: <b>{start}</b>"
    )


def build_live_multi_reminder(start: str, items: List[Tuple[str, str, bool]], minutes_before: int) -> str:
    before_text = reminder_before_text(minutes_before)
    lines = [f"Напоминание: {before_text} начнутся живые группы.", "", f"Начало: <b>{start}</b>", ""]
    for name, address, is_work_meeting in sorted(items, key=lambda x: x[0].lower()):
        label = " 🔧" if is_work_meeting else ""
        lines.append(f"• <b>{escape_html(name)}</b>{label} — {escape_html(address)}")
    return "\n".join(lines)


def build_combined_reminder(start: str, online_items: List[Tuple[str, str]], live_items: List[Tuple[str, str, bool]], minutes_before: int) -> str:
    """One reminder for all subscribed groups that start at the same time."""
    before_text = reminder_before_text(minutes_before)
    total = len(online_items) + len(live_items)

    if online_items and live_items:
        header = f"Напоминание: {before_text} начнутся группы."
    elif online_items:
        header = (
            f"Напоминание: {before_text} начнётся онлайн-группа."
            if total == 1
            else f"Напоминание: {before_text} начнутся онлайн-группы."
        )
    else:
        header = (
            f"Напоминание: {before_text} начнётся живая группа."
            if total == 1
            else f"Напоминание: {before_text} начнутся живые группы."
        )

    lines = [header, "", f"Начало: <b>{start}</b>"]

    if online_items:
        lines.extend(["", "🌐 <b>Онлайн</b>"])
        for name, url in sorted(online_items, key=lambda x: x[0].lower()):
            lines.append(f"• <a href=\"{url}\"><b>{escape_html(name)}</b></a>")

    if live_items:
        lines.extend(["", "🏙 <b>Живые</b>"])
        for name, address, is_work_meeting in sorted(live_items, key=lambda x: x[0].lower()):
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

    # Combined by (start time, minutes_before), regardless of group type.
    # This prevents duplicate reminders when online and live groups start at the same time.
    combined_due: Dict[Tuple[str, int], dict] = {}

    if not get_online_settings(user_data).get("remind_enabled", True):
        online_reminders_enabled = False
    else:
        online_reminders_enabled = True

    for time_str, name, url in get_online_by_day(now_dt.weekday()):
        if not online_reminders_enabled or not is_user_subscribed_to_online(user_data, name):
            continue
        for minutes_before in get_online_settings(user_data)["remind_before"]:
            if time_to_minutes(time_str) - now_minutes == minutes_before:
                bucket = combined_due.setdefault((time_str, minutes_before), {"online": [], "live": []})
                bucket["online"].append((name, url))

    city = user_data.get("city")
    if city and get_live_settings(user_data).get("remind_enabled", True):
        for name, address, start, _, is_work_meeting in get_live_groups_for_day(city, now_dt.weekday(), user_data.get("country")):
            if not is_user_subscribed_to_live(user_data, name):
                continue
            for minutes_before in get_live_settings(user_data)["remind_before"]:
                if time_to_minutes(start) - now_minutes == minutes_before:
                    bucket = combined_due.setdefault((start, minutes_before), {"online": [], "live": []})
                    bucket["live"].append((name, address, is_work_meeting))

    for (start, minutes_before), payload in sorted(combined_due.items(), key=lambda x: (x[0][0], x[0][1])):
        online_items = payload.get("online", [])
        live_items = payload.get("live", [])
        names_for_key = [f"o:{name}" for name, _ in online_items] + [f"l:{name}" for name, _, _ in live_items]
        key = build_reminder_key(
            "combined",
            "|".join(sorted(names_for_key)),
            today_str,
            start,
            minutes_before,
        )
        if reminders_meta.get(key) != today_str:
            disable_preview = bool(online_items)
            due.append((key, build_combined_reminder(start, online_items, live_items, minutes_before), disable_preview))

    return due

def cleanup_old_reminders(user_data: dict, now_dt: datetime):
    cutoff = (now_dt.date() - timedelta(days=3)).strftime("%Y-%m-%d")
    reminders = user_data.setdefault("meta", {}).setdefault("last_reminders", {})
    for key in [k for k, v in reminders.items() if isinstance(v, str) and v < cutoff]:
        del reminders[key]


async def send_daily_notifications(bot: Bot, now_dt: datetime):
    subs = STORE.load_all()
    today_str = now_dt.strftime("%Y-%m-%d")
    hour_label = f"{now_dt.hour:02d}:00"

    for uid, raw_data in subs.items():
        user_data = normalize_user_sub(raw_data)
        meta = user_data.setdefault("meta", {})
        online_settings = get_online_settings(user_data)
        live_settings = get_live_settings(user_data)

        legacy_daily_sent = meta.get("last_daily_sent") == today_str
        has_split_daily_marks = bool(meta.get("last_daily_sent_online") or meta.get("last_daily_sent_live"))
        online_already_sent = meta.get("last_daily_sent_online") == today_str or (legacy_daily_sent and not has_split_daily_marks)
        live_already_sent = meta.get("last_daily_sent_live") == today_str or (legacy_daily_sent and not has_split_daily_marks)

        should_send_online = (
            has_online_subscriptions(user_data)
            and online_settings.get("daily_enabled", True)
            and now_dt.hour == online_settings["daily_hour"]
            and not online_already_sent
        )
        should_send_live = (
            has_live_subscriptions(user_data)
            and live_settings.get("daily_enabled", True)
            and now_dt.hour == live_settings["daily_hour"]
            and not live_already_sent
        )

        if not (should_send_online or should_send_live):
            subs[uid] = user_data
            continue

        text = build_daily_message(user_data, include_online=should_send_online, include_live=should_send_live)
        if not text:
            subs[uid] = user_data
            continue

        # Mark before sending and save immediately.
        # This prevents duplicate sends inside the same hour if the worker loops again
        # or if the process restarts after sending but before final save.
        if should_send_online:
            meta["last_daily_sent_online"] = today_str
        if should_send_live:
            meta["last_daily_sent_live"] = today_str
        subs[uid] = user_data
        STORE.save_all(subs)

        try:
            await bot.send_message(int(uid), text, parse_mode=HTML_MODE, disable_web_page_preview=True)
        except Exception as e:
            print(f"daily send failed for {uid}: {e}")

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
            real_now = moscow_now()
            now_dt = real_now.replace(second=0, microsecond=0)

            # Daily summary: only once per process per date+hour, during the first minute.
            # Do not use replace(second=0) for this condition, otherwise a 30-second loop
            # can enter this block twice in the same minute.
            slot_key = f"{real_now.strftime('%Y-%m-%d')}|{real_now.hour:02d}"
            if real_now.minute == 0 and slot_key not in DAILY_CHECKED_SLOTS:
                DAILY_CHECKED_SLOTS.add(slot_key)
                await send_daily_notifications(bot, now_dt)

            await send_hourly_reminders(bot, now_dt)
        except Exception as e:
            print(f"notifications worker error: {e}")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


SUBSCRIPTION_PAGE_SIZE = 6

def clamp_subscription_page(page: int, total_items: int, page_size: int = SUBSCRIPTION_PAGE_SIZE) -> int:
    if total_items <= 0:
        return 0
    max_page = max(0, (total_items - 1) // page_size)
    return max(0, min(int(page or 0), max_page))

def page_slice(items: list, page: int, page_size: int = SUBSCRIPTION_PAGE_SIZE):
    page = clamp_subscription_page(page, len(items), page_size)
    start = page * page_size
    end = start + page_size
    return page, items[start:end], max(1, (len(items) + page_size - 1) // page_size)


def normalize_expanded_id(raw: Optional[str]) -> str:
    raw = str(raw or "").strip()
    if raw in {"", "-", "none", "None", "null"}:
        return ""
    # callback ids are short hashes without colon; reject unexpected payloads
    if ":" in raw or len(raw) > 16:
        return ""
    return raw


async def show_sub_main(target: CallbackQuery | Message):
    user_data = get_user_sub(str(target.from_user.id))
    await send_or_edit(
        target,
        render_my_groups_text(user_data),
        parse_mode=HTML_MODE,
        reply_markup=build_subscriptions_menu(),
    )


def compact_time_only_for_button(time_text: str) -> str:
    """Short start-time formatting for internal use."""
    text = str(time_text or "").strip()
    text = re.sub(r"\b(\d{1,2}):00\b", lambda m: str(int(m.group(1))), text)
    return text


def subscription_action_label(is_subscribed: bool) -> str:
    return "🔔" if is_subscribed else "🔕"


def shorten_group_name_for_button(name: str) -> str:
    text = str(name or "").strip()
    replacements = {
        "Тепло («Азария»)": "Тепло",
        "Тепло (\"Азария\")": "Тепло",
        "Тепло / Азария": "Тепло",
        "Говори, доверяй, чувствуй": "Говори, доверяй…",
        "Практика применения пособия «Любящий родитель»": "Практика ЛР",
        "Практика применения пособия Любящий родитель": "Практика ЛР",
        "Доверие (Любящий Родитель)": "Доверие (ЛР)",
        "Доверие (Любящий родитель)": "Доверие (ЛР)",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    text = re.sub(r"Любящ(ий|его|им|ем|ая|ую)\s+Родител[ьяюем]*", "ЛР", text, flags=re.IGNORECASE)
    return text


def compact_days_only_schedule_for_button(schedule_text: str) -> str:
    """Return only days for a button: 'Вт, Ср, Пт'."""
    text = str(schedule_text or "").strip()
    if not text:
        return ""
    day_order = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    found = []
    patterns = [
        ("Пн", r"\b(Понедельник|Пн\.?)\b"),
        ("Вт", r"\b(Вторник|Вт\.?)\b"),
        ("Ср", r"\b(Среда|Ср\.?)\b"),
        ("Чт", r"\b(Четверг|Чт\.?)\b"),
        ("Пт", r"\b(Пятница|Пт\.?)\b"),
        ("Сб", r"\b(Суббота|Сб\.?)\b"),
        ("Вс", r"\b(Воскресенье|Вс\.?)\b"),
    ]
    for label, pattern in patterns:
        if re.search(pattern, text, flags=re.IGNORECASE):
            found.append(label)
    found = [d for d in day_order if d in found]
    if found == day_order:
        return "ежедневно"
    if found == day_order[:6]:
        return "Пн–Сб"
    if found == day_order[:5]:
        return "Пн–Пт"
    return ", ".join(found)


def live_days_only_for_button(group: dict) -> str:
    entries = [
        e for e in group.get("days", [])
        if isinstance(e.get("day"), int) and 0 <= e.get("day") < len(DAY_SHORT)
    ]
    if not entries:
        return ""
    days = sorted({e["day"] for e in entries})
    return format_day_list(days)


def live_subscription_button_text(group: dict, max_len: int = 58) -> str:
    """Кнопка живой группы: название + дни + короткий ориентир. Без времени."""
    name = shorten_group_name_for_button(group.get("name", ""))
    days = live_days_only_for_button(group)
    hint = abbreviate_place_hint(compact_address_hint(group.get("address", ""), max_len=28), max_len=16)

    parts = [name]
    if days:
        parts.append(days)
    if hint:
        parts.append(hint)

    text = " · ".join(parts)
    if len(text) <= max_len:
        return text

    # Сначала режем ориентир, потому что название и дни важнее.
    if hint:
        for hint_len in (14, 12, 10, 8):
            short_hint = abbreviate_place_hint(hint, max_len=hint_len)
            text = " · ".join([p for p in [name, days, short_hint] if p])
            if len(text) <= max_len:
                return text

    # Если всё ещё длинно — убираем ориентир, но это крайний случай.
    text = " · ".join([p for p in [name, days] if p])
    if len(text) <= max_len:
        return text

    return trim_button_text(text, max_len=max_len)


def online_subscription_button_text(name: str) -> str:
    short_name = shorten_group_name_for_button(name)
    schedule = compact_online_schedule_for_button(name)
    days = compact_days_only_schedule_for_button(schedule)
    if days:
        return f"{short_name} · {days}"
    return short_name


async def show_sub_online_list(target: CallbackQuery | Message, page: int = 0):
    """Короткий список онлайн-групп: название + минимальное расписание. Подробности и подписка — в карточке группы."""
    user_data = get_user_sub(str(target.from_user.id))
    builder = InlineKeyboardBuilder()
    all_prefix = "🔔" if user_data.get("all_online") else "🔕"
    builder.row(InlineKeyboardButton(text=f"{all_prefix} Все онлайн", callback_data="subtoggleonlineall"))

    online_items = sorted(ONLINE_GROUP_ID_TO_NAME.items(), key=lambda x: x[1].lower())
    page, visible_items, total_pages = page_slice(online_items, page)

    lines = [
        "🌐 <b>Онлайн-группы</b>",
        "",
        escape_html(ONLINE_TIME_NOTE),
        "Выберите группу, чтобы посмотреть подробности и настроить подписку.",
        "",
    ]
    if total_pages > 1:
        lines.append(f"Страница {page + 1} из {total_pages}")
        lines.append("")

    for gid, name in visible_items:
        subscribed = is_user_subscribed_to_online(user_data, name)
        bell = "🔔" if subscribed else "🔕"
        schedule = compact_online_schedule_for_button(name, max_len=34)
        button_name = short_group_name_for_button(name, max_len=34)
        button_text = trim_button_text(f"{bell} {button_name} · {schedule}", max_len=60)
        builder.row(InlineKeyboardButton(text=button_text, callback_data=f"subonlineinfo:{page}:{gid}"))

    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="← Предыдущие", callback_data=f"subonlinepage:{page - 1}"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="Следующие →", callback_data=f"subonlinepage:{page + 1}"))
        if nav:
            builder.row(*nav)

    builder.row(InlineKeyboardButton(text="← К подпискам", callback_data="submainback"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    await send_or_edit(
        target,
        "\n".join(lines).strip(),
        parse_mode=HTML_MODE,
        reply_markup=builder.as_markup(),
        disable_web_page_preview=True,
    )


def online_subscription_card_text(name: str, user_data: dict) -> str:
    status = "включена" if is_user_subscribed_to_online(user_data, name) else "выключена"
    lines = [
        f"🌐 <b>{escape_html(name)}</b>",
        "",
        f"🕒 {escape_html(format_online_full_schedule(name))}",
        f"Время: {escape_html(ONLINE_TIME_NOTE.lower())}",
    ]
    occurrences = collect_online_occurrences_for_group(name)
    url = occurrences[0][2] if occurrences else ONLINE_GROUP_INFO.get(name, {}).get("url", "")
    if url:
        lines.append(f'Ссылка: <a href="{url}">перейти</a>')
    lines.extend(["", f"Подписка: <b>{status}</b>"])
    return "\n".join(lines)


def online_subscription_card_keyboard(gid: str, page: int, user_data: dict) -> InlineKeyboardMarkup:
    name = ONLINE_GROUP_ID_TO_NAME.get(gid, "")
    sub_text = "Отключить подписку" if is_user_subscribed_to_online(user_data, name) else "Включить подписку"
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=sub_text, callback_data=f"subonlineinfotoggle:{page}:{gid}"))
    builder.row(InlineKeyboardButton(text="← К онлайн-группам", callback_data=f"subonlinepage:{page}"))
    builder.row(InlineKeyboardButton(text="← К подпискам", callback_data="submainback"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    return builder.as_markup()


async def show_sub_online_info(target: CallbackQuery | Message, gid: str, page: int = 0):
    name = ONLINE_GROUP_ID_TO_NAME.get(gid)
    if not name:
        await send_or_edit(target, "Группа не найдена.", reply_markup=back_markup("← К онлайн-группам", f"subonlinepage:{page}"))
        return
    user_data = get_user_sub(str(target.from_user.id))
    await send_or_edit(
        target,
        online_subscription_card_text(name, user_data),
        parse_mode=HTML_MODE,
        reply_markup=online_subscription_card_keyboard(gid, page, user_data),
        disable_web_page_preview=True,
    )

async def show_sub_live_country_selector(target: CallbackQuery | Message):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📍 Найти город", callback_data="sublivecitysearch"))
    builder.row(InlineKeyboardButton(text="← К подпискам", callback_data="submainback"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    await send_or_edit(target, "🏙 Для живых подписок найдите город через поиск.", reply_markup=builder.as_markup())

async def show_sub_live_city_selector(target: CallbackQuery | Message, country: str, page: int = 0):
    await show_sub_live_country_selector(target)

async def show_sub_live_list(target: CallbackQuery | Message, city: str, country: Optional[str] = None, page: int = 0):
    """Короткий список живых групп города: название + время + ориентир. Подробности и подписка — в карточке группы."""
    user_data = get_user_sub(str(target.from_user.id))
    builder = InlineKeyboardBuilder()
    all_prefix = "🔔" if user_data.get("all_live") else "🔕"
    builder.row(InlineKeyboardButton(text=f"{all_prefix} Все живые в этом городе", callback_data="subtoggleliveall"))

    city_groups = sorted(get_live_groups_for_city(city, country), key=lambda g: (g.get("name", "").lower(), compact_address_hint(g.get("address", "")).lower()))
    if not city_groups:
        builder.row(InlineKeyboardButton(text="🏙 Сменить город", callback_data="sublivecitychange"))
        builder.row(InlineKeyboardButton(text="← К подпискам", callback_data="submainback"))
        builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
        await send_or_edit(target, f"🏙 <b>{escape_html(city)}</b>\n\nЖивые группы не найдены.", parse_mode=HTML_MODE, reply_markup=builder.as_markup())
        return

    seen_names = set()
    visible_groups_all = []
    for group in city_groups:
        name = group.get("name", "")
        if name in seen_names:
            continue
        seen_names.add(name)
        visible_groups_all.append(group)

    page, visible_groups, total_pages = page_slice(visible_groups_all, page)

    lines = [
        f"🏙 <b>{escape_html(get_country_city_label(country, city))}</b>",
        "",
        "Выберите группу, чтобы посмотреть подробности и настроить подписку.",
        "",
    ]
    if total_pages > 1:
        lines.append(f"Страница {page + 1} из {total_pages}")
        lines.append("")

    for group in visible_groups:
        name = group.get("name", "")
        gid = make_short_id("l", name)
        subscribed = is_user_subscribed_to_live(user_data, name)
        bell = "🔔" if subscribed else "🔕"
        button_text = f"{live_subscription_button_text(group)} {bell}"
        builder.row(InlineKeyboardButton(text=button_text, callback_data=f"subliveinfo:{page}:{gid}"))

    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="← Предыдущие", callback_data=f"sublivepage:{page - 1}"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="Следующие →", callback_data=f"sublivepage:{page + 1}"))
        if nav:
            builder.row(*nav)

    builder.row(InlineKeyboardButton(text="🏙 Сменить город", callback_data="sublivecitychange"))
    builder.row(InlineKeyboardButton(text="← К подпискам", callback_data="submainback"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    await send_or_edit(
        target,
        "\n".join(lines).strip(),
        parse_mode=HTML_MODE,
        reply_markup=builder.as_markup(),
    )


def find_live_group_by_gid(city: str, country: Optional[str], gid: str) -> Optional[dict]:
    for group in get_live_groups_for_city(city, country):
        if make_short_id("l", group.get("name", "")) == gid:
            return group
    return None


def live_subscription_card_text(group: dict, user_data: dict) -> str:
    name = group.get("name", "")
    status = "включена" if is_user_subscribed_to_live(user_data, name) else "выключена"
    country = group.get("country")
    city = group.get("city", "")
    address = group.get("address", "")
    schedule = format_group_days_for_search(group, limit=30)
    lines = [
        f"🏙 <b>{escape_html(name)}</b>",
        "",
        f"🕒 {escape_html(schedule)}",
        f"📍 {escape_html(address) if address else 'Адрес не указан'}",
        f"Город: <b>{escape_html(get_country_city_label(country, city))}</b>",
        "",
        f"Подписка: <b>{status}</b>",
    ]
    return "\n".join(lines)


def live_subscription_card_keyboard(gid: str, page: int, user_data: dict, group: dict) -> InlineKeyboardMarkup:
    name = group.get("name", "")
    sub_text = "Отключить подписку" if is_user_subscribed_to_live(user_data, name) else "Включить подписку"
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=sub_text, callback_data=f"subliveinfotoggle:{page}:{gid}"))
    builder.row(InlineKeyboardButton(text="← К живым группам", callback_data=f"sublivepage:{page}"))
    builder.row(InlineKeyboardButton(text="← К подпискам", callback_data="submainback"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))
    return builder.as_markup()


async def show_sub_live_info(target: CallbackQuery | Message, gid: str, page: int = 0):
    user_data = get_user_sub(str(target.from_user.id))
    city = user_data.get("city")
    country = user_data.get("country")
    if not city:
        await show_sub_live_country_selector(target)
        return
    group = find_live_group_by_gid(city, country, gid)
    if not group:
        await send_or_edit(target, "Группа не найдена.", reply_markup=back_markup("← К живым группам", f"sublivepage:{page}"))
        return
    await send_or_edit(
        target,
        live_subscription_card_text(group, user_data),
        parse_mode=HTML_MODE,
        reply_markup=live_subscription_card_keyboard(gid, page, user_data, group),
    )

async def settings_menu(target: CallbackQuery | Message, group_type: str):
    user_data = get_user_sub(str(target.from_user.id))
    settings = get_online_settings(user_data) if group_type == "online" else get_live_settings(user_data)
    is_online = group_type == "online"
    title = "🌐 Настройки онлайн" if is_online else "🏙 Настройки живых"
    prefix = "online" if is_online else "live"

    builder = InlineKeyboardBuilder()

    # Утренняя сводка: если выбранного времени нет — сводка выключена.
    builder.row(InlineKeyboardButton(text="🕖 Утренняя сводка", callback_data="noop"))
    daily_enabled = settings.get("daily_enabled", True)
    daily_hour = int(settings.get("daily_hour", DEFAULT_DAILY_HOUR))

    hour_buttons = []
    for hour in DAY_HOUR_CHOICES:
        checked = "✅ " if daily_enabled and daily_hour == hour else ""
        hour_buttons.append(
            InlineKeyboardButton(
                text=f"{checked}{hour:02d}:00",
                callback_data=f"toggledailyhour:{prefix}:{hour}",
            )
        )
    builder.row(*hour_buttons[:3])
    builder.row(*hour_buttons[3:])

    # Напоминания: если ни одной галочки нет — напоминания выключены.
    builder.row(InlineKeyboardButton(text="⏰ Напоминания", callback_data="noop"))
    remind_enabled = settings.get("remind_enabled", True)
    remind_set = set(settings.get("remind_before", DEFAULT_REMIND_BEFORE)) if remind_enabled else set()

    def remind_toggle_button(minutes: int, label: str) -> InlineKeyboardButton:
        mark = "✅ " if minutes in remind_set else ""
        return InlineKeyboardButton(text=f"{mark}{label}", callback_data=f"toggleremind:{prefix}:{minutes}")

    if is_online:
        builder.row(
            remind_toggle_button(15, "За 15 мин"),
            remind_toggle_button(60, "За 1 час"),
        )
        builder.row(remind_toggle_button(120, "За 2 часа"))
    else:
        builder.row(
            remind_toggle_button(60, "За 1 час"),
            remind_toggle_button(120, "За 2 часа"),
        )

    builder.row(InlineKeyboardButton(text="← К настройкам", callback_data="settingsroot"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="mainmenu"))

    daily_text = f"включена, {daily_hour:02d}:00" if daily_enabled else "выключена"
    remind_text = format_remind_label(sorted(remind_set)) if remind_enabled and remind_set else "выключены"

    text = (
        f"<b>{title}</b>\n\n"
        f"🕖 <b>Утренняя сводка</b>\n"
        f"Статус: <b>{daily_text}</b>\n"
        "Нажмите выбранное время ещё раз, чтобы выключить сводку.\n\n"
        f"⏰ <b>Напоминания о встречах</b>\n"
        f"Статус: <b>{remind_text}</b>\n"
        "Снимите все галочки, чтобы выключить напоминания."
    )
    await send_or_edit(target, text, parse_mode=HTML_MODE, reply_markup=builder.as_markup())


DP = Dispatcher(storage=MemoryStorage())


@DP.callback_query(F.data == "noop")
async def noop_callback(callback: CallbackQuery):
    await safe_callback_answer(callback)


@DP.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer("Выберите раздел ниже.", reply_markup=REPLY_MAIN_MENU)
    await message.answer("🏠 <b>Главное меню</b>\n\nИспользуйте кнопки нижнего меню.", parse_mode=HTML_MODE)


@DP.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        HELP_TEXT,
        parse_mode=HTML_MODE,
        reply_markup=back_markup("⬅️ Главное меню", "mainmenu"),
    )


@DP.callback_query(F.data == "mainmenu")
async def main_menu_callback(callback: CallbackQuery):
    await send_or_edit(callback, "🏠 <b>Главное меню</b>\n\nИспользуйте кнопки нижнего меню.", parse_mode=HTML_MODE, reply_markup=None)


@DP.callback_query(F.data == "mainonline")
async def main_online_callback(callback: CallbackQuery):
    await send_or_edit(callback, "🌐 <b>Онлайн-встречи</b>\n\nПроходят в Telegram / Zoom / MAX.\nВремя указано московское.", parse_mode=HTML_MODE, reply_markup=build_online_menu_keyboard())


@DP.callback_query(F.data == "mainlive")
async def main_live_callback(callback: CallbackQuery):
    await send_or_edit(callback, "🏙 <b>Живые встречи</b>\n\nПроходят очно в выбранном городе.", parse_mode=HTML_MODE, reply_markup=build_live_root_keyboard(get_user_sub(str(callback.from_user.id))))


@DP.callback_query(F.data == "searchgroup")
async def search_group_callback(callback: CallbackQuery, state: FSMContext):
    await state.set_state(GroupNameSearch.waiting_for_name)
    await send_or_edit(callback, "Введите название группы или город:")


@DP.message(StateFilter(GroupNameSearch.waiting_for_name))
async def search_group_input(message: Message, state: FSMContext):
    await state.clear()
    query = (message.text or "").strip()
    if len(query) < 2:
        await message.answer(
            "Введите минимум 2 символа.",
            reply_markup=build_search_retry_keyboard(),
        )
        return

    city_matches = get_searchable_cities(query)
    online_matches = collect_online_group_matches(query)
    live_matches = collect_live_group_matches(query)
    if not city_matches and not online_matches and not live_matches:
        await message.answer(
            f"По запросу «{escape_html(query)}» ничего не найдено.",
            parse_mode=HTML_MODE,
            reply_markup=build_search_retry_keyboard(),
        )
        return

    await send_long_text(
        message,
        None,
        format_search_results(query),
        final_markup=build_search_results_keyboard(query),
        parse_mode=HTML_MODE,
        disable_web_page_preview=True,
    )


@DP.callback_query(F.data.startswith("searchcityfull"))
async def search_full_city_callback(callback: CallbackQuery):
    # Совместимость со старыми кнопками: список групп по городу больше не показываем.
    location_id = callback.data[len("searchcityfull"):]
    country, city = resolve_location_id(location_id)
    await send_or_edit(
        callback,
        f"🏙 <b>{escape_html(get_country_city_label(country, city))}</b>",
        parse_mode=HTML_MODE,
        reply_markup=live_period_keyboard(city, country),
    )


@DP.callback_query(F.data.startswith("searchshowcity"))
async def search_show_city_callback(callback: CallbackQuery):
    location_id = callback.data[len("searchshowcity"):]
    country, city = resolve_location_id(location_id)
    cid = get_location_id(city, country)
    await send_or_edit(
        callback,
        f"🏙 <b>{escape_html(get_country_city_label(country, city))}</b>",
        parse_mode=HTML_MODE,
        reply_markup=live_period_keyboard(city, country),
    )


@DP.callback_query(F.data.startswith("searchsetcity"))
async def search_set_city_callback(callback: CallbackQuery):
    location_id = callback.data[len("searchsetcity"):]
    country, city = resolve_location_id(location_id)

    uid = str(callback.from_user.id)
    user_data = get_user_sub(uid)
    user_data["country"] = country
    user_data["city"] = city
    set_user_sub(uid, user_data)

    await send_or_edit(
        callback,
        f"Город выбран: <b>{escape_html(get_country_city_label(country, city))}</b>",
        parse_mode=HTML_MODE,
        reply_markup=live_period_keyboard(city, country),
    )


@DP.callback_query(F.data == "livechoosecountry")
async def live_choose_country_callback(callback: CallbackQuery):
    await send_or_edit(callback, "Выбор по странам убран. Найдите город через поиск.", reply_markup=build_live_root_keyboard(get_user_sub(str(callback.from_user.id))))


@DP.callback_query(F.data.startswith("liveallcities:"))
async def live_all_cities_callback(callback: CallbackQuery):
    await send_or_edit(callback, "Список городов убран. Найдите город через поиск.", reply_markup=build_live_root_keyboard(get_user_sub(str(callback.from_user.id))))


@DP.callback_query(F.data == "liveallcities")
async def live_all_cities_legacy_callback(callback: CallbackQuery):
    await send_or_edit(callback, "Список городов убран. Найдите город через поиск.", reply_markup=build_live_root_keyboard(get_user_sub(str(callback.from_user.id))))


@DP.callback_query(F.data.startswith("livecountry"))
async def live_choose_city_callback(callback: CallbackQuery):
    await send_or_edit(callback, "Выбор по странам убран. Найдите город через поиск.", reply_markup=build_live_root_keyboard(get_user_sub(str(callback.from_user.id))))


@DP.callback_query(F.data == "livemycity")
async def live_my_city_callback(callback: CallbackQuery):
    user_data = get_user_sub(str(callback.from_user.id))
    city = user_data.get("city")
    if not city:
        await send_or_edit(callback, "🏙 Город ещё не выбран. Найдите город через поиск.", parse_mode=HTML_MODE, reply_markup=build_live_root_keyboard(user_data))
        return
    await send_or_edit(callback, f"🏙 <b>{escape_html(get_country_city_label(user_data.get('country'), city))}</b>", parse_mode=HTML_MODE, reply_markup=live_period_keyboard(city, user_data.get("country")))


@DP.callback_query(F.data == "maincontacts")
async def main_contacts_callback(callback: CallbackQuery):
    await send_or_edit(
        callback,
        CONTACTS_TEXT,
        parse_mode=HTML_MODE,
        disable_web_page_preview=True,
        reply_markup=back_markup("⬅️ Главное меню", "mainmenu"),
    )


@DP.callback_query(F.data == "mainsub")
async def main_sub_callback(callback: CallbackQuery):
    await show_sub_main(callback)


@DP.callback_query(F.data == "mainsettings")
async def main_settings_callback(callback: CallbackQuery):
    user_data = get_user_sub(str(callback.from_user.id))
    await send_or_edit(callback, render_settings_root_text(user_data), parse_mode=HTML_MODE, reply_markup=build_settings_root_menu(user_data))


@DP.callback_query(F.data == "mainmygroups")
async def main_my_groups_callback(callback: CallbackQuery):
    user_data = get_user_sub(str(callback.from_user.id))
    await send_or_edit(callback, render_my_groups_text(user_data), parse_mode=HTML_MODE, reply_markup=build_my_groups_menu())


@DP.callback_query(F.data == "mainunsubscribe")
async def main_unsubscribe_callback(callback: CallbackQuery):
    remove_subscriber(str(callback.from_user.id))
    await send_or_edit(callback, "🔕 Вы отписались от всех уведомлений.")


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
    await show_sub_live_country_selector(callback)


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
        user_data["groups"] = {k: v for k, v in user_data.get("groups", {}).items() if v.get("type") != "online"}
    set_user_sub(uid, user_data)
    await safe_callback_answer(callback, "Готово")
    await show_sub_online_list(callback)


@DP.callback_query(F.data.startswith("subonlinepage:"))
async def sub_online_page(callback: CallbackQuery):
    parts = callback.data.split(":", 1)
    try:
        page = int(parts[1])
    except Exception:
        page = 0
    await show_sub_online_list(callback, page)


@DP.callback_query(F.data.startswith("subonlineinfo:"))
async def sub_online_info(callback: CallbackQuery):
    parts = callback.data.split(":", 2)
    if len(parts) < 3:
        await safe_callback_answer(callback, "Ошибка")
        return
    try:
        page = int(parts[1])
    except Exception:
        page = 0
    gid = parts[2]
    await show_sub_online_info(callback, gid, page)


@DP.callback_query(F.data.startswith("subonlineinfotoggle:"))
async def sub_online_info_toggle(callback: CallbackQuery):
    parts = callback.data.split(":", 2)
    if len(parts) < 3:
        await safe_callback_answer(callback, "Ошибка")
        return
    try:
        page = int(parts[1])
    except Exception:
        page = 0
    gid = parts[2]
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
    await show_sub_online_info(callback, gid, page)


# Совместимость со старыми callback-кнопками, если они остались в открытом сообщении Telegram.
@DP.callback_query(F.data.startswith("subonlinepage"))
async def sub_online_page_legacy(callback: CallbackQuery):
    raw = callback.data[len("subonlinepage"):]
    try:
        page = int(raw)
    except Exception:
        page = 0
    await show_sub_online_list(callback, page)


@DP.callback_query(F.data.startswith("sublivepage:"))
async def sub_live_page(callback: CallbackQuery):
    parts = callback.data.split(":", 1)
    try:
        page = int(parts[1])
    except Exception:
        page = 0
    user_data = get_user_sub(str(callback.from_user.id))
    city = user_data.get("city")
    if city:
        await show_sub_live_list(callback, city, user_data.get("country"), page)
    else:
        await show_sub_live_country_selector(callback)


@DP.callback_query(F.data.startswith("subliveinfo:"))
async def sub_live_info(callback: CallbackQuery):
    parts = callback.data.split(":", 2)
    if len(parts) < 3:
        await safe_callback_answer(callback, "Ошибка")
        return
    try:
        page = int(parts[1])
    except Exception:
        page = 0
    gid = parts[2]
    await show_sub_live_info(callback, gid, page)


@DP.callback_query(F.data.startswith("subliveinfotoggle:"))
async def sub_live_info_toggle(callback: CallbackQuery):
    parts = callback.data.split(":", 2)
    if len(parts) < 3:
        await safe_callback_answer(callback, "Ошибка")
        return
    try:
        page = int(parts[1])
    except Exception:
        page = 0
    gid = parts[2]
    uid = str(callback.from_user.id)
    user_data = get_user_sub(uid)
    city = user_data.get("city")
    country = user_data.get("country")
    group = find_live_group_by_gid(city, country, gid) if city else None
    if not group:
        await safe_callback_answer(callback, "Ошибка ID")
        return
    group_name = group.get("name", "")
    groups = user_data.setdefault("groups", {})
    if group_name in groups and groups[group_name].get("type") == "live":
        del groups[group_name]
    else:
        groups[group_name] = {"type": "live", "info": LIVE_GROUP_INFO.get(group_name, {})}
        user_data["all_live"] = False
    set_user_sub(uid, user_data)
    await safe_callback_answer(callback, "Готово")
    await show_sub_live_info(callback, gid, page)


# Совместимость со старыми callback-кнопками.
@DP.callback_query(F.data.startswith("sublivepage"))
async def sub_live_page_legacy(callback: CallbackQuery):
    raw = callback.data[len("sublivepage"):]
    try:
        page = int(raw)
    except Exception:
        page = 0
    user_data = get_user_sub(str(callback.from_user.id))
    city = user_data.get("city")
    if city:
        await show_sub_live_list(callback, city, user_data.get("country"), page)
    else:
        await show_sub_live_country_selector(callback)


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
    user_data = get_user_sub(str(callback.from_user.id))
    await send_or_edit(callback, render_settings_root_text(user_data), parse_mode=HTML_MODE, reply_markup=build_settings_root_menu(user_data))


@DP.callback_query(F.data == "subsettingsonline")
async def sub_settings_online(callback: CallbackQuery):
    await settings_menu(callback, "online")


@DP.callback_query(F.data == "subsettingslive")
async def sub_settings_live(callback: CallbackQuery):
    await settings_menu(callback, "live")


@DP.callback_query(F.data.startswith("toggledaily:"))
async def toggle_daily_summary(callback: CallbackQuery):
    _, group_type = callback.data.split(":")
    uid = str(callback.from_user.id)
    user_data = get_user_sub(uid)
    settings = user_data[f"{group_type}_settings"]
    settings["daily_enabled"] = not settings.get("daily_enabled", True)
    set_user_sub(uid, user_data)
    await settings_menu(callback, group_type)


@DP.callback_query(F.data.startswith("setdailyhour:"))
async def set_daily_hour(callback: CallbackQuery):
    _, group_type, hour = callback.data.split(":")
    uid = str(callback.from_user.id)
    user_data = get_user_sub(uid)
    user_data[f"{group_type}_settings"]["daily_hour"] = int(hour)
    user_data[f"{group_type}_settings"]["daily_enabled"] = True
    set_user_sub(uid, user_data)
    await settings_menu(callback, group_type)



@DP.callback_query(F.data.startswith("toggleremind:"))
async def toggle_remind(callback: CallbackQuery):
    _, group_type, minutes_raw = callback.data.split(":")
    try:
        minutes = int(minutes_raw)
    except Exception:
        await safe_callback_answer(callback, "Ошибка")
        return

    allowed = {15, 60, 120} if group_type == "online" else {60, 120}
    if minutes not in allowed:
        await safe_callback_answer(callback, "Недоступный вариант")
        return

    uid = str(callback.from_user.id)
    user_data = get_user_sub(uid)
    settings = user_data[f"{group_type}_settings"]

    current = set(settings.get("remind_before", DEFAULT_REMIND_BEFORE)) if settings.get("remind_enabled", True) else set()
    current = {int(v) for v in current if int(v) in allowed}

    if minutes in current:
        current.remove(minutes)
    else:
        current.add(minutes)

    if current:
        settings["remind_enabled"] = True
        settings["remind_before"] = sorted(current)
    else:
        settings["remind_enabled"] = False
        # Оставляем последнее содержимое remind_before технически валидным, но выключаем напоминания флагом.
        settings["remind_before"] = []

    set_user_sub(uid, user_data)
    await settings_menu(callback, group_type)


@DP.callback_query(F.data.startswith("setremind:"))
async def set_remind(callback: CallbackQuery):
    _, group_type, option = callback.data.split(":")
    mapping = {"15": [15], "1": [60], "2": [120], "both": [60, 120], "15_1": [15, 60], "all": [15, 60, 120]}
    uid = str(callback.from_user.id)
    user_data = get_user_sub(uid)
    user_data[f"{group_type}_settings"]["remind_before"] = mapping.get(option, [60])
    user_data[f"{group_type}_settings"]["remind_enabled"] = True
    set_user_sub(uid, user_data)
    await settings_menu(callback, group_type)


@DP.callback_query(F.data == "onlinetoday")
async def online_today(callback: CallbackQuery):
    user_data = get_user_sub(str(callback.from_user.id))
    day_index = moscow_now().weekday()
    groups = get_online_by_day(day_index)
    text = f"{format_day_header(DAYS[day_index])}\n{ONLINE_TIME_NOTE}\n\n"
    text += "\n".join(format_online_group(t, n, u) for t, n, u in groups) if groups else "Нет групп."
    await send_or_edit(callback, text, parse_mode=HTML_MODE, disable_web_page_preview=True, reply_markup=back_markup("← К онлайн", "mainonline"))


@DP.callback_query(F.data == "onlinefull")
async def online_full(callback: CallbackQuery):
    await send_long_text(
        callback,
        "📋 Онлайн на всю неделю:\nВремя указано московское.",
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
    text = f"{format_day_header(DAYS[day_index])}\n{ONLINE_TIME_NOTE}\n\n"
    text += "\n".join(format_online_group(t, n, u) for t, n, u in groups) if groups else "Нет групп."
    await send_or_edit(callback, text, parse_mode=HTML_MODE, disable_web_page_preview=True, reply_markup=back_markup("← К дням", "onlinechooseday"))


@DP.callback_query(F.data == "modelive")
async def back_to_live(callback: CallbackQuery):
    await send_or_edit(callback, "🏙 <b>Живые встречи</b>\n\nПроходят очно в выбранном городе.", parse_mode=HTML_MODE, reply_markup=build_live_root_keyboard(get_user_sub(str(callback.from_user.id))))


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
    text += render_live_group_list(groups) if groups else "Нет групп."
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
    text += render_live_group_list(groups) if groups else "Нет групп."
    await send_or_edit(callback, text, parse_mode=HTML_MODE, reply_markup=back_markup("← К дням", f"livechooseday{cid}"))


@DP.message(F.text == "🌐 Онлайн")
@DP.message(F.text == "🌐 Онлайн-встречи")
async def btn_online(message: Message):
    await message.answer("🌐 <b>Онлайн-встречи</b>\n\nПроходят в Telegram / Zoom / MAX.\nВремя указано московское.", parse_mode=HTML_MODE, reply_markup=build_online_menu_keyboard())


@DP.message(F.text == "🏙 Живые")
@DP.message(F.text == "🏙 Живые встречи")
async def btn_live(message: Message):
    await message.answer("🏙 <b>Живые встречи</b>\n\nПроходят очно в выбранном городе.", parse_mode=HTML_MODE, reply_markup=build_live_root_keyboard(get_user_sub(str(message.from_user.id))))


@DP.message(F.text == "🔔 Подписки")
async def btn_subscriptions(message: Message):
    await show_sub_main(message)


# Совместимость со старыми клавиатурами: раньше эта кнопка была на первом экране.
@DP.message(F.text == "🔔 Мои подписки")
@DP.message(F.text == "⭐ Мои группы")
async def btn_my_groups(message: Message):
    await message.answer(
        render_my_groups_text(get_user_sub(str(message.from_user.id))),
        parse_mode=HTML_MODE,
        reply_markup=build_my_groups_menu(),
    )


@DP.message(F.text == "⚙️ Настройки")
async def btn_settings(message: Message):
    user_data = get_user_sub(str(message.from_user.id))
    await message.answer(render_settings_root_text(user_data), parse_mode=HTML_MODE, reply_markup=build_settings_root_menu(user_data))


@DP.message(F.text == "Настройки уведомлений")
@DP.message(F.text == "Настройки подписок")
async def btn_notification_settings(message: Message):
    user_data = get_user_sub(str(message.from_user.id))
    await message.answer(render_settings_root_text(user_data), parse_mode=HTML_MODE, reply_markup=build_settings_root_menu(user_data))


@DP.callback_query(F.data == "remainingtoday")
async def remaining_today_callback(callback: CallbackQuery):
    await send_or_edit(callback, "Раздел убран. Используйте расписание по дням или неделю.", reply_markup=build_removed_section_keyboard())


@DP.callback_query(F.data == "remainingonline")
async def remaining_online_callback(callback: CallbackQuery):
    await send_or_edit(callback, "Раздел убран. Используйте расписание по дням или неделю.", reply_markup=build_removed_section_keyboard())


@DP.callback_query(F.data == "remaininglive")
async def remaining_live_callback(callback: CallbackQuery):
    await send_or_edit(callback, "Раздел убран. Используйте расписание по дням или неделю.", reply_markup=build_removed_section_keyboard())


@DP.callback_query(F.data.startswith("remaininglive"))
async def remaining_live_legacy_callback(callback: CallbackQuery):
    await send_or_edit(callback, "Раздел убран. Используйте расписание по дням или неделю.", reply_markup=build_removed_section_keyboard())


@DP.message(F.text == "📩 Контакты")
@DP.message(F.text == "Контакты")
async def btn_contacts(message: Message):
    await message.answer(
        CONTACTS_TEXT,
        parse_mode=HTML_MODE,
        disable_web_page_preview=True,
        reply_markup=back_markup("⬅️ Главное меню", "mainmenu"),
    )


@DP.message(F.text == "❌ Отписаться от всего")
async def btn_unsubscribe_all(message: Message):
    remove_subscriber(str(message.from_user.id))
    await message.answer("🔕 Вы отписались от всех уведомлений.", reply_markup=REPLY_MAIN_MENU)


@DP.message(F.text == "🔎 Найти группу или город")
@DP.message(F.text == "🔍 Найти группу")
async def btn_search_group(message: Message, state: FSMContext):
    await state.set_state(GroupNameSearch.waiting_for_name)
    await message.answer("Введите название группы или город:")


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
