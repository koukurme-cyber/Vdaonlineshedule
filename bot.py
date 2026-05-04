import asyncio
import hashlib
import html
import json
import os
import random
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union

from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

BOT_TOKEN = os.getenv("BOT_TOKEN")
SUBSCRIBERS_FILE = "vda_subscribers.json"
LIVE_GROUPS_FILE = "live_groups.tsv"
CHECK_INTERVAL_SECONDS = 30

DAYS = [
    "Понедельник",
    "Вторник",
    "Среда",
    "Четверг",
    "Пятница",
    "Суббота",
    "Воскресенье",
]

SHORT_DAYS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

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

SPB_SUBURBS = [
    "Санкт-Петербург",
    "Пушкин",
    "Петергоф",
    "Всеволожск",
    "Выборг",
    "Кириши",
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
    "И эта боль тоже пройдет",
    "Отпусти. Пусти Бога",
    "Возвращайтесь снова и снова",
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


class SearchState(StatesGroup):
    waiting_for_query = State()


class CityState(StatesGroup):
    waiting_for_city = State()


class GroupKind:
    ONLINE = "online"
    LIVE = "live"


class GroupScope:
    ALL_ONLINE = "all_online"
    ALL_LIVE = "all_live"
    SINGLE = "single"


def moscow_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=3)


def escape_html(value: object) -> str:
    return html.escape(str(value), quote=True)


def time_to_minutes(hhmm: str) -> int:
    h, m = map(int, hhmm.split(":"))
    return h * 60 + m


def split_long_message(text: str, limit: int = 3800) -> List[str]:
    parts = []
    while len(text) > limit:
        cut = text.rfind("\n", 0, limit)
        if cut == -1:
            cut = limit
        parts.append(text[:cut].strip())
        text = text[cut:].lstrip("\n")
    if text.strip():
        parts.append(text.strip())
    return parts


def normalize_city_name(city: str) -> str:
    value = city.lower().strip()
    aliases = {
        "мск": "москва",
        "москва": "москва",
        "спб": "санкт-петербург",
        "питер": "санкт-петербург",
        "петербург": "санкт-петербург",
        "санкт-петербург": "санкт-петербург",
    }
    return aliases.get(value, value)


def week_of_month(dt: datetime.date) -> int:
    return ((dt.day - 1) // 7) + 1


def is_last_weekday_of_month(dt: datetime.date) -> bool:
    return (dt + timedelta(days=7)).month != dt.month


def day_entry_matches_date(day_entry: dict, target_date) -> bool:
    if day_entry["day"] != target_date.weekday():
        return False
    occurrence = day_entry.get("occurrence")
    if occurrence is None:
        return True
    if occurrence == "last":
        return is_last_weekday_of_month(target_date)
    return week_of_month(target_date) == occurrence


def parse_live_schedule(raw_lines: str) -> list:
    groups = []
    short_map = {
        "пн": 0,
        "пон": 0,
        "вт": 1,
        "вто": 1,
        "ср": 2,
        "сре": 2,
        "чт": 3,
        "чет": 3,
        "пт": 4,
        "пят": 4,
        "сб": 5,
        "суб": 5,
        "вс": 6,
        "вос": 6,
    }

    for line in raw_lines.strip().split("\n"):
        if not line.strip():
            continue

        parts = [c.strip() for c in line.split("\t")]
        if len(parts) < 5:
            continue

        country, city, name, address, time_str = parts[:5]
        if country != "Россия":
            continue

        time_str = time_str.replace('"', "").replace("\\n", " ")
        entries = re.split(r";|\n| и ", time_str)
        days = []

        for entry in entries:
            entry = entry.strip()
            if not entry:
                continue

            entry_lower = entry.lower()
            day_found = None

            for idx, day_name in enumerate(DAYS):
                if day_name.lower() in entry_lower or day_name[:3].lower() in entry_lower:
                    day_found = idx
                    break

            if day_found is None:
                for key, idx in short_map.items():
                    if re.search(rf"(^|\W){re.escape(key)}(\W|$)", entry_lower):
                        day_found = idx
                        break

            if day_found is None:
                continue

            occurrence = None
            if "последн" in entry_lower:
                occurrence = "last"
            elif "перв" in entry_lower:
                occurrence = 1
            elif "втор" in entry_lower and "вторник" not in entry_lower:
                occurrence = 2
            elif "треть" in entry_lower:
                occurrence = 3
            elif "четверт" in entry_lower and "четверг" not in entry_lower:
                occurrence = 4

            is_work_meeting = "рабоч" in entry_lower or "рабочка" in entry_lower

            times = re.findall(r"(\d{1,2}[.:]\d{2})\s*[-–]\s*(\d{1,2}[.:]\d{2})", entry)
            if not times:
                times = re.findall(r"с\s*(\d{1,2}[.:-]\d{2})\s*до\s*(\d{1,2}[.:-]\d{2})", entry)
            if not times:
                single = re.findall(r"в\s*(\d{1,2}[.:]\d{2})", entry)
                if not single:
                    single = re.findall(r"(?<!\d)(\d{1,2}[.:]\d{2})(?!\d)", entry)
                if single:
                    start = single[0].replace(".", ":").replace("-", ":")
                    h, m = start.split(":")
                    end = f"{(int(h) + 1):02d}:{m}"
                    times = [(start, end)]

            for start, end in times:
                days.append(
                    {
                        "day": day_found,
                        "start": start.replace(".", ":").replace("-", ":"),
                        "end": end.replace(".", ":").replace("-", ":"),
                        "occurrence": occurrence,
                        "is_work_meeting": is_work_meeting,
                    }
                )

        if days:
            groups.append(
                {
                    "city": city.strip(),
                    "name": name.strip(),
                    "address": address.strip(),
                    "days": days,
                }
            )

    return groups


def load_live_groups() -> list:
    try:
        with open(LIVE_GROUPS_FILE, "r", encoding="utf-8") as f:
            return parse_live_schedule(f.read())
    except FileNotFoundError:
        print(f"⚠️ Файл {LIVE_GROUPS_FILE} не найден. Живые группы не загружены.")
        return []
    except Exception as e:
        print(f"❌ Ошибка чтения {LIVE_GROUPS_FILE}: {e}")
        return []


LIVE_GROUPS = load_live_groups()


def make_short_id(prefix: str, value: str) -> str:
    return prefix + hashlib.md5(value.encode("utf-8")).hexdigest()[:12]


ONLINE_GROUP_ID_TO_NAME: Dict[str, str] = {}
LIVE_GROUP_ID_TO_NAME: Dict[str, str] = {}
GROUP_ID_TO_KIND: Dict[str, str] = {}

all_online_names = sorted({name for day_groups in ONLINE_SCHEDULE.values() for _, name, _ in day_groups})
for name in all_online_names:
    gid = make_short_id("o", name)
    ONLINE_GROUP_ID_TO_NAME[gid] = name
    GROUP_ID_TO_KIND[gid] = GroupKind.ONLINE

all_live_names = sorted({g["name"] for g in LIVE_GROUPS})
for name in all_live_names:
    gid = make_short_id("l", name)
    LIVE_GROUP_ID_TO_NAME[gid] = name
    GROUP_ID_TO_KIND[gid] = GroupKind.LIVE

city_to_id: Dict[str, str] = {}
id_to_city: Dict[str, str] = {}
city_id_counter = 0
for city in POPULAR_CITIES:
    cid = str(city_id_counter)
    city_to_id[city] = cid
    id_to_city[cid] = city
    city_id_counter += 1
for g in LIVE_GROUPS:
    city = g["city"]
    if city not in city_to_id:
        cid = str(city_id_counter)
        city_to_id[city] = cid
        id_to_city[cid] = city
        city_id_counter += 1


def load_subscribers() -> dict:
    try:
        with open(SUBSCRIBERS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_subscribers(data: dict) -> None:
    temp_file = SUBSCRIBERS_FILE + ".tmp"
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(temp_file, SUBSCRIBERS_FILE)


def normalize_user_sub(data: Optional[dict]) -> dict:
    base = {
        "city": None,
        "all_online": False,
        "all_live": False,
        "groups": {},
        "daily_hour": 7,
        "remind_before": [60],
        "meta": {"last_daily_sent": None, "last_reminders": {}},
    }

    if not isinstance(data, dict):
        return base

    for key in ("city", "all_online", "all_live", "groups", "daily_hour", "remind_before", "meta"):
        if key in data:
            base[key] = data[key]

    if isinstance(base.get("remind_before"), int):
        base["remind_before"] = [base["remind_before"]]
    if not base.get("remind_before"):
        base["remind_before"] = [60]

    if not isinstance(base.get("groups"), dict):
        base["groups"] = {}

    if not isinstance(base.get("meta"), dict):
        base["meta"] = {}
    base["meta"].setdefault("last_daily_sent", None)
    base["meta"].setdefault("last_reminders", {})

    return base


def get_user_sub(uid: str) -> dict:
    subs = load_subscribers()
    return normalize_user_sub(subs.get(uid))


def set_user_sub(uid: str, data: dict) -> None:
    subs = load_subscribers()
    subs[uid] = normalize_user_sub(data)
    save_subscribers(subs)


def remove_subscriber(uid: str) -> None:
    subs = load_subscribers()
    subs.pop(uid, None)
    save_subscribers(subs)


def get_online_url_by_name(group_name: str) -> Optional[str]:
    for day_groups in ONLINE_SCHEDULE.values():
        for _, name, url in day_groups:
            if name == group_name:
                return url
    return None


def get_live_group_by_name(group_name: str) -> Optional[dict]:
    for group in LIVE_GROUPS:
        if group["name"] == group_name:
            return group
    return None


def get_online_by_day(day_index: int) -> list:
    return sorted(ONLINE_SCHEDULE.get(day_index, []), key=lambda x: time_to_minutes(x[0]))


def get_live_groups_for_city(city: str) -> list:
    city_norm = normalize_city_name(city)
    result = []

    for group in LIVE_GROUPS:
        group_city = group["city"].lower().strip()

        if city_norm == "москва":
            if group_city == "москва" or group_city.startswith("москва,") or group_city.startswith("москва "):
                result.append(group)
        elif city_norm == "санкт-петербург":
            if group_city == "санкт-петербург" or group_city.startswith("санкт-петербург,") or group_city.startswith("санкт-петербург "):
                result.append(group)
        else:
            if city_norm in group_city:
                result.append(group)

    return result


def get_searchable_cities(query: str) -> list:
    query_lower = query.lower().strip()
    is_spb_search = query_lower in ("санкт-петербург", "спб", "питер", "петербург")
    matched, seen = [], set()

    for group in LIVE_GROUPS:
        city = group["city"]
        city_lower = city.lower()

        if query_lower in city_lower:
            if is_spb_search:
                is_suburb = False
                for suburb in SPB_SUBURBS:
                    if suburb.lower() in city_lower and suburb.lower() != "санкт-петербург":
                        is_suburb = True
                        break
                if is_suburb:
                    continue
            if city not in seen:
                matched.append(city)
                seen.add(city)

    if not matched and not is_spb_search:
        for group in LIVE_GROUPS:
            for suburb in SPB_SUBURBS:
                if query_lower in suburb.lower() and query_lower in group["city"].lower():
                    if group["city"] not in seen:
                        matched.append(group["city"])
                        seen.add(group["city"])

    return matched


def get_live_groups_for_date(city: Optional[str], target_date) -> list:
    if not city:
        return []

    result = []
    for group in get_live_groups_for_city(city):
        for entry in group["days"]:
            if day_entry_matches_date(entry, target_date):
                result.append(
                    {
                        "kind": GroupKind.LIVE,
                        "time": entry["start"],
                        "end": entry["end"],
                        "name": group["name"],
                        "address": group["address"],
                        "city": group["city"],
                        "is_work_meeting": entry.get("is_work_meeting", False),
                    }
                )

    return sorted(result, key=lambda x: (time_to_minutes(x["time"]), x["name"].lower()))


def get_online_groups_for_date(target_date) -> list:
    result = []
    for time_str, name, url in get_online_by_day(target_date.weekday()):
        result.append(
            {
                "kind": GroupKind.ONLINE,
                "time": time_str,
                "name": name,
                "url": url,
            }
        )
    return result


def get_groups_for_date(user_data: dict, target_date) -> list:
    city = user_data.get("city")
    items = []
    items.extend(get_online_groups_for_date(target_date))
    items.extend(get_live_groups_for_date(city, target_date))
    return sorted(items, key=lambda x: (time_to_minutes(x["time"]), x["kind"], x["name"].lower()))


def group_is_subscribed(user_data: dict, kind: str, name: str) -> bool:
    if kind == GroupKind.ONLINE:
        return bool(user_data.get("all_online")) or name in user_data.get("groups", {})
    return bool(user_data.get("all_live")) or name in user_data.get("groups", {})


def get_group_id(kind: str, name: str) -> str:
    return make_short_id("o" if kind == GroupKind.ONLINE else "l", name)


def format_group_line(item: dict, user_data: Optional[dict] = None, show_sub: bool = False, show_address: bool = True) -> str:
    kind = item["kind"]
    name = escape_html(item["name"])
    time_str = escape_html(item["time"])

    if kind == GroupKind.ONLINE:
        url = item.get("url") or get_online_url_by_name(item["name"]) or ""
        base = f'🟠 <b>{time_str}</b> — <a href="{escape_html(url)}">{name}</a>'
    else:
        work_label = " 🔧" if item.get("is_work_meeting") else ""
        base = f"🏙 <b>{time_str}</b> — {name}{work_label}"
        if show_address and item.get("address"):
            base += f"\n   📍 {escape_html(item['address'])}"

    if show_sub and user_data is not None:
        base += " 🔔" if group_is_subscribed(user_data, kind, item["name"]) else ""

    return base


def format_day_title(target_date) -> str:
    today = moscow_now().date()
    if target_date == today:
        prefix = "Сегодня"
    elif target_date == today + timedelta(days=1):
        prefix = "Завтра"
    else:
        prefix = DAYS[target_date.weekday()]
    return f"{prefix}, {target_date.strftime('%d.%m')}"


def get_next_item(items: list, now_dt: datetime) -> Optional[dict]:
    now_min = now_dt.hour * 60 + now_dt.minute
    future = [item for item in items if time_to_minutes(item["time"]) >= now_min]
    if not future:
        return None
    return sorted(future, key=lambda x: time_to_minutes(x["time"]))[0]


def format_until(time_str: str, now_dt: datetime) -> str:
    delta = time_to_minutes(time_str) - (now_dt.hour * 60 + now_dt.minute)
    if delta <= 0:
        return "сейчас"
    h, m = divmod(delta, 60)
    if h and m:
        return f"через {h} ч {m} мин"
    if h:
        return f"через {h} ч"
    return f"через {m} мин"


def build_today_text(uid: str, target_date=None) -> str:
    user_data = get_user_sub(uid)
    now_dt = moscow_now()
    if target_date is None:
        target_date = now_dt.date()

    items = get_groups_for_date(user_data, target_date)
    title = format_day_title(target_date)

    if not items:
        city_note = "" if user_data.get("city") else "\n\nГород для живых групп не выбран."
        return f"📍 <b>{escape_html(title)}</b>\n\nГрупп не найдено.{city_note}"

    lines = [f"📍 <b>{escape_html(title)}</b>"]

    if target_date == now_dt.date():
        next_item = get_next_item(items, now_dt)
        if next_item:
            icon = "🟠" if next_item["kind"] == GroupKind.ONLINE else "🏙"
            lines.append(
                f"Следующая: {icon} <b>{escape_html(next_item['time'])}</b> — {escape_html(next_item['name'])} ({format_until(next_item['time'], now_dt)})"
            )

    lines.append("")
    for item in items:
        lines.append(format_group_line(item, user_data, show_sub=True, show_address=True))

    if not user_data.get("city"):
        lines.append("\nГород для живых групп не выбран.")

    lines.append("\n🟠 онлайн · 🏙 живая")
    return "\n".join(lines)


def main_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📍 Сегодня", callback_data="today"))
    builder.row(
        InlineKeyboardButton(text="📅 Другой день", callback_data="choose_day"),
        InlineKeyboardButton(text="🔎 Найти", callback_data="search_start"),
    )
    builder.row(
        InlineKeyboardButton(text="🔔 Подписки", callback_data="subscriptions"),
        InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings"),
    )
    builder.row(InlineKeyboardButton(text="💫 Установка", callback_data="slogan"))
    return builder.as_markup()


def back_to_main_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="⬅️ Меню", callback_data="main"))
    return builder.as_markup()


def today_keyboard(target_date=None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if target_date is None:
        target_date = moscow_now().date()
    day_index = target_date.weekday()
    builder.row(
        InlineKeyboardButton(text="🔔 Подписаться на день", callback_data=f"sub_day_{day_index}"),
    )
    builder.row(
        InlineKeyboardButton(text="📅 Другой день", callback_data="choose_day"),
        InlineKeyboardButton(text="🔎 Найти", callback_data="search_start"),
    )
    builder.row(InlineKeyboardButton(text="⬅️ Меню", callback_data="main"))
    return builder.as_markup()


def days_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    today = moscow_now().date()
    for offset in range(7):
        date = today + timedelta(days=offset)
        label = "Сегодня" if offset == 0 else SHORT_DAYS[date.weekday()]
        builder.button(text=f"{label} {date.strftime('%d.%m')}", callback_data=f"day_{date.weekday()}_{date.strftime('%Y%m%d')}")
    builder.adjust(2)
    builder.row(InlineKeyboardButton(text="⬅️ Меню", callback_data="main"))
    return builder.as_markup()


def subscriptions_keyboard(uid: str) -> InlineKeyboardMarkup:
    data = get_user_sub(uid)
    builder = InlineKeyboardBuilder()

    online_label = "✅ Все онлайн" if data.get("all_online") else "🟠 Все онлайн"
    live_label = "✅ Все живые" if data.get("all_live") else "🏙 Все живые"
    builder.row(
        InlineKeyboardButton(text=online_label, callback_data="toggle_all_online"),
        InlineKeyboardButton(text=live_label, callback_data="toggle_all_live"),
    )

    groups = data.get("groups", {})
    for name, info in sorted(groups.items(), key=lambda x: (x[1].get("type", ""), x[0].lower())):
        kind = info.get("type")
        gid = get_group_id(kind, name)
        icon = "🟠" if kind == GroupKind.ONLINE else "🏙"
        builder.row(InlineKeyboardButton(text=f"❌ {icon} {name}", callback_data=f"unsub_{gid}"))

    builder.row(
        InlineKeyboardButton(text="➕ Добавить", callback_data="search_start"),
        InlineKeyboardButton(text="🗑 Очистить", callback_data="clear_subs_confirm"),
    )
    builder.row(InlineKeyboardButton(text="⬅️ Меню", callback_data="main"))
    return builder.as_markup()


def settings_keyboard(uid: str) -> InlineKeyboardMarkup:
    data = get_user_sub(uid)
    hour = int(data.get("daily_hour", 7))
    reminders = set(data.get("remind_before", [60]))

    builder = InlineKeyboardBuilder()
    for h in [5, 6, 7, 8, 9]:
        builder.button(text=f"{'✅ ' if h == hour else ''}{h:02d}:00", callback_data=f"set_hour_{h}")
    builder.adjust(5)
    builder.row(
        InlineKeyboardButton(text=f"{'✅ ' if reminders == {60} else ''}1 час", callback_data="set_remind_1"),
        InlineKeyboardButton(text=f"{'✅ ' if reminders == {120} else ''}2 часа", callback_data="set_remind_2"),
        InlineKeyboardButton(text=f"{'✅ ' if reminders == {60, 120} else ''}оба", callback_data="set_remind_both"),
    )
    builder.row(InlineKeyboardButton(text="🏙 Город", callback_data="city_choose"))
    builder.row(InlineKeyboardButton(text="⬅️ Меню", callback_data="main"))
    return builder.as_markup()


def city_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for city in POPULAR_CITIES:
        cid = city_to_id.get(city, city)
        builder.button(text=city, callback_data=f"city_{cid}")
    builder.adjust(2)
    builder.row(InlineKeyboardButton(text="🔎 Другой город", callback_data="city_search"))
    builder.row(InlineKeyboardButton(text="⬅️ Меню", callback_data="main"))
    return builder.as_markup()


def clear_subs_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="Да", callback_data="clear_subs_yes"),
        InlineKeyboardButton(text="Нет", callback_data="subscriptions"),
    )
    return builder.as_markup()


def search_results_keyboard(results: list) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for item in results[:30]:
        gid = get_group_id(item["kind"], item["name"])
        icon = "🟠" if item["kind"] == GroupKind.ONLINE else "🏙"
        builder.row(InlineKeyboardButton(text=f"➕ {icon} {item['name']}", callback_data=f"add_{gid}"))
    builder.row(InlineKeyboardButton(text="⬅️ Меню", callback_data="main"))
    return builder.as_markup()


def markup_signature(markup: Optional[InlineKeyboardMarkup]) -> str:
    if markup is None:
        return ""
    try:
        rows = []
        for row in markup.inline_keyboard:
            rows.append([(btn.text, btn.callback_data, btn.url) for btn in row])
        return json.dumps(rows, ensure_ascii=False, sort_keys=True)
    except Exception:
        return repr(markup)


async def safe_callback_answer(callback: CallbackQuery, text: str = "", show_alert: bool = False) -> None:
    try:
        await callback.answer(text, show_alert=show_alert)
    except Exception:
        pass


async def safe_edit_text(message: Message, text: str, **kwargs) -> None:
    try:
        current_text = message.html_text or message.text or ""
    except Exception:
        current_text = ""

    new_markup = kwargs.get("reply_markup")
    current_markup_sig = markup_signature(message.reply_markup) if getattr(message, "reply_markup", None) else ""
    new_markup_sig = markup_signature(new_markup)

    if current_text == text and current_markup_sig == new_markup_sig:
        return

    try:
        await message.edit_text(text, **kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise


def build_subscriptions_text(uid: str) -> str:
    data = get_user_sub(uid)
    groups = data.get("groups", {})
    lines = ["🔔 <b>Подписки</b>"]

    if data.get("all_online"):
        lines.append("✅ Все онлайн")
    if data.get("all_live"):
        city = data.get("city") or "выбранный город"
        lines.append(f"✅ Все живые: {escape_html(city)}")

    if groups:
        lines.append("")
        for name, info in sorted(groups.items(), key=lambda x: (x[1].get("type", ""), x[0].lower())):
            icon = "🟠" if info.get("type") == GroupKind.ONLINE else "🏙"
            lines.append(f"{icon} {escape_html(name)}")

    if not data.get("all_online") and not data.get("all_live") and not groups:
        lines.append("Пока пусто.")

    lines.append("\n🟠 онлайн · 🏙 живая")
    return "\n".join(lines)


def build_settings_text(uid: str) -> str:
    data = get_user_sub(uid)
    hour = int(data.get("daily_hour", 7))
    reminders = set(data.get("remind_before", [60]))

    if reminders == {60, 120}:
        remind_label = "за 1 и 2 часа"
    elif reminders == {120}:
        remind_label = "за 2 часа"
    else:
        remind_label = "за 1 час"

    city = data.get("city") or "не выбран"

    return (
        "⚙️ <b>Настройки</b>\n\n"
        f"Расписание: <b>{hour:02d}:00</b>\n"
        f"Напоминание: <b>{remind_label}</b>\n"
        f"Город: <b>{escape_html(city)}</b>"
    )


def add_subscription(uid: str, kind: str, name: str) -> Tuple[bool, str]:
    data = get_user_sub(uid)
    groups = data.setdefault("groups", {})

    if kind == GroupKind.ONLINE:
        if data.get("all_online"):
            return False, "Уже включены все онлайн-группы"
        url = get_online_url_by_name(name)
        groups[name] = {"type": GroupKind.ONLINE, "info": {"url": url}}
    else:
        if data.get("all_live"):
            return False, "Уже включены все живые группы"
        group = get_live_group_by_name(name)
        info = {}
        if group:
            info = {"city": group.get("city"), "address": group.get("address")}
            if not data.get("city"):
                data["city"] = group.get("city")
        groups[name] = {"type": GroupKind.LIVE, "info": info}

    set_user_sub(uid, data)
    return True, "Добавлено"


def remove_subscription(uid: str, kind: str, name: str) -> bool:
    data = get_user_sub(uid)
    groups = data.setdefault("groups", {})
    if name in groups:
        del groups[name]
        set_user_sub(uid, data)
        return True
    return False


def resolve_group_by_id(gid: str) -> Tuple[Optional[str], Optional[str]]:
    if gid in ONLINE_GROUP_ID_TO_NAME:
        return GroupKind.ONLINE, ONLINE_GROUP_ID_TO_NAME[gid]
    if gid in LIVE_GROUP_ID_TO_NAME:
        return GroupKind.LIVE, LIVE_GROUP_ID_TO_NAME[gid]
    return None, None


def search_groups(query: str, uid: str) -> list:
    query_norm = query.lower().strip()
    user_data = get_user_sub(uid)
    results = []
    seen = set()

    for day_groups in ONLINE_SCHEDULE.values():
        for time_str, name, url in day_groups:
            key = (GroupKind.ONLINE, name)
            if key in seen:
                continue
            if query_norm in name.lower() or query_norm in "онлайн":
                results.append({"kind": GroupKind.ONLINE, "time": time_str, "name": name, "url": url})
                seen.add(key)

    matched_cities = get_searchable_cities(query)
    city_candidates = matched_cities if matched_cities else [user_data.get("city")] if user_data.get("city") else []

    for group in LIVE_GROUPS:
        key = (GroupKind.LIVE, group["name"])
        if key in seen:
            continue

        by_name = query_norm in group["name"].lower()
        by_city = query_norm in group["city"].lower()
        by_address = query_norm in group["address"].lower()
        by_matched_city = group["city"] in city_candidates

        if by_name or by_city or by_address or by_matched_city:
            first_time = "--:--"
            if group.get("days"):
                first_time = sorted(group["days"], key=lambda d: time_to_minutes(d["start"]))[0]["start"]
            results.append(
                {
                    "kind": GroupKind.LIVE,
                    "time": first_time,
                    "name": group["name"],
                    "address": group["address"],
                    "city": group["city"],
                }
            )
            seen.add(key)

    return sorted(results, key=lambda x: (x["kind"], x["name"].lower()))


def build_search_results_text(query: str, results: list) -> str:
    if not results:
        return f"🔎 <b>{escape_html(query)}</b>\n\nНичего не найдено."

    lines = [f"🔎 <b>{escape_html(query)}</b>", ""]
    for item in results[:30]:
        if item["kind"] == GroupKind.ONLINE:
            lines.append(f"🟠 {escape_html(item['name'])}")
        else:
            lines.append(f"🏙 {escape_html(item['name'])}\n   📍 {escape_html(item.get('city', ''))}, {escape_html(item.get('address', ''))}")
    lines.append("\n🟠 онлайн · 🏙 живая")
    return "\n".join(lines)


def get_day_date_from_weekday(day_index: int) -> datetime.date:
    today = moscow_now().date()
    return today + timedelta(days=(day_index - today.weekday()) % 7)


def build_daily_message(uid: str, data: dict, now_dt: datetime) -> Optional[str]:
    today = now_dt.date()
    items = get_groups_for_date(data, today)
    subscribed = [item for item in items if group_is_subscribed(data, item["kind"], item["name"])]

    if not subscribed:
        return None

    lines = [f"📍 <b>Сегодня, {DAYS[today.weekday()]}</b>", ""]
    for item in subscribed:
        lines.append(format_group_line(item, data, show_sub=False, show_address=True))
    return "\n".join(lines)


def build_reminder_key(kind: str, group_name: str, date_str: str, time_str: str, minutes_before: int) -> str:
    return f"{kind}|{group_name}|{date_str}|{time_str}|{minutes_before}"


def build_single_reminder(item: dict, minutes_before: int) -> str:
    before_text = "через час" if minutes_before == 60 else "через два часа"

    if item["kind"] == GroupKind.ONLINE:
        url = item.get("url") or get_online_url_by_name(item["name"]) or ""
        return (
            f"Напоминание: {before_text} начнётся группа.\n\n"
            f"🟠 <b>{escape_html(item['name'])}</b>\n"
            f"Начало: <b>{escape_html(item['time'])}</b>\n"
            f"<a href=\"{escape_html(url)}\">Войти</a>"
        )

    work_label = " 🔧" if item.get("is_work_meeting") else ""
    return (
        f"Напоминание: {before_text} начнётся живая встреча.\n\n"
        f"🏙 <b>{escape_html(item['name'])}</b>{work_label}\n"
        f"Начало: <b>{escape_html(item['time'])}</b>\n"
        f"📍 {escape_html(item.get('address', ''))}"
    )


def build_multi_reminder(time_str: str, items: list, minutes_before: int) -> str:
    before_text = "через час" if minutes_before == 60 else "через два часа"
    lines = [f"Напоминание: {before_text} начнутся группы.", "", f"Начало: <b>{escape_html(time_str)}</b>", ""]

    for item in sorted(items, key=lambda x: (x["kind"], x["name"].lower())):
        if item["kind"] == GroupKind.ONLINE:
            url = item.get("url") or get_online_url_by_name(item["name"]) or ""
            lines.append(f'🟠 <a href="{escape_html(url)}"><b>{escape_html(item["name"])}</b></a>')
        else:
            work_label = " 🔧" if item.get("is_work_meeting") else ""
            lines.append(f"🏙 <b>{escape_html(item['name'])}</b>{work_label} — {escape_html(item.get('address', ''))}")

    return "\n".join(lines)


def collect_due_reminders(user_data: dict, now_dt: datetime) -> list:
    due = []
    today = now_dt.date()
    today_str = today.strftime("%Y-%m-%d")
    now_minutes = now_dt.hour * 60 + now_dt.minute
    reminders_meta = user_data.setdefault("meta", {}).setdefault("last_reminders", {})
    remind_before = user_data.get("remind_before", [60])

    items = get_groups_for_date(user_data, today)
    subscribed = [item for item in items if group_is_subscribed(user_data, item["kind"], item["name"])]

    due_by_time: Dict[Tuple[str, int], list] = {}
    for item in subscribed:
        group_minutes = time_to_minutes(item["time"])
        for r_min in remind_before:
            diff = group_minutes - now_minutes
            if 0 <= diff <= max(1, CHECK_INTERVAL_SECONDS // 60) and diff == r_min:
                due_by_time.setdefault((item["time"], r_min), []).append(item)
            elif diff == r_min:
                due_by_time.setdefault((item["time"], r_min), []).append(item)

    for (time_str, r_min), items_at_time in due_by_time.items():
        if len(items_at_time) == 1:
            item = items_at_time[0]
            key = build_reminder_key(item["kind"], item["name"], today_str, time_str, r_min)
            if reminders_meta.get(key) != today_str:
                due.append((key, build_single_reminder(item, r_min)))
        else:
            names = "|".join(sorted(f"{item['kind']}:{item['name']}" for item in items_at_time))
            key = build_reminder_key("multi", names, today_str, time_str, r_min)
            if reminders_meta.get(key) != today_str:
                due.append((key, build_multi_reminder(time_str, items_at_time, r_min)))

    return due


def cleanup_old_reminders(user_data: dict, now_dt: datetime) -> None:
    reminders = user_data.setdefault("meta", {}).setdefault("last_reminders", {})
    cutoff = (now_dt.date() - timedelta(days=3)).strftime("%Y-%m-%d")
    for key, value in list(reminders.items()):
        if isinstance(value, str) and value < cutoff:
            del reminders[key]


async def send_daily_notifications(bot: Bot, now_dt: datetime) -> None:
    subs = load_subscribers()
    today_str = now_dt.strftime("%Y-%m-%d")
    changed = False

    for uid, raw_data in subs.items():
        data = normalize_user_sub(raw_data)
        if not data.get("groups") and not data.get("all_online") and not data.get("all_live"):
            subs[uid] = data
            continue

        if now_dt.hour != int(data.get("daily_hour", 7)):
            subs[uid] = data
            continue

        if data.setdefault("meta", {}).get("last_daily_sent") == today_str:
            subs[uid] = data
            continue

        message_text = build_daily_message(uid, data, now_dt)
        if not message_text:
            subs[uid] = data
            continue

        try:
            await bot.send_message(int(uid), message_text, parse_mode="HTML", disable_web_page_preview=True)
            data["meta"]["last_daily_sent"] = today_str
            changed = True
        except Exception as e:
            print(f"❌ Не удалось отправить daily пользователю {uid}: {e}")

        subs[uid] = data

    if changed:
        save_subscribers(subs)


async def send_reminders(bot: Bot, now_dt: datetime) -> None:
    subs = load_subscribers()
    changed = False
    today_str = now_dt.strftime("%Y-%m-%d")

    for uid, raw_data in subs.items():
        data = normalize_user_sub(raw_data)
        cleanup_old_reminders(data, now_dt)
        due = collect_due_reminders(data, now_dt)

        if not due:
            subs[uid] = data
            continue

        for key, text in due:
            try:
                await bot.send_message(int(uid), text, parse_mode="HTML", disable_web_page_preview=True)
                data["meta"]["last_reminders"][key] = today_str
                changed = True
            except Exception as e:
                print(f"❌ Не удалось отправить reminder пользователю {uid}: {e}")

        subs[uid] = data

    if changed:
        save_subscribers(subs)


async def notifications_worker(bot: Bot) -> None:
    print("✅ Планировщик уведомлений запущен")
    while True:
        try:
            now_dt = moscow_now().replace(second=0, microsecond=0)
            await send_daily_notifications(bot, now_dt)
            await send_reminders(bot, now_dt)
        except Exception as e:
            print(f"❌ Ошибка в notifications_worker: {e}")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


dp = Dispatcher(storage=MemoryStorage())


@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "🏠 <b>Меню</b>\n\n🟠 онлайн · 🏙 живая",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )


@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "Команды:\n/start — меню\n/help — помощь\n/slogan — установка",
        reply_markup=back_to_main_keyboard(),
    )


@dp.message(Command("slogan"))
async def cmd_slogan(message: Message):
    slogan = random.choice(SLOGANS_AND_AFFIRMATIONS)
    await message.answer(
        f"💫 <i>{escape_html(slogan)}</i>",
        parse_mode="HTML",
        reply_markup=back_to_main_keyboard(),
    )


@dp.callback_query(F.data == "main")
async def cb_main(callback: CallbackQuery):
    await safe_edit_text(
        callback.message,
        "🏠 <b>Меню</b>\n\n🟠 онлайн · 🏙 живая",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )
    await safe_callback_answer(callback)


@dp.callback_query(F.data == "today")
async def cb_today(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    text = build_today_text(uid)
    await safe_edit_text(
        callback.message,
        text,
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=today_keyboard(),
    )
    await safe_callback_answer(callback)


@dp.callback_query(F.data == "choose_day")
async def cb_choose_day(callback: CallbackQuery):
    await safe_edit_text(
        callback.message,
        "📅 День недели",
        reply_markup=days_keyboard(),
    )
    await safe_callback_answer(callback)


@dp.callback_query(F.data.startswith("day_"))
async def cb_day(callback: CallbackQuery):
    parts = callback.data.split("_")
    day_index = int(parts[1])
    target_date = datetime.strptime(parts[2], "%Y%m%d").date() if len(parts) > 2 else get_day_date_from_weekday(day_index)
    uid = str(callback.from_user.id)
    text = build_today_text(uid, target_date)
    await safe_edit_text(
        callback.message,
        text,
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=today_keyboard(target_date),
    )
    await safe_callback_answer(callback)


@dp.callback_query(F.data.startswith("sub_day_"))
async def cb_sub_day(callback: CallbackQuery):
    day_index = int(callback.data[len("sub_day_"):])
    target_date = get_day_date_from_weekday(day_index)
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    items = get_groups_for_date(data, target_date)

    added = 0
    for item in items:
        ok, _ = add_subscription(uid, item["kind"], item["name"])
        if ok:
            added += 1

    await safe_callback_answer(callback, f"Добавлено: {added}")
    text = build_today_text(uid, target_date)
    await safe_edit_text(
        callback.message,
        text,
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=today_keyboard(target_date),
    )


@dp.callback_query(F.data == "search_start")
async def cb_search_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SearchState.waiting_for_query)
    await safe_edit_text(
        callback.message,
        "🔎 Введите город или название группы:",
        reply_markup=back_to_main_keyboard(),
    )
    await safe_callback_answer(callback)


@dp.message(StateFilter(SearchState.waiting_for_query))
async def search_input(message: Message, state: FSMContext):
    query = (message.text or "").strip()
    await state.clear()

    if not query:
        await message.answer("Пустой запрос.", reply_markup=back_to_main_keyboard())
        return

    results = search_groups(query, str(message.from_user.id))
    await message.answer(
        build_search_results_text(query, results),
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=search_results_keyboard(results) if results else back_to_main_keyboard(),
    )


@dp.callback_query(F.data.startswith("add_"))
async def cb_add(callback: CallbackQuery):
    gid = callback.data[len("add_"):]
    kind, name = resolve_group_by_id(gid)
    if not kind or not name:
        await safe_callback_answer(callback, "Группа не найдена", show_alert=True)
        return

    ok, text = add_subscription(str(callback.from_user.id), kind, name)
    await safe_callback_answer(callback, text)


@dp.callback_query(F.data == "subscriptions")
async def cb_subscriptions(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    await safe_edit_text(
        callback.message,
        build_subscriptions_text(uid),
        parse_mode="HTML",
        reply_markup=subscriptions_keyboard(uid),
    )
    await safe_callback_answer(callback)


@dp.callback_query(F.data == "toggle_all_online")
async def cb_toggle_all_online(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    data["all_online"] = not bool(data.get("all_online"))

    if data["all_online"]:
        data["groups"] = {k: v for k, v in data.get("groups", {}).items() if v.get("type") != GroupKind.ONLINE}
        message = "Все онлайн включены"
    else:
        message = "Все онлайн выключены"

    set_user_sub(uid, data)
    await safe_callback_answer(callback, message)
    await safe_edit_text(
        callback.message,
        build_subscriptions_text(uid),
        parse_mode="HTML",
        reply_markup=subscriptions_keyboard(uid),
    )


@dp.callback_query(F.data == "toggle_all_live")
async def cb_toggle_all_live(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)

    if not data.get("city"):
        await safe_callback_answer(callback, "Сначала выберите город", show_alert=True)
        await safe_edit_text(callback.message, "🏙 Город", reply_markup=city_keyboard())
        return

    data["all_live"] = not bool(data.get("all_live"))

    if data["all_live"]:
        data["groups"] = {k: v for k, v in data.get("groups", {}).items() if v.get("type") != GroupKind.LIVE}
        message = "Все живые включены"
    else:
        message = "Все живые выключены"

    set_user_sub(uid, data)
    await safe_callback_answer(callback, message)
    await safe_edit_text(
        callback.message,
        build_subscriptions_text(uid),
        parse_mode="HTML",
        reply_markup=subscriptions_keyboard(uid),
    )


@dp.callback_query(F.data.startswith("unsub_"))
async def cb_unsub(callback: CallbackQuery):
    gid = callback.data[len("unsub_"):]
    kind, name = resolve_group_by_id(gid)
    uid = str(callback.from_user.id)

    if not kind or not name:
        await safe_callback_answer(callback, "Группа не найдена", show_alert=True)
        return

    remove_subscription(uid, kind, name)
    await safe_callback_answer(callback, "Удалено")
    await safe_edit_text(
        callback.message,
        build_subscriptions_text(uid),
        parse_mode="HTML",
        reply_markup=subscriptions_keyboard(uid),
    )


@dp.callback_query(F.data == "clear_subs_confirm")
async def cb_clear_subs_confirm(callback: CallbackQuery):
    await safe_edit_text(
        callback.message,
        "Очистить все подписки?",
        reply_markup=clear_subs_keyboard(),
    )
    await safe_callback_answer(callback)


@dp.callback_query(F.data == "clear_subs_yes")
async def cb_clear_subs_yes(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    data["groups"] = {}
    data["all_online"] = False
    data["all_live"] = False
    set_user_sub(uid, data)
    await safe_callback_answer(callback, "Очищено")
    await safe_edit_text(
        callback.message,
        build_subscriptions_text(uid),
        parse_mode="HTML",
        reply_markup=subscriptions_keyboard(uid),
    )


@dp.callback_query(F.data == "settings")
async def cb_settings(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    await safe_edit_text(
        callback.message,
        build_settings_text(uid),
        parse_mode="HTML",
        reply_markup=settings_keyboard(uid),
    )
    await safe_callback_answer(callback)


@dp.callback_query(F.data.startswith("set_hour_"))
async def cb_set_hour(callback: CallbackQuery):
    hour = int(callback.data[len("set_hour_"):])
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    data["daily_hour"] = hour
    set_user_sub(uid, data)
    await safe_callback_answer(callback, f"{hour:02d}:00")
    await safe_edit_text(
        callback.message,
        build_settings_text(uid),
        parse_mode="HTML",
        reply_markup=settings_keyboard(uid),
    )


@dp.callback_query(F.data.startswith("set_remind_"))
async def cb_set_remind(callback: CallbackQuery):
    option = callback.data[len("set_remind_"):]
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)

    if option == "1":
        data["remind_before"] = [60]
    elif option == "2":
        data["remind_before"] = [120]
    elif option == "both":
        data["remind_before"] = [60, 120]
    else:
        data["remind_before"] = [60]

    set_user_sub(uid, data)
    await safe_callback_answer(callback, "Сохранено")
    await safe_edit_text(
        callback.message,
        build_settings_text(uid),
        parse_mode="HTML",
        reply_markup=settings_keyboard(uid),
    )


@dp.callback_query(F.data == "city_choose")
async def cb_city_choose(callback: CallbackQuery):
    await safe_edit_text(callback.message, "🏙 Город", reply_markup=city_keyboard())
    await safe_callback_answer(callback)


@dp.callback_query(F.data.startswith("city_"))
async def cb_city_set(callback: CallbackQuery):
    cid = callback.data[len("city_"):]
    city = id_to_city.get(cid, cid)
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    data["city"] = city
    set_user_sub(uid, data)
    await safe_callback_answer(callback, city)
    await safe_edit_text(
        callback.message,
        build_settings_text(uid),
        parse_mode="HTML",
        reply_markup=settings_keyboard(uid),
    )


@dp.callback_query(F.data == "city_search")
async def cb_city_search(callback: CallbackQuery, state: FSMContext):
    await state.set_state(CityState.waiting_for_city)
    await safe_edit_text(callback.message, "Введите город:", reply_markup=back_to_main_keyboard())
    await safe_callback_answer(callback)


@dp.message(StateFilter(CityState.waiting_for_city))
async def city_input(message: Message, state: FSMContext):
    query = (message.text or "").strip()
    await state.clear()
    matched = get_searchable_cities(query)

    if not matched:
        await message.answer("Город не найден.", reply_markup=back_to_main_keyboard())
        return

    uid = str(message.from_user.id)
    city = matched[0]
    data = get_user_sub(uid)
    data["city"] = city
    set_user_sub(uid, data)

    await message.answer(
        build_settings_text(uid),
        parse_mode="HTML",
        reply_markup=settings_keyboard(uid),
    )


@dp.callback_query(F.data == "slogan")
async def cb_slogan(callback: CallbackQuery):
    slogan = random.choice(SLOGANS_AND_AFFIRMATIONS)
    await safe_edit_text(
        callback.message,
        f"💫 <i>{escape_html(slogan)}</i>",
        parse_mode="HTML",
        reply_markup=back_to_main_keyboard(),
    )
    await safe_callback_answer(callback)


@dp.message()
async def fallback(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is not None:
        return
    await message.answer(
        "🏠 <b>Меню</b>\n\n🟠 онлайн · 🏙 живая",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )


async def main():
    if not BOT_TOKEN:
        print("❌ BOT_TOKEN не задан")
        return

    bot = Bot(token=BOT_TOKEN)
    asyncio.create_task(notifications_worker(bot))
    print("✅ Бот запущен")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
