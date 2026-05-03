import asyncio
import hashlib
import json
import os
import random
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

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

BOT_TOKEN = os.getenv("BOT_TOKEN")
SUBSCRIBERS_FILE = "vda_subscribers.json"
CHECK_INTERVAL_SECONDS = 30
DAILY_SEND_HOUR = 7
DAILY_SEND_MINUTE = 0

# ==================== СОСТОЯНИЯ ====================
class LiveGroupSearch(StatesGroup):
    waiting_for_city = State()

class SubCitySearch(StatesGroup):
    waiting_for_city = State()

# ==================== ВРЕМЯ ====================
def moscow_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=3)

def time_to_minutes(hhmm: str) -> int:
    h, m = map(int, hhmm.split(":"))
    return h * 60 + m

def minutes_to_time(minutes: int) -> str:
    return f"{minutes // 60:02d}:{minutes % 60:02d}"

# ==================== ХРАНЕНИЕ ====================
def load_subscribers() -> dict:
    try:
        with open(SUBSCRIBERS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def save_subscribers(data: dict):
    with open(SUBSCRIBERS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def normalize_user_sub(data: Optional[dict]) -> dict:
    base = {
        "city": None,
        "all_online": False,
        "all_live": False,
        "groups": {},
        "reminder_minutes": [60],
        "meta": {
            "last_daily_sent": None,
            "last_reminders": {},
        },
    }
    if not isinstance(data, dict):
        return base
    base.update(data)
    if not isinstance(base.get("groups"), dict):
        base["groups"] = {}
    if not isinstance(base.get("reminder_minutes"), list):
        base["reminder_minutes"] = [60]
    if not base["reminder_minutes"]:
        base["reminder_minutes"] = [60]
    if not isinstance(base.get("meta"), dict):
        base["meta"] = {}
    if not isinstance(base["meta"].get("last_reminders"), dict):
        base["meta"]["last_reminders"] = {}
    if "last_daily_sent" not in base["meta"]:
        base["meta"]["last_daily_sent"] = None
    return base

def get_user_sub(uid: str) -> dict:
    subs = load_subscribers()
    return normalize_user_sub(subs.get(uid))

def set_user_sub(uid: str, data: dict):
    subs = load_subscribers()
    subs[uid] = normalize_user_sub(data)
    save_subscribers(subs)

def remove_subscriber(uid: str):
    subs = load_subscribers()
    subs.pop(uid, None)
    save_subscribers(subs)

# ==================== ФОРМАТИРОВАНИЕ ====================
def escape_html(text: str) -> str:
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

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

def format_online_group(time_str: str, name: str, url: str) -> str:
    return f'🟠 <b>{time_str}</b> — <a href="{url}">{escape_html(name)}</a>'

def format_live_group(name: str, address: str, start: str, end: str, is_work_meeting: bool = False) -> str:
    label = " 🔧" if is_work_meeting else ""
    return f"• <b>{escape_html(name)}</b>{label} — {escape_html(address)} ({start}–{end})"

# ==================== CALLBACK SAFE ====================
async def safe_callback_answer(callback: CallbackQuery, text: str = "", show_alert: bool = False):
    try:
        await callback.answer(text, show_alert=show_alert)
    except Exception:
        pass

async def safe_edit_text(message: Message, text: str, **kwargs):
    try:
        current_text = message.html_text or message.text or ""
    except Exception:
        current_text = ""
    new_markup = kwargs.get("reply_markup")
    current_markup = message.reply_markup if hasattr(message, "reply_markup") else None
    if current_text == text and current_markup == new_markup:
        return
    try:
        await message.edit_text(text, **kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise

# ==================== ДАННЫЕ ====================
reply_main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🌐 Онлайн"), KeyboardButton(text="🏙 Живые"), KeyboardButton(text="🔔 Мои подписки")],
    ],
    resize_keyboard=True,
    is_persistent=True,
)

DAYS = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
SHORT_DAYS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

ONLINE_SCHEDULE = {
    0: [("05:00","Восход","https://t.me/+gdi_B_ctmVJkMTAy"),("07:00","ВДА Утро","https://t.me/+KBt9VaElvMA4NTcy"),("07:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("08:00","Единство утро","https://t.me/ACAgroupUnityMoscow"),("08:00","Говори Доверяй Чувствуй","https://t.me/govori_vda"),("09:00","Доверие","https://t.me/VDADoverie"),("12:00","День за днём","https://t.me/+BwAsiX1KsGljZjQy"),("17:00","Шаг за шагом","https://t.me/joinchat/4SFNdPrxumNkYzky"),("18:00","Весна","https://t.me/vdavesna_2021"),("19:00","Рассвет","https://t.me/+OOw9IMnM5x1hNDJi"),("19:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("21:00","ДЫШИ!","https://t.me/breathelivebe"),("21:00","ВДА ВЕЧЕР","https://t.me/vda_vecher"),("21:00","Свобода","https://t.me/vda_svoboda")],
    1: [("05:00","Восход","https://t.me/+gdi_B_ctmVJkMTAy"),("07:00","ВДА Утро","https://t.me/+KBt9VaElvMA4NTcy"),("07:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("09:00","Доверие","https://t.me/VDADoverie"),("12:00","День за днём","https://t.me/+BwAsiX1KsGljZjQy"),("15:00","ВДА вокруг света","https://t.me/+nFn14RqYkyozZmUy"),("18:00","Весна","https://t.me/vdavesna_2021"),("19:00","Артплей","https://t.me/VDAartPlay"),("19:00","Рассвет","https://t.me/+OOw9IMnM5x1hNDJi"),("19:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("20:00","Феникс","https://t.me/+1GAp8vi4hyNmMzUy"),("20:00","По шагам Тони А.","https://t.me/+ajasg4oH0SU3MjFi"),("21:00","ДЫШИ!","https://t.me/breathelivebe"),("21:00","ВДА ВЕЧЕР","https://t.me/vda_vecher"),("21:00","Свобода","https://t.me/vda_svoboda")],
    2: [("05:00","Восход","https://t.me/+gdi_B_ctmVJkMTAy"),("07:00","ВДА Утро","https://t.me/+KBt9VaElvMA4NTcy"),("07:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("08:00","Единство утро","https://t.me/ACAgroupUnityMoscow"),("09:00","Доверие","https://t.me/VDADoverie"),("12:00","День за днём","https://t.me/+BwAsiX1KsGljZjQy"),("14:00","Венеция","https://t.me/joinchat/AocB9y6QC_k2ZjJi"),("18:00","Весна","https://t.me/vdavesna_2021"),("19:00","Рассвет","https://t.me/+OOw9IMnM5x1hNDJi"),("19:00","Вместе","https://chat.whatsapp.com/0CvEyMffhB60ZHQcShdva7"),("19:00","Точка Опоры","https://us06web.zoom.us/j/88678026186?pwd=QYiLNtlro6gEZ3f6eVZdwu7CAHbVF3.1"),("19:00","Светский круг (жен.)","https://t.me/+Pyarr0R7MSEyMGIy"),("19:00","ВДА-ВЕРА","https://t.me/+J2m1MAbQ818zNTFi"),("19:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("19:30","Эффект бабочки","https://t.me/+FcaUkHDOuMpkMTI8"),("20:00","Мужская ВДА","https://t.me/+ewtjezZaCtM5YTdi"),("20:00","Доверие (вопросы)","https://t.me/VDADoverie"),("21:00","ДЫШИ!","https://t.me/breathelivebe"),("21:00","ВДА ВЕЧЕР","https://t.me/vda_vecher"),("21:00","Свобода","https://t.me/vda_svoboda"),("22:00","Восст. Люб. Род.","https://us02web.zoom.us/j/86893102645?pwd=d2N1UWFDY3Y5RXBpTUdQcWpDdEZVUT09UT09")],
    3: [("05:00","Восход","https://t.me/+gdi_B_ctmVJkMTAy"),("07:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("07:00","ВДА Утро","https://t.me/+KBt9VaElvMA4NTcy"),("09:00","Доверие","https://t.me/VDADoverie"),("12:00","День за днём","https://t.me/+BwAsiX1KsGljZjQy"),("14:00","Венеция","https://t.me/joinchat/AocB9y6QC_k2ZjJi"),("18:00","Весна","https://t.me/vdavesna_2021"),("19:00","праВДА","https://t.me/+ZYfdfXWBRltjZGEy"),("19:00","ВДА в Рязани","https://t.me/+MHSRTpkJliw5YzUy"),("19:00","Артплей (онлайн)","https://t.me/VDAartPlay"),("19:00","Рассвет","https://t.me/+OOw9IMnM5x1hNDJi"),("19:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("21:00","ДЫШИ!","https://t.me/breathelivebe"),("21:00","ВДА ВЕЧЕР","https://t.me/vda_vecher"),("21:00","Свобода","https://t.me/vda_svoboda")],
    4: [("05:00","Восход","https://t.me/+gdi_B_ctmVJkMTAy"),("07:00","ВДА Утро","https://t.me/+KBt9VaElvMA4NTcy"),("07:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("08:00","Говори Доверяй Чувствуй","https://t.me/govori_vda"),("08:00","Единство утро","https://t.me/ACAgroupUnityMoscow"),("09:00","Доверие","https://t.me/VDADoverie"),("12:00","День за днём","https://t.me/+BwAsiX1KsGljZjQy"),("14:00","Венеция","https://t.me/joinchat/AocB9y6QC_k2ZjJi"),("14:00","ВДА вокруг света","https://t.me/+nFn14RqYkyozZmUy"),("18:00","Весна","https://t.me/vdavesna_2021"),("19:00","Братский Круг","https://t.me/+uEG2E5FVndA0YTc6"),("19:00","Рассвет","https://t.me/+OOw9IMnM5x1hNDJi"),("19:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("20:00","Феникс","https://t.me/+1GAp8vi4hyNmMzUy"),("20:00","Доверие (Любящий Родитель)","https://t.me/VDADoverie"),("21:00","ДЫШИ!","https://t.me/breathelivebe"),("21:00","ВДА ВЕЧЕР","https://t.me/vda_vecher"),("21:00","Свобода","https://t.me/vda_svoboda")],
    5: [("05:00","Восход","https://t.me/+gdi_B_ctmVJkMTAy"),("08:00","ВДА Утро","https://t.me/+KBt9VaElvMA4NTcy"),("09:00","Доверие","https://t.me/VDADoverie"),("12:00","День за днём","https://t.me/+BwAsiX1KsGljZjQy"),("14:00","Венеция","https://t.me/joinchat/AocB9y6QC_k2ZjJi"),("18:00","ВДА «Весь мир»","https://t.me/+zfYoUgHPiVRhN2Iy"),("18:00","Весна","https://t.me/vdavesna_2021"),("19:00","Девчата","https://t.me/+FKs5HqhF711iZTli"),("19:00","ВДА-ВЕРА","https://t.me/+J2m1MAbQ818zNTFi"),("19:00","Рассвет","https://t.me/+OOw9IMnM5x1hNDJi"),("20:00","Восст. Люб. Род.","https://us02web.zoom.us/j/86893102645?pwd=d2N1UWFDY3Y5RXBpTUdQcWpDdEZVUT09UT09"),("21:00","ВДА ВЕЧЕР","https://t.me/vda_vecher"),("21:00","Свобода","https://t.me/vda_svoboda")],
    6: [("05:00","Восход","https://t.me/+gdi_B_ctmVJkMTAy"),("07:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("08:00","ВДА Утро","https://t.me/+KBt9VaElvMA4NTcy"),("10:00","ВДА НСК онлайн","https://t.me/VDANsk"),("12:00","Только сегодня (MAX)","https://max.ru/join/y25EwyRl_K_F1OeJv5JbewpRpww71IKfWS-gwtTB65Q"),("12:00","День за днём","https://t.me/+BwAsiX1KsGljZjQy"),("12:30","Мужская ВДА","https://t.me/+ewtjezZaCtM5YTdi"),("14:00","Венеция","https://t.me/joinchat/AocB9y6QC_k2ZjJi"),("18:00","Ежедневник ВДА","https://t.me/VDAOXOTNIRYAD"),("18:00","Весна","https://t.me/vdavesna_2021"),("19:00","Сила и Надежда (Watsup)","https://chat.whatsapp.com/CUc0VVemIvl7Aoe2cuYCav"),("19:00","Рассвет","https://t.me/+OOw9IMnM5x1hNDJi"),("19:30","Эффект бабочки","https://t.me/+FcaUkHDOuMpkMTI8"),("20:00","Огоньки","https://t.me/ogonki2025"),("20:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("21:00","ВДА ВЕЧЕР","https://t.me/vda_vecher"),("21:00","Свобода","https://t.me/vda_svoboda")],
}

SLOGANS_AND_AFFIRMATIONS = [
    "Программа простая, но не лёгкая", "Жизнь больше, чем просто выживание", "Можно жить по-другому", "Только сегодня",
    "Не суетись", "Не усложняй", "Прогресс, а не совершенство", "Первым делом — главное", "И эта боль тоже пройдет",
    "Отпусти. Пусти Бога", "Стоп — не будь Голодным, Злым, Одиноким и Уставшим", "Возвращайтесь снова и снова",
    "Назови, но не обвиняй", "Попроси о помощи и прими её", "Без чувств нет исцеления",
    "Сегодня я люблю и принимаю себя таким, какой я есть", "Сегодня я принимаю свои чувства",
    "Сегодня я делюсь своими чувствами", "Сегодня я позволяю себе совершать ошибки",
    "Сегодня мне достаточно того, кто я есть", "Сегодня я принимаю тебя таким, какой ты есть",
    "Сегодня я позволю жить другим", "Сегодня я попрошу мою Высшую Силу о поддержке и руководстве мной",
    "Сегодня я не стану обвинять ни тебя, ни себя", "Сегодня я имею право оберегать свои мысли, чувства и заботиться о своём теле",
    "Сегодня я смогу сказать «Нет» без чувства вины", "Сегодня я смогу сказать «Да» без чувства стыда",
    "Сегодня я желанный ребёнок любящих родителей", "Нормально знать, кто я есть", "Нормально доверять себе",
    "Нормально сказать: я взрослый ребёнок из дисфункциональной семьи", "Нормально знать другой способ жить",
    "Нормально отказывать без чувства вины", "Нормально дать себе передышку", "Нормально плакать от фильма или песни",
    "Мои чувства нормальны, даже если я их только учусь различать", "Нормально злиться", "Нормально веселиться и праздновать",
    "Нормально мечтать и надеяться", "Нормально отделяться с любовью", "Нормально заново учиться заботиться о себе",
    "Нормально сказать: я люблю себя", "Нормально работать по программе ВДА",
]

# ==================== ЖИВЫЕ ГРУППЫ ====================
def parse_live_schedule(raw_lines: str):
    groups = []
    for line in raw_lines.strip().split("\n"):
        if not line.strip():
            continue
        parts = [c.strip() for c in line.split("\t")]
        if len(parts) < 5:
            continue
        country, city, name, address, time_str = parts[:5]
        if country != "Россия":
            continue
        time_str = time_str.replace('"', '').replace('\\n', ' ')
        entries = re.split(r";|\n| и ", time_str)
        days = []
        for entry in entries:
            entry = entry.strip()
            if not entry:
                continue
            day_found = None
            for i, day_name in enumerate(DAYS):
                if day_name.lower() in entry.lower() or day_name[:3].lower() in entry.lower():
                    day_found = i
                    break
            if day_found is None:
                short_map = {
                    "пн": 0, "пон": 0, "вт": 1, "вто": 1, "ср": 2, "сре": 2,
                    "чт": 3, "чет": 3, "пт": 4, "пят": 4, "сб": 5, "суб": 5,
                    "вс": 6, "вос": 6,
                }
                for key, idx in short_map.items():
                    if key in entry.lower():
                        day_found = idx
                        break
            if day_found is None:
                continue
            occurrence = None
            e = entry.lower()
            if "последн" in e:
                occurrence = "last"
            elif "перв" in e:
                occurrence = 1
            elif "втор" in e and "вторник" not in e:
                occurrence = 2
            elif "треть" in e:
                occurrence = 3
            elif "четверт" in e and "четверг" not in e:
                occurrence = 4
            is_work_meeting = "рабоч" in e or "рабочка" in e
            times = re.findall(r"(\d{1,2}[.:]\d{2})\s*[-–]\s*(\d{1,2}[.:]\d{2})", entry)
            if not times:
                times = re.findall(r"с\s*(\d{1,2}[.:\-]\d{2})\s*до\s*(\d{1,2}[.:\-]\d{2})", entry)
            if not times:
                single = re.findall(r"в\s*(\d{1,2}[.:]\d{2})", entry)
                if not single:
                    single = re.findall(r"(?<!\d)(\d{1,2}[.:]\d{2})(?!\d)", entry)
                if single:
                    start = single[0].replace(".", ":")
                    h, m = start.split(":")
                    end_h = int(h) + 1
                    end = f"{end_h:02d}:{m}"
                    times = [(start, end)]
            if times:
                start, end = times[0]
                start = start.replace(".", ":").replace("-", ":")
                end = end.replace(".", ":").replace("-", ":")
                days.append({
                    "day": day_found,
                    "start": start,
                    "end": end,
                    "occurrence": occurrence,
                    "is_work_meeting": is_work_meeting,
                })
        if days:
            groups.append({
                "city": city.strip(),
                "name": name.strip(),
                "address": address.strip(),
                "days": days,
            })
    return groups

try:
    with open("live_groups.tsv", "r", encoding="utf-8") as f:
        raw_excel = f.read()
except FileNotFoundError:
    print("❌ live_groups.tsv не найден")
    raw_excel = ""

LIVE_GROUPS = parse_live_schedule(raw_excel)

POPULAR_CITIES = [
    "Москва", "Санкт-Петербург", "Ростов-на-Дону", "Казань", "Новосибирск", "Екатеринбург",
    "Самара", "Нижний Новгород", "Краснодар", "Омск", "Калининград", "Воронеж",
    "Челябинск", "Красноярск", "Уфа", "Тюмень", "Ижевск", "Тольятти", "Хабаровск", "Иркутск"
]

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

SPB_SUBURBS = [
    "Санкт-Петербург, город Пушкин", "Пушкин", "Петергоф", "Всеволожск", "Выборг", "Гатчина",
    "Колпино", "Кронштадт", "Ломоносов", "Павловск", "Сестрорецк", "Зеленогорск"
]

# ==================== ID ГРУПП ====================
def make_short_id(prefix: str, name: str) -> str:
    return prefix + hashlib.md5(name.encode("utf-8")).hexdigest()[:10]

ONLINE_GROUP_ID_TO_NAME: Dict[str, str] = {}
LIVE_GROUP_ID_TO_NAME: Dict[str, str] = {}

all_online_names = sorted({name for day_groups in ONLINE_SCHEDULE.values() for _, name, _ in day_groups})
for name in all_online_names:
    ONLINE_GROUP_ID_TO_NAME[make_short_id("o", name)] = name

all_live_names = sorted({g["name"] for g in LIVE_GROUPS})
for name in all_live_names:
    LIVE_GROUP_ID_TO_NAME[make_short_id("l", name)] = name

# ==================== ПОИСК И РАСПИСАНИЯ ====================
def get_searchable_cities(query: str) -> list:
    query_lower = query.lower().strip()
    is_spb_search = query_lower in ("санкт-петербург", "спб", "питер", "петербург")
    matched, seen = [], set()
    for g in LIVE_GROUPS:
        city = g["city"]
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
        for g in LIVE_GROUPS:
            for suburb in SPB_SUBURBS:
                if query_lower in suburb.lower() and query_lower in g["city"].lower():
                    if g["city"] not in seen:
                        matched.append(g["city"])
                        seen.add(g["city"])
    return matched

def week_of_month(dt):
    return ((dt.day - 1) // 7) + 1

def is_last_weekday_of_month(dt):
    next_same = dt + timedelta(days=7)
    return next_same.month != dt.month

def day_entry_matches_date(day_entry, target_date):
    if day_entry["day"] != target_date.weekday():
        return False
    occurrence = day_entry.get("occurrence")
    if occurrence is None:
        return True
    if occurrence == "last":
        return is_last_weekday_of_month(target_date)
    return week_of_month(target_date) == occurrence

def get_live_groups_for_city(city: str):
    city_lower = city.lower().strip()
    return [g for g in LIVE_GROUPS if city_lower in g["city"].lower()]

def get_live_groups_for_day(city: str, day_index: int):
    city_groups = get_live_groups_for_city(city)
    result = []
    today = moscow_now().date()
    target_date = today + timedelta(days=(day_index - today.weekday()) % 7)
    for g in city_groups:
        for entry in g["days"]:
            if day_entry_matches_date(entry, target_date):
                result.append((g["name"], g["address"], entry["start"], entry["end"], entry.get("is_work_meeting", False)))
    return sorted(result, key=lambda x: x[2])

def get_live_week(city: str):
    city_groups = get_live_groups_for_city(city)
    if not city_groups:
        return f"В городе «{escape_html(city)}» живых групп не найдено."
    parts = [f"🏙 <b>Живые группы в {escape_html(city)}:</b>"]
    today = moscow_now().date()
    for offset in range(7):
        target_date = today + timedelta(days=offset)
        day_name = DAYS[target_date.weekday()]
        day_groups = []
        for g in city_groups:
            for entry in g["days"]:
                if day_entry_matches_date(entry, target_date):
                    day_groups.append((g["name"], g["address"], entry["start"], entry["end"], entry.get("is_work_meeting", False)))
        if day_groups:
            parts.append(f"<b>{day_name} ({target_date.strftime('%d.%m')}):</b>")
            parts.extend(format_live_group(n, a, s, e, w) for n, a, s, e, w in sorted(day_groups, key=lambda x: x[2]))
    return "\n".join(parts)

def get_online_by_day(day_index: int):
    return sorted(ONLINE_SCHEDULE.get(day_index, []), key=lambda x: x[0])

# ==================== УВЕДОМЛЕНИЯ ====================
def get_today_online_subscriptions(user_data: dict):
    day_index = moscow_now().weekday()
    groups = []
    for time_str, name, url in get_online_by_day(day_index):
        if name in user_data.get("groups", {}) and user_data["groups"][name].get("type") == "online":
            groups.append((time_str, name, url))
    return groups

def get_today_live_subscriptions(user_data: dict):
    city = user_data.get("city")
    if not city:
        return []
    day_index = moscow_now().weekday()
    groups = []
    for name, address, start, end, is_work_meeting in get_live_groups_for_day(city, day_index):
        if name in user_data.get("groups", {}) and user_data["groups"][name].get("type") == "live":
            groups.append((name, address, start, end, is_work_meeting))
    return groups

def build_daily_message(uid: str, user_data: dict) -> Optional[str]:
    day_index = moscow_now().weekday()
    online_groups = get_today_online_subscriptions(user_data)
    live_groups = get_today_live_subscriptions(user_data)
    if not online_groups and not live_groups:
        return None
    parts = [f"☀ <b>Ваши группы на сегодня ({DAYS[day_index]}):</b>"]
    if online_groups:
        parts.append("\n<b>🌐 Онлайн:</b>")
        for t, n, u in online_groups:
            parts.append(f'🟠 <b>{t}</b> — <a href="{u}">{escape_html(n)}</a>')
    if live_groups:
        parts.append("\n<b>🏙 Живые:</b>")
        for n, a, s, e, w in live_groups:
            label = " 🔧" if w else ""
            parts.append(f"• <b>{escape_html(n)}</b>{label} — {escape_html(a)} ({s}–{e})")
    return "\n".join(parts)

def build_reminder_key(group_type: str, group_name: str, date_str: str, time_str: str, reminder_minutes: int) -> str:
    return f"{group_type}|{group_name}|{date_str}|{time_str}|{reminder_minutes}"

def collect_due_reminders(user_data: dict, now_dt: datetime):
    groups = user_data.get("groups", {})
    if not groups:
        return []

    reminders_meta = user_data.setdefault("meta", {}).setdefault("last_reminders", {})
    reminder_minutes_list = user_data.get("reminder_minutes", [60])
    today_str = now_dt.strftime("%Y-%m-%d")
    now_minutes = now_dt.hour * 60 + now_dt.minute

    due_by_start: Dict[int, List[Tuple[str, str, bool, str]]] = {}

    for time_str, name, url in get_online_by_day(now_dt.weekday()):
        if name not in groups or groups[name].get("type") != "online":
            continue
        start_minutes = time_to_minutes(time_str)
        for r_min in reminder_minutes_list:
            if start_minutes - now_minutes == r_min:
                key = build_reminder_key("online", name, today_str, time_str, r_min)
                if reminders_meta.get(key) == today_str:
                    continue
                details = f'<a href="{url}">{escape_html(name)}</a>'
                if start_minutes not in due_by_start:
                    due_by_start[start_minutes] = []
                due_by_start[start_minutes].append(("online", name, details, key, url))
                break

    city = user_data.get("city")
    if city:
        for name, address, start, end, is_work_meeting in get_live_groups_for_day(city, now_dt.weekday()):
            if name not in groups or groups[name].get("type") != "live":
                continue
            start_minutes = time_to_minutes(start)
            for r_min in reminder_minutes_list:
                if start_minutes - now_minutes == r_min:
                    key = build_reminder_key("live", name, today_str, start, r_min)
                    if reminders_meta.get(key) == today_str:
                        continue
                    label = " 🔧" if is_work_meeting else ""
                    details = f'🏙 <b>{escape_html(name)}</b>{label}\n   📍 {escape_html(address)}'
                    if start_minutes not in due_by_start:
                        due_by_start[start_minutes] = []
                    due_by_start[start_minutes].append(("live", name, details, key, address))
                    break

    result = []
    for start_minutes, entries in due_by_start.items():
        time_label = f"в {minutes_to_time(start_minutes)}"
        r_min_val = start_minutes - now_minutes
        if r_min_val == 60:
            time_text = "за час"
        elif r_min_val == 120:
            time_text = "за два часа"
        else:
            time_text = f"за {r_min_val // 60} ч"

        online_lines = []
        live_lines = []
        keys = []
        has_disabled_preview = False
        for gtype, gname, detail, key, extra in entries:
            keys.append(key)
            if gtype == "online":
                online_lines.append(detail)
                if not has_disabled_preview:
                    has_disabled_preview = True
            else:
                live_lines.append(detail)

        intro = f"🕊 Привет, это бережное напоминание: {time_text} начнётся"
        if online_lines:
            online_part = "\n".join(online_lines)
            if len(online_lines) == 1:
                intro += f" онлайн-группа:\n{online_part}"
            else:
                intro += f" онлайн-группы:\n{online_part}"
        if live_lines:
            live_part = "\n".join(live_lines)
            if online_lines:
                if len(live_lines) == 1:
                    intro += "\nа также живая группа:\n" + live_part
                else:
                    intro += "\nа также живые группы:\n" + live_part
            else:
                if len(live_lines) == 1:
                    intro += f" живая группа:\n{live_part}"
                else:
                    intro += f" живые группы:\n{live_part}"

        result.append((keys, intro, has_disabled_preview))

    return result

def cleanup_old_reminders(user_data: dict, now_dt: datetime):
    meta = user_data.setdefault("meta", {})
    reminders = meta.setdefault("last_reminders", {})
    cutoff = (now_dt.date() - timedelta(days=3)).strftime("%Y-%m-%d")
    keys_to_delete = [k for k, v in reminders.items() if isinstance(v, str) and v < cutoff]
    for key in keys_to_delete:
        del reminders[key]

async def send_daily_notifications(bot: Bot, now_dt: datetime):
    subs = load_subscribers()
    today_str = now_dt.strftime("%Y-%m-%d")
    changed = False
    for uid, raw_data in subs.items():
        data = normalize_user_sub(raw_data)
        if not data.get("groups"):
            subs[uid] = data
            continue
        if data.setdefault("meta", {}).get("last_daily_sent") == today_str:
            subs[uid] = data
            continue
        message_text = build_daily_message(uid, data)
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

async def send_hourly_reminders(bot: Bot, now_dt: datetime):
    subs = load_subscribers()
    changed = False
    for uid, raw_data in subs.items():
        data = normalize_user_sub(raw_data)
        cleanup_old_reminders(data, now_dt)
        due_reminders = collect_due_reminders(data, now_dt)
        if not due_reminders:
            subs[uid] = data
            continue
        for keys, text, disable_preview in due_reminders:
            try:
                await bot.send_message(int(uid), text, parse_mode="HTML", disable_web_page_preview=disable_preview)
                for key in keys:
                    data["meta"]["last_reminders"][key] = now_dt.strftime("%Y-%m-%d")
                changed = True
            except Exception as e:
                print(f"❌ Не удалось отправить reminder пользователю {uid}: {e}")
        subs[uid] = data
    if changed:
        save_subscribers(subs)

async def notifications_worker(bot: Bot):
    print("✅ Планировщик уведомлений запущен")
    while True:
        try:
            now_dt = moscow_now().replace(second=0, microsecond=0)
            if now_dt.hour == DAILY_SEND_HOUR and now_dt.minute == DAILY_SEND_MINUTE:
                await send_daily_notifications(bot, now_dt)
            await send_hourly_reminders(bot, now_dt)
        except Exception as e:
            print(f"❌ Ошибка в notifications_worker: {e}")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)

# ==================== РЕНДЕРИНГ ====================
def build_day_selector(current_day: int, prefix: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for i, short in enumerate(SHORT_DAYS):
        label = f"[{short}]" if i == current_day else short
        builder.button(text=label, callback_data=f"{prefix}{i}")
    builder.adjust(7)
    return builder.as_markup()

def render_online_day(day_index: int, uid: str) -> Tuple[str, InlineKeyboardMarkup]:
    groups = get_online_by_day(day_index)
    data = get_user_sub(uid)
    today = moscow_now().weekday()
    is_today = (day_index == today)
    title = f"🌐 <b>Онлайн — {DAYS[day_index]}</b>"
    if is_today:
        title = f"🌐 <b>Онлайн — Сегодня ({DAYS[day_index]})</b>"
    lines = [title, "", "🔔 — подписка | ℹ️ — информация", ""]
    if groups:
        for t, n, u in groups:
            subbed = n in data.get("groups", {})
            icon = "🔕" if subbed else "🔔"
            lines.append(f'{icon} <b>{t}</b> — <a href="{u}">{escape_html(n)}</a>')
    else:
        lines.append("Нет групп.")

    builder = InlineKeyboardBuilder()
    prev_day = (day_index - 1) % 7
    next_day = (day_index + 1) % 7
    builder.row(
        InlineKeyboardButton(text="◀", callback_data=f"online_day_{prev_day}"),
        InlineKeyboardButton(text="▶", callback_data=f"online_day_{next_day}"),
    )
    days_row = build_day_selector(day_index, "online_day_")
    builder.attach(InlineKeyboardBuilder.from_markup(days_row))

    for t, n, u in groups:
        gid = make_short_id("o", n)
        subbed = n in data.get("groups", {})
        bell = "🔕" if subbed else "🔔"
        builder.row(
            InlineKeyboardButton(text=bell, callback_data=f"toggle_online_{gid}_{day_index}"),
            InlineKeyboardButton(text="ℹ️", callback_data=f"info_online_{gid}"),
        )

    builder.row(InlineKeyboardButton(text="📋 Вся неделя", callback_data="online_week"))
    return "\n".join(lines), builder.as_markup()

def render_live_day(city: str, day_index: int, uid: str) -> Tuple[str, InlineKeyboardMarkup]:
    groups = get_live_groups_for_day(city, day_index)
    data = get_user_sub(uid)
    cid = city_to_id.get(city, city)
    today = moscow_now().weekday()
    is_today = (day_index == today)
    title = f"🏙 <b>{escape_html(city)} — {DAYS[day_index]}</b>"
    if is_today:
        title = f"🏙 <b>{escape_html(city)} — Сегодня ({DAYS[day_index]})</b>"
    lines = [title, "", "🔔 — подписка | ℹ️ — информация", ""]
    if groups:
        for n, a, s, e, w in groups:
            subbed = n in data.get("groups", {})
            icon = "🔕" if subbed else "🔔"
            label = " 🔧" if w else ""
            lines.append(f'{icon} <b>{escape_html(n)}</b>{label} — {escape_html(a)} ({s}–{e})')
    else:
        lines.append("Нет групп.")

    builder = InlineKeyboardBuilder()
    prev_day = (day_index - 1) % 7
    next_day = (day_index + 1) % 7
    builder.row(
        InlineKeyboardButton(text="◀", callback_data=f"live_day_{cid}_{prev_day}"),
        InlineKeyboardButton(text="▶", callback_data=f"live_day_{cid}_{next_day}"),
    )
    days_row = build_day_selector(day_index, f"live_day_{cid}_")
    builder.attach(InlineKeyboardBuilder.from_markup(days_row))

    for n, a, s, e, w in groups:
        gid = make_short_id("l", n)
        subbed = n in data.get("groups", {})
        bell = "🔕" if subbed else "🔔"
        builder.row(
            InlineKeyboardButton(text=bell, callback_data=f"toggle_live_{cid}_{gid}_{day_index}"),
            InlineKeyboardButton(text="ℹ️", callback_data=f"info_live_{gid}"),
        )

    builder.row(InlineKeyboardButton(text="📋 Вся неделя", callback_data=f"live_week_{cid}"))
    builder.row(InlineKeyboardButton(text="← Назад", callback_data="live_city_list"))
    return "\n".join(lines), builder.as_markup()

def render_subscriptions(uid: str) -> Tuple[str, InlineKeyboardMarkup]:
    data = get_user_sub(uid)
    groups = data.get("groups", {})
    reminder_minutes = data.get("reminder_minutes", [60])
    if 60 in reminder_minutes and 120 in reminder_minutes:
        reminder_text = "⏰ Напоминания: за 1 ч и за 2 ч"
    elif 120 in reminder_minutes:
        reminder_text = "⏰ Напоминания: за 2 ч"
    else:
        reminder_text = "⏰ Напоминания: за 1 ч"

    text = "🔔 <b>Мои подписки</b>\n\n"
    if not groups:
        text += "Нет активных подписок.\nПодпишитесь на группы в разделах «Онлайн» или «Живые»."
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="🌐 Онлайн", callback_data="goto_online"),
            InlineKeyboardButton(text="🏙 Живые", callback_data="goto_live"),
        )
        builder.row(InlineKeyboardButton(text=reminder_text, callback_data="change_reminder"))
        return text, builder.as_markup()

    builder = InlineKeyboardBuilder()
    for name, gdata in groups.items():
        gtype = gdata.get("type", "online")
        emoji = "🌐" if gtype == "online" else "🏙"
        gid = make_short_id("o" if gtype == "online" else "l", name)
        builder.row(InlineKeyboardButton(
            text=f"🔕 Отписаться от {emoji} «{name}»",
            callback_data=f"unsub_{gtype}_{gid}"
        ))

    city = data.get("city")
    if city:
        builder.row(InlineKeyboardButton(text=f"🏙 Сменить город ({city})", callback_data="sub_change_city"))
    else:
        builder.row(InlineKeyboardButton(text="🏙 Выбрать город (для живых)", callback_data="sub_change_city"))

    builder.row(InlineKeyboardButton(text=reminder_text, callback_data="change_reminder"))
    builder.row(InlineKeyboardButton(text="🔕 Отписаться от всего", callback_data="unsub_all"))
    builder.row(
        InlineKeyboardButton(text="🌐 Онлайн", callback_data="goto_online"),
        InlineKeyboardButton(text="🏙 Живые", callback_data="goto_live"),
    )
    return text, builder.as_markup()

# ==================== ДИСПЕТЧЕР ====================
dp = Dispatcher(storage=MemoryStorage())

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer("🕊 <b>Добро пожаловать в бот ВДА!</b>", parse_mode="HTML", reply_markup=reply_main_menu)

@dp.message(F.text == "🌐 Онлайн")
async def show_online(message: Message):
    uid = str(message.from_user.id)
    day_index = moscow_now().weekday()
    text, markup = render_online_day(day_index, uid)
    await message.answer(text, parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True)

@dp.message(F.text == "🏙 Живые")
async def show_live(message: Message):
    uid = str(message.from_user.id)
    data = get_user_sub(uid)
    city = data.get("city")
    if city:
        day_index = moscow_now().weekday()
        text, markup = render_live_day(city, day_index, uid)
        await message.answer(text, parse_mode="HTML", reply_markup=markup)
    else:
        builder = InlineKeyboardBuilder()
        for c in POPULAR_CITIES:
            cid = city_to_id.get(c, c)
            builder.button(text=c, callback_data=f"live_city_{cid}")
        builder.adjust(2)
        builder.row(InlineKeyboardButton(text="🔍 Найти город", callback_data="live_search_city"))
        await message.answer("🏙 Выберите город:", reply_markup=builder.as_markup())

@dp.message(F.text == "🔔 Мои подписки")
async def show_subscriptions(message: Message):
    uid = str(message.from_user.id)
    text, markup = render_subscriptions(uid)
    await message.answer(text, parse_mode="HTML", reply_markup=markup)

@dp.message(Command("slogan"))
async def cmd_slogan(message: Message):
    slogan = random.choice(SLOGANS_AND_AFFIRMATIONS)
    await message.answer(f"💫 <b>Установка:</b>\n\n<i>«{escape_html(slogan)}»</i>", parse_mode="HTML")

# --- Онлайн ---
@dp.callback_query(F.data.startswith("online_day_"))
async def online_day_callback(callback: CallbackQuery):
    day_index = int(callback.data.split("_")[-1])
    uid = str(callback.from_user.id)
    text, markup = render_online_day(day_index, uid)
    await safe_edit_text(callback.message, text, parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True)
    await safe_callback_answer(callback)

@dp.callback_query(F.data == "online_week")
async def online_week_callback(callback: CallbackQuery):
    full = []
    for i in range(7):
        groups = get_online_by_day(i)
        if groups:
            lines = [f"<b>{DAYS[i]}:</b>"]
            lines.extend(format_online_group(t, n, u) for t, n, u in groups)
            full.append("\n".join(lines))
    text = "\n\n".join(full) if full else "Онлайн-групп нет."
    parts = split_long_message(text)
    await callback.message.edit_text("📋 Полное расписание:")
    for idx, part in enumerate(parts):
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="online_week_back")]]) if idx == len(parts) - 1 else None
        await callback.message.answer(part, parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb)
    await safe_callback_answer(callback)

@dp.callback_query(F.data == "online_week_back")
async def online_week_back(callback: CallbackQuery):
    await show_online(callback.message)
    await safe_callback_answer(callback)

@dp.callback_query(F.data.startswith("toggle_online_"))
async def toggle_online(callback: CallbackQuery):
    parts = callback.data.split("_")
    gid = parts[2]
    day_index = int(parts[3])
    group_name = ONLINE_GROUP_ID_TO_NAME.get(gid)
    if not group_name:
        await safe_callback_answer(callback, "Ошибка")
        return
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    groups = data.setdefault("groups", {})
    if group_name in groups:
        del groups[group_name]
    else:
        url = ""
        for dg in ONLINE_SCHEDULE.values():
            for t, n, u in dg:
                if n == group_name:
                    url = u
                    break
        groups[group_name] = {"type": "online", "info": {"url": url}}
    set_user_sub(uid, data)
    text, markup = render_online_day(day_index, uid)
    await safe_edit_text(callback.message, text, parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True)
    await safe_callback_answer(callback, "Обновлено")

@dp.callback_query(F.data.startswith("info_online_"))
async def info_online(callback: CallbackQuery):
    gid = callback.data[len("info_online_"):]
    group_name = ONLINE_GROUP_ID_TO_NAME.get(gid)
    if not group_name:
        await safe_callback_answer(callback, "Информация не найдена")
        return
    url = ""
    for dg in ONLINE_SCHEDULE.values():
        for t, n, u in dg:
            if n == group_name:
                times = t
                url = u
                break
    text = f"🌐 <b>{escape_html(group_name)}</b>\nВремя: {times}\nСсылка: {url}"
    await safe_callback_answer(callback, text, show_alert=True)

# --- Живые ---
@dp.callback_query(F.data.startswith("live_city_"))
async def live_city_selected(callback: CallbackQuery):
    cid = callback.data[len("live_city_"):]
    city = id_to_city.get(cid, cid)
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    data["city"] = city
    set_user_sub(uid, data)
    day_index = moscow_now().weekday()
    text, markup = render_live_day(city, day_index, uid)
    await safe_edit_text(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await safe_callback_answer(callback)

@dp.callback_query(F.data == "live_city_list")
async def live_city_list_callback(callback: CallbackQuery):
    builder = InlineKeyboardBuilder()
    for c in POPULAR_CITIES:
        cid = city_to_id.get(c, c)
        builder.button(text=c, callback_data=f"live_city_{cid}")
    builder.adjust(2)
    builder.row(InlineKeyboardButton(text="🔍 Найти город", callback_data="live_search_city"))
    await safe_edit_text(callback.message, "🏙 Выберите город:", reply_markup=builder.as_markup())
    await safe_callback_answer(callback)

@dp.callback_query(F.data.startswith("live_day_"))
async def live_day_callback(callback: CallbackQuery):
    parts = callback.data.split("_")
    cid = parts[2]
    day_index = int(parts[3])
    city = id_to_city.get(cid, cid)
    uid = str(callback.from_user.id)
    text, markup = render_live_day(city, day_index, uid)
    await safe_edit_text(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await safe_callback_answer(callback)

@dp.callback_query(F.data.startswith("live_week_"))
async def live_week_callback(callback: CallbackQuery):
    cid = callback.data.split("_")[-1]
    city = id_to_city.get(cid, cid)
    text = get_live_week(city)
    await safe_edit_text(callback.message, text, parse_mode="HTML",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data=f"live_back_{cid}")]]))
    await safe_callback_answer(callback)

@dp.callback_query(F.data.startswith("live_back_"))
async def live_back_callback(callback: CallbackQuery):
    cid = callback.data.split("_")[-1]
    city = id_to_city.get(cid, cid)
    uid = str(callback.from_user.id)
    day_index = moscow_now().weekday()
    text, markup = render_live_day(city, day_index, uid)
    await safe_edit_text(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await safe_callback_answer(callback)

@dp.callback_query(F.data.startswith("toggle_live_"))
async def toggle_live(callback: CallbackQuery):
    parts = callback.data.split("_")
    cid = parts[2]
    gid = parts[3]
    day_index = int(parts[4])
    city = id_to_city.get(cid, cid)
    group_name = LIVE_GROUP_ID_TO_NAME.get(gid)
    if not group_name:
        await safe_callback_answer(callback, "Ошибка")
        return
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    groups = data.setdefault("groups", {})
    if group_name in groups:
        del groups[group_name]
    else:
        info = {}
        for g in LIVE_GROUPS:
            if g["name"] == group_name:
                info = {"city": city, "address": g["address"]}
                break
        groups[group_name] = {"type": "live", "info": info}
    set_user_sub(uid, data)
    text, markup = render_live_day(city, day_index, uid)
    await safe_edit_text(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await safe_callback_answer(callback, "Обновлено")

@dp.callback_query(F.data.startswith("info_live_"))
async def info_live(callback: CallbackQuery):
    gid = callback.data[len("info_live_"):]
    group_name = LIVE_GROUP_ID_TO_NAME.get(gid)
    if not group_name:
        await safe_callback_answer(callback, "Информация не найдена")
        return
    city = address = ""
    for g in LIVE_GROUPS:
        if g["name"] == group_name:
            city = g["city"]
            address = g["address"]
            break
    text = f"🏙 <b>{escape_html(group_name)}</b>\n📍 {escape_html(address)}\n🏙 {escape_html(city)}"
    await safe_callback_answer(callback, text, show_alert=True)

@dp.callback_query(F.data == "live_search_city")
async def live_search_city_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(LiveGroupSearch.waiting_for_city)
    await safe_edit_text(callback.message, "🔍 Введите город:",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="live_city_list")]]))
    await safe_callback_answer(callback)

@dp.message(StateFilter(LiveGroupSearch.waiting_for_city))
async def live_search_city_handle(message: Message, state: FSMContext):
    query = message.text.strip()
    matched = get_searchable_cities(query)
    await state.clear()
    if not matched:
        await message.answer("Город не найден.", reply_markup=reply_main_menu)
        return
    if len(matched) == 1:
        city = matched[0]
        uid = str(message.from_user.id)
        data = get_user_sub(uid)
        data["city"] = city
        set_user_sub(uid, data)
        day_index = moscow_now().weekday()
        text, markup = render_live_day(city, day_index, uid)
        await message.answer(text, parse_mode="HTML", reply_markup=markup)
    else:
        builder = InlineKeyboardBuilder()
        for c in matched[:20]:
            cid = city_to_id.get(c, c)
            builder.button(text=c, callback_data=f"live_city_{cid}")
        builder.adjust(1)
        builder.row(InlineKeyboardButton(text="← Назад", callback_data="live_city_list"))
        await message.answer("🔍 Уточните:", reply_markup=builder.as_markup())

# --- Мои подписки ---
@dp.callback_query(F.data.startswith("unsub_"))
async def unsub_group(callback: CallbackQuery):
    parts = callback.data.split("_")
    gtype = parts[1]
    gid = parts[2]
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    groups = data.get("groups", {})
    group_name = ONLINE_GROUP_ID_TO_NAME.get(gid) if gtype == "online" else LIVE_GROUP_ID_TO_NAME.get(gid)
    if group_name and group_name in groups:
        del groups[group_name]
        set_user_sub(uid, data)
        text, markup = render_subscriptions(uid)
        await safe_edit_text(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await safe_callback_answer(callback, "Отписаны")

@dp.callback_query(F.data == "unsub_all")
async def unsub_all(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    remove_subscriber(uid)
    text, _ = render_subscriptions(uid)
    await safe_edit_text(callback.message, text, parse_mode="HTML", reply_markup=None)
    await safe_callback_answer(callback, "Отписаны от всего")

@dp.callback_query(F.data == "sub_change_city")
async def sub_change_city(callback: CallbackQuery):
    builder = InlineKeyboardBuilder()
    for c in POPULAR_CITIES:
        cid = city_to_id.get(c, c)
        builder.button(text=c, callback_data=f"sub_city_{cid}")
    builder.adjust(2)
    builder.row(InlineKeyboardButton(text="🔍 Найти", callback_data="sub_city_search"))
    builder.row(InlineKeyboardButton(text="← Назад", callback_data="sub_back_to_list"))
    await safe_edit_text(callback.message, "🏙 Выберите город для живых групп:", reply_markup=builder.as_markup())
    await safe_callback_answer(callback)

@dp.callback_query(F.data.startswith("sub_city_"))
async def sub_city_selected(callback: CallbackQuery):
    cid = callback.data[len("sub_city_"):]
    city = id_to_city.get(cid, cid)
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    data["city"] = city
    set_user_sub(uid, data)
    text, markup = render_subscriptions(uid)
    await safe_edit_text(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await safe_callback_answer(callback, f"Город: {city}")

@dp.callback_query(F.data == "sub_city_search")
async def sub_city_search_callback(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SubCitySearch.waiting_for_city)
    await safe_edit_text(callback.message, "🔍 Введите город:",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="sub_change_city")]]))
    await safe_callback_answer(callback)

@dp.message(StateFilter(SubCitySearch.waiting_for_city))
async def sub_city_search_input(message: Message, state: FSMContext):
    query = message.text.strip()
    matched = get_searchable_cities(query)
    await state.clear()
    if not matched:
        await message.answer("Город не найден.")
        return
    city = matched[0]
    uid = str(message.from_user.id)
    data = get_user_sub(uid)
    data["city"] = city
    set_user_sub(uid, data)
    await message.answer(f"✅ Город: <b>{escape_html(city)}</b>", parse_mode="HTML")
    text, markup = render_subscriptions(uid)
    await message.answer(text, parse_mode="HTML", reply_markup=markup)

@dp.callback_query(F.data == "sub_back_to_list")
async def sub_back_to_list(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    text, markup = render_subscriptions(uid)
    await safe_edit_text(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await safe_callback_answer(callback)

@dp.callback_query(F.data == "goto_online")
async def goto_online(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    day_index = moscow_now().weekday()
    text, markup = render_online_day(day_index, uid)
    await safe_edit_text(callback.message, text, parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True)
    await safe_callback_answer(callback)

@dp.callback_query(F.data == "goto_live")
async def goto_live(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    city = data.get("city")
    if city:
        day_index = moscow_now().weekday()
        text, markup = render_live_day(city, day_index, uid)
        await safe_edit_text(callback.message, text, parse_mode="HTML", reply_markup=markup)
    else:
        builder = InlineKeyboardBuilder()
        for c in POPULAR_CITIES:
            cid = city_to_id.get(c, c)
            builder.button(text=c, callback_data=f"live_city_{cid}")
        builder.adjust(2)
        builder.row(InlineKeyboardButton(text="🔍 Найти город", callback_data="live_search_city"))
        await safe_edit_text(callback.message, "🏙 Выберите город:", reply_markup=builder.as_markup())
    await safe_callback_answer(callback)

# Изменение интервала напоминаний
@dp.callback_query(F.data == "change_reminder")
async def change_reminder(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    current = data.get("reminder_minutes", [60])
    builder = InlineKeyboardBuilder()
    if 60 in current and 120 in current:
        selected = "both"
    elif 120 in current:
        selected = "120"
    else:
        selected = "60"
    builder.row(InlineKeyboardButton(text=f"{'✅ ' if selected == '60' else ''}За час", callback_data="set_reminder_60"))
    builder.row(InlineKeyboardButton(text=f"{'✅ ' if selected == '120' else ''}За два часа", callback_data="set_reminder_120"))
    builder.row(InlineKeyboardButton(text=f"{'✅ ' if selected == 'both' else ''}За час и за два", callback_data="set_reminder_both"))
    builder.row(InlineKeyboardButton(text="← Назад", callback_data="sub_back_to_list"))
    await safe_edit_text(callback.message, "⏰ Выберите, за сколько времени до группы присылать напоминания:",
                         reply_markup=builder.as_markup())
    await safe_callback_answer(callback)

@dp.callback_query(F.data.startswith("set_reminder_"))
async def set_reminder(callback: CallbackQuery):
    val = callback.data.split("_")[-1]
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    if val == "both":
        data["reminder_minutes"] = [60, 120]
    elif val == "60":
        data["reminder_minutes"] = [60]
    elif val == "120":
        data["reminder_minutes"] = [120]
    set_user_sub(uid, data)
    text, markup = render_subscriptions(uid)
    await safe_edit_text(callback.message, text, parse_mode="HTML", reply_markup=markup)
    await safe_callback_answer(callback, "Настройки сохранены")

# --- Fallback ---
@dp.message()
async def fallback(message: Message):
    await message.answer("Используйте кнопки меню 👇", reply_markup=reply_main_menu)

# ==================== ЗАПУСК ====================
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