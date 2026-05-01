import asyncio
import hashlib
import json
import os
import random
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional

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


class LiveGroupSearch(StatesGroup):
    waiting_for_city = State()


class SubCitySearch(StatesGroup):
    waiting_for_city = State()


reply_main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="🌐 Онлайн"),
            KeyboardButton(text="🏙 Живые"),
            KeyboardButton(text="💫 Установка"),
        ],
        [
            KeyboardButton(text="🔔 Подписка"),
            KeyboardButton(text="⭐ Мои группы"),
            KeyboardButton(text="🔕 Отписаться"),
        ],
    ],
    resize_keyboard=True,
    is_persistent=True,
)

DAYS = [
    "Понедельник",
    "Вторник",
    "Среда",
    "Четверг",
    "Пятница",
    "Суббота",
    "Воскресенье",
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

SPB_SUBURBS = [
    "Санкт-Петербург",
    "Пушкин",
    "Петергоф",
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

def moscow_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=3)


def time_to_minutes(hhmm: str) -> int:
    h, m = map(int, hhmm.split(":"))
    return h * 60 + m


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
        "daily_hour": 7,
        "remind_before": [60],
        "meta": {"last_daily_sent": None, "last_reminders": {}},
        "online_settings": {"daily_hour": 7, "remind_before": [60]},
        "live_settings": {"daily_hour": 7, "remind_before": [60]},
        "quiet_mode_date": None,  # добавлено для тихого режима
    }
    if not isinstance(data, dict):
        return base
    for key in ("city", "all_online", "all_live", "groups", "meta", "daily_hour", "remind_before", "quiet_mode_date"):
        if key in data:
            base[key] = data[key]
    if "online_settings" in data and isinstance(data["online_settings"], dict):
        base["online_settings"] = data["online_settings"]
    else:
        base["online_settings"]["daily_hour"] = data.get("daily_hour", 7)
        base["online_settings"]["remind_before"] = data.get("remind_before", [60])
    if "live_settings" in data and isinstance(data["live_settings"], dict):
        base["live_settings"] = data["live_settings"]
    else:
        base["live_settings"]["daily_hour"] = data.get("daily_hour", 7)
        base["live_settings"]["remind_before"] = data.get("remind_before", [60])
    if not isinstance(base["meta"], dict):
        base["meta"] = {}
    if "last_daily_sent" not in base["meta"]:
        base["meta"]["last_daily_sent"] = None
    if "last_reminders" not in base["meta"]:
        base["meta"]["last_reminders"] = {}
    for settings in (base["online_settings"], base["live_settings"]):
        if isinstance(settings.get("remind_before"), int):
            settings["remind_before"] = [settings["remind_before"]]
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


def format_online_group_with_sub(time_str: str, name: str, url: str, uid: str) -> str:
    data = get_user_sub(uid)
    subscribed = data.get("all_online") or name in data.get("groups", {})
    bell = " 🔔" if subscribed else " 🔕"
    # Таймер
    now = moscow_now()
    today_str = now.strftime("%Y-%m-%d")
    try:
        group_dt = datetime.strptime(f"{today_str} {time_str}", "%Y-%m-%d %H:%M") + timedelta(hours=0)
    except ValueError:
        group_dt = now + timedelta(days=1)
    delta = group_dt - now
    timer = ""
    if delta.total_seconds() > 0 and now.date() == group_dt.date():
        hours, rem = divmod(delta.seconds, 3600)
        minutes = rem // 60
        if hours > 0:
            timer = f" ⏳ через {hours} ч {minutes} мин"
        else:
            timer = f" ⏳ через {minutes} мин"
    return f'🟠 <b>{time_str}</b> — <a href="{url}">{escape_html(name)}</a>{bell}{timer}'


def format_live_group(
    name: str,
    address: str,
    start: str,
    end: str,
    is_work_meeting: bool = False,
) -> str:
    label = " 🔧" if is_work_meeting else ""
    now = moscow_now()
    today_str = now.strftime("%Y-%m-%d")
    try:
        start_dt = datetime.strptime(f"{today_str} {start}", "%Y-%m-%d %H:%M") + timedelta(hours=0)
    except ValueError:
        start_dt = now + timedelta(days=1)
    delta = start_dt - now
    timer = ""
    if delta.total_seconds() > 0 and now.date() == start_dt.date():
        hours, rem = divmod(delta.seconds, 3600)
        minutes = rem // 60
        if hours > 0:
            timer = f" ⏳ через {hours} ч {minutes} мин"
        else:
            timer = f" ⏳ через {minutes} мин"
    return f"• {start}–{end} — {escape_html(name)}{label} — {escape_html(address)}{timer}"


def format_live_group_with_sub(
    name: str,
    address: str,
    start: str,
    end: str,
    is_work_meeting: bool,
    uid: str,
) -> str:
    label = " 🔧" if is_work_meeting else ""
    data = get_user_sub(uid)
    subscribed = data.get("all_live") or name in data.get("groups", {})
    bell = " 🔔" if subscribed else " 🔕"
    now = moscow_now()
    today_str = now.strftime("%Y-%m-%d")
    try:
        start_dt = datetime.strptime(f"{today_str} {start}", "%Y-%m-%d %H:%M") + timedelta(hours=0)
    except ValueError:
        start_dt = now + timedelta(days=1)
    delta = start_dt - now
    timer = ""
    if delta.total_seconds() > 0 and now.date() == start_dt.date():
        hours, rem = divmod(delta.seconds, 3600)
        minutes = rem // 60
        if hours > 0:
            timer = f" ⏳ через {hours} ч {minutes} мин"
        else:
            timer = f" ⏳ через {minutes} мин"
    return f"• {start}–{end} — {escape_html(name)}{label}{bell} — {escape_html(address)}{timer}"


def format_day_header(day_name: str, date_label: Optional[str] = None) -> str:
    day = escape_html(day_name)
    if date_label:
        return f"━━━ <b>{day}</b> · <code>{escape_html(date_label)}</code> ━━━"
    return f"━━━ <b>{day}</b> ━━━"


def back_markup(text: str, callback_data: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=text, callback_data=callback_data)]]
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
        rows = []
        for row in markup.inline_keyboard:
            rows.append([(btn.text, btn.callback_data, btn.url) for btn in row])
        return json.dumps(rows, ensure_ascii=False, sort_keys=True)
    except Exception:
        return repr(markup)


async def safe_edit_text(message: Message, text: str, **kwargs):
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
                    start = single[0].replace('.', ':').replace('-', ':')
                    h, m = start.split(':')
                    end = f"{(int(h) + 1):02d}:{m}"
                    times = [(start, end)]
            for start, end in times:
                days.append({
                    "day": day_found,
                    "start": start.replace('.', ':').replace('-', ':'),
                    "end": end.replace('.', ':').replace('-', ':'),
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
        RAW_EXCEL = f.read()
except FileNotFoundError:
    RAW_EXCEL = ""

LIVE_GROUPS = parse_live_schedule(RAW_EXCEL) if RAW_EXCEL else []

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


def normalize_city_name(city: str) -> str:
    city = city.lower().strip()
    aliases = {
        "мск": "москва",
        "москва": "москва",
        "спб": "санкт-петербург",
        "питер": "санкт-петербург",
        "петербург": "санкт-петербург",
        "санкт-петербург": "санкт-петербург",
    }
    return aliases.get(city, city)


def get_live_groups_for_city(city: str):
    city_norm = normalize_city_name(city)
    result = []
    for g in LIVE_GROUPS:
        gcity = g["city"].lower().strip()
        if city_norm == "москва":
            if gcity == "москва" or gcity.startswith("москва,") or gcity.startswith("москва "):
                result.append(g)
        elif city_norm == "санкт-петербург":
            if gcity == "санкт-петербург" or gcity.startswith("санкт-петербург,") or gcity.startswith("санкт-петербург "):
                result.append(g)
        else:
            if city_norm in gcity:
                result.append(g)
    return result


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


def get_live_week(city: str) -> str:
    city_groups = get_live_groups_for_city(city)
    if not city_groups:
        return f"В городе «{escape_html(city)}» живых групп не найдено."

    parts: List[str] = [f"🏙 Живые группы в {escape_html(city)}:"]
    today = moscow_now().date()
    monday = today - timedelta(days=today.weekday())

    for offset in range(7):
        target_date = monday + timedelta(days=offset)
        day_name = DAYS[target_date.weekday()]
        day_set = set()
        for g in city_groups:
            for entry in g["days"]:
                if day_entry_matches_date(entry, target_date):
                    key = (
                        g["name"],
                        g["address"],
                        entry["start"],
                        entry["end"],
                        entry.get("is_work_meeting", False),
                    )
                    day_set.add(key)
        if day_set:
            parts.append(format_day_header(day_name, target_date.strftime('%d.%m')))
            for name, address, start, end, is_work_meeting in sorted(day_set, key=lambda x: x[2]):
                parts.append(
                    format_live_group(
                        name=name,
                        address=address,
                        start=start,
                        end=end,
                        is_work_meeting=is_work_meeting,
                    )
                )
            parts.append("")

    if parts and parts[-1] == "":
        parts.pop()
    return "\n".join(parts)


def get_online_by_day(day_index: int):
    return sorted(ONLINE_SCHEDULE.get(day_index, []), key=lambda x: x[0])


def get_online_full():
    parts = []
    for i, day_name in enumerate(DAYS):
        groups = get_online_by_day(i)
        if groups:
            lines = [f"<b>{day_name}</b>"]
            lines.extend(format_online_group(t, n, u) for t, n, u in groups)
            parts.append("\n".join(lines))
    return "\n\n".join(parts) if parts else "Онлайн-групп нет."


def is_user_subscribed_to_online(user_data: dict, group_name: str) -> bool:
    return user_data.get("all_online", False) or group_name in user_data.get("groups", {})


def is_user_subscribed_to_live(user_data: dict, group_name: str) -> bool:
    return user_data.get("all_live", False) or group_name in user_data.get("groups", {})


def get_today_online_subscriptions(user_data: dict):
    day_index = moscow_now().weekday()
    return [(time_str, name, url) for time_str, name, url in get_online_by_day(day_index) if is_user_subscribed_to_online(user_data, name)]


def get_today_live_subscriptions(user_data: dict):
    city = user_data.get("city")
    if not city:
        return []
    day_index = moscow_now().weekday()
    return [
        (name, address, start, end, is_work_meeting)
        for name, address, start, end, is_work_meeting in get_live_groups_for_day(city, day_index)
        if is_user_subscribed_to_live(user_data, name)
    ]


def build_daily_message(uid: str, user_data: dict) -> Optional[str]:
    day_index = moscow_now().weekday()
    online_groups = get_today_online_subscriptions(user_data)
    live_groups = get_today_live_subscriptions(user_data)
    if not online_groups and not live_groups:
        return None
    parts = [f"☀ Доброе утро. Вот ваши группы на сегодня, <b>{DAYS[day_index]}</b>:"]
    if online_groups:
        parts.append("\n🌐 <b>Онлайн</b>")
        parts.extend(format_online_group_with_sub(t, n, u, uid) for t, n, u in online_groups)
    if live_groups:
        parts.append("\n🏙 <b>Живые</b>")
        parts.extend(format_live_group_with_sub(n, a, s, e, w, uid) for n, a, s, e, w in live_groups)
    return "\n".join(parts)


def build_reminder_key(group_type: str, group_name: str, date_str: str, time_str: str) -> str:
    return f"{group_type}|{group_name}|{date_str}|{time_str}"


def build_online_single_reminder(name: str, url: str, time_str: str, minutes_before: int = 60) -> str:
    before_text = "за час" if minutes_before == 60 else "за два часа"
    return (
        f"Привет! Это бережное напоминание: {before_text} начнётся онлайн-группа.\n\n"
        f"🌐 <b>{escape_html(name)}</b>\n"
        f"Начало: <b>{time_str}</b>\n"
        f"Ссылка: <a href=\"{url}\">перейти в группу</a>\n\n"
        "Возвращайтесь, программа работает"
    )


def build_online_multi_reminder(time_str: str, items: list[tuple[str, str]], minutes_before: int = 60) -> str:
    before_text = "за час" if minutes_before == 60 else "за два часа"
    text = [
        f"Привет! Это бережное напоминание: {before_text} начнутся онлайн-группы.",
        "",
        f"Начало: <b>{time_str}</b>",
        "",
    ]
    for name, url in sorted(items, key=lambda x: x[0].lower()):
        text.append(f"• <a href=\"{url}\"><b>{escape_html(name)}</b></a>")
    text.append("")
    text.append("Возвращайтесь, программа работает")
    return "\n".join(text)


def build_live_single_reminder(name: str, address: str, start: str, is_work_meeting: bool, minutes_before: int = 60) -> str:
    label = " 🔧" if is_work_meeting else ""
    before_text = "за час" if minutes_before == 60 else "за два часа"
    return (
        f"Привет! Это бережное напоминание: {before_text} начнётся живая группа.\n\n"
        f"🏙 <b>{escape_html(name)}</b>{label}\n"
        f"Адрес: {escape_html(address)}\n"
        f"Начало: <b>{start}</b>\n\n"
        "Возвращайтесь, программа работает"
    )


def build_live_multi_reminder(start: str, items: list[tuple[str, str, bool]], minutes_before: int = 60) -> str:
    before_text = "за час" if minutes_before == 60 else "за два часа"
    text = [
        f"Привет! Это бережное напоминание: {before_text} начнутся живые группы.",
        "",
        f"Начало: <b>{start}</b>",
        "",
    ]
    for name, address, is_work_meeting in sorted(items, key=lambda x: x[0].lower()):
        label = " 🔧" if is_work_meeting else ""
        text.append(f"• <b>{escape_html(name)}</b>{label} — {escape_html(address)}")
    text.append("")
    text.append("Возвращайтесь, программа работает")
    return "\n".join(text)


def get_online_settings(user_data: dict) -> dict:
    return user_data.get("online_settings", {
        "daily_hour": user_data.get("daily_hour", 7),
        "remind_before": user_data.get("remind_before", [60]),
    })


def get_live_settings(user_data: dict) -> dict:
    return user_data.get("live_settings", {
        "daily_hour": user_data.get("daily_hour", 7),
        "remind_before": user_data.get("remind_before", [60]),
    })


def set_quiet_mode(uid: str, enabled: bool):
    data = get_user_sub(uid)
    today_str = moscow_now().strftime("%Y-%m-%d")
    if enabled:
        data["quiet_mode_date"] = today_str
    else:
        data["quiet_mode_date"] = None
    set_user_sub(uid, data)



def is_quiet_mode_active(user_data: dict) -> bool:
    if user_data.get("quiet_mode_date") is None:
        return False
    return user_data["quiet_mode_date"] == moscow_now().strftime("%Y-%m-%d")


def collect_due_reminders(user_data: dict, now_dt: datetime):
    if is_quiet_mode_active(user_data):
        return []
    due = []
    today_str = now_dt.strftime("%Y-%m-%d")
    now_minutes = now_dt.hour * 60 + now_dt.minute
    reminders_meta = user_data.setdefault("meta", {}).setdefault("last_reminders", {})

    online_settings = get_online_settings(user_data)
    online_due_by_time: Dict[tuple, list[tuple[str, str]]] = {}
    for time_str, name, url in get_online_by_day(now_dt.weekday()):
        if not is_user_subscribed_to_online(user_data, name):
            continue
        group_minutes = time_to_minutes(time_str)
        for r_min in online_settings.get("remind_before", [60]):
            if group_minutes - now_minutes == r_min:
                key = (time_str, r_min)
                online_due_by_time.setdefault(key, []).append((name, url))

    for (time_str, r_min), items in online_due_by_time.items():
        if len(items) == 1:
            name, url = items[0]
            remind_key = build_reminder_key("online", name, today_str, time_str) + f"|{r_min}"
            if reminders_meta.get(remind_key) != today_str:
                text = build_online_single_reminder(name, url, time_str, r_min)
                due.append((remind_key, text, True))
        else:
            names = "|".join(sorted(name for name, _ in items))
            remind_key = build_reminder_key("online_multi", names, today_str, time_str) + f"|{r_min}"
            if reminders_meta.get(remind_key) != today_str:
                text = build_online_multi_reminder(time_str, items, r_min)
                due.append((remind_key, text, True))

    city = user_data.get("city")
    if city:
        live_settings = get_live_settings(user_data)
        live_due_by_time: Dict[tuple, list[tuple[str, str, bool]]] = {}
        for name, address, start, end, is_work_meeting in get_live_groups_for_day(city, now_dt.weekday()):
            if not is_user_subscribed_to_live(user_data, name):
                continue
            start_minutes = time_to_minutes(start)
            for r_min in live_settings.get("remind_before", [60]):
                if start_minutes - now_minutes == r_min:
                    key = (start, r_min)
                    live_due_by_time.setdefault(key, []).append((name, address, is_work_meeting))

        for (start, r_min), items in live_due_by_time.items():
            if len(items) == 1:
                name, address, is_work_meeting = items[0]
                remind_key = build_reminder_key("live", name, today_str, start) + f"|{r_min}"
                if reminders_meta.get(remind_key) != today_str:
                    text = build_live_single_reminder(name, address, start, is_work_meeting, r_min)
                    due.append((remind_key, text, False))
            else:
                names = "|".join(sorted(name for name, _, _ in items))
                remind_key = build_reminder_key("live_multi", names, today_str, start) + f"|{r_min}"
                if reminders_meta.get(remind_key) != today_str:
                    text = build_live_multi_reminder(start, items, r_min)
                    due.append((remind_key, text, False))
    return due


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
        if is_quiet_mode_active(data):
            subs[uid] = data
            continue
        if not data.get("groups") and not data.get("all_online") and not data.get("all_live"):
            subs[uid] = data
            continue

        send_online = False
        send_live = False
        if data.get("all_online") or any(v.get("type") == "online" for v in data.get("groups", {}).values()):
            online_settings = get_online_settings(data)
            if now_dt.hour == online_settings.get("daily_hour", 7):
                send_online = True
        if data.get("all_live") or any(v.get("type") == "live" for v in data.get("groups", {}).values()):
            live_settings = get_live_settings(data)
            if now_dt.hour == live_settings.get("daily_hour", 7):
                send_live = True
        if not send_online and not send_live:
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
        for key, text, disable_preview in due_reminders:
            try:
                await bot.send_message(int(uid), text, parse_mode="HTML", disable_web_page_preview=disable_preview)
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
            await send_daily_notifications(bot, now_dt)
            await send_hourly_reminders(bot, now_dt)
        except Exception as e:
            print(f"❌ Ошибка в notifications_worker: {e}")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


# ─── Шаблоны подписки ───
def apply_online_subscription_template(uid: str, template: str):
    data = get_user_sub(uid)
    data["all_online"] = False
    groups = data.setdefault("groups", {})
    data["groups"] = {k: v for k, v in groups.items() if v.get("type") != "online"}
    for day_idx in range(7):
        for time_str, name, url in get_online_by_day(day_idx):
            hour = int(time_str.split(":")[0])
            if template == "morning" and hour < 12:
                data["groups"][name] = {"type": "online", "info": {"url": url}}
            elif template == "day" and 12 <= hour < 18:
                data["groups"][name] = {"type": "online", "info": {"url": url}}
            elif template == "evening" and hour >= 18:
                data["groups"][name] = {"type": "online", "info": {"url": url}}
            elif template == "weekdays" and day_idx < 5:
                data["groups"][name] = {"type": "online", "info": {"url": url}}
            elif template == "weekends" and day_idx >= 5:
                data["groups"][name] = {"type": "online", "info": {"url": url}}
    set_user_sub(uid, data)


def apply_live_subscription_template(uid: str, city: str, template: str):
    data = get_user_sub(uid)
    data["all_live"] = False
    groups = data.setdefault("groups", {})
    data["groups"] = {k: v for k, v in groups.items() if v.get("type") != "live"}
    city_groups = get_live_groups_for_city(city)
    for g in city_groups:
        for entry in g["days"]:
            start_hour = int(entry["start"].split(":")[0])
            if template == "morning" and start_hour < 12:
                data["groups"][g["name"]] = {"type": "live", "info": {"city": g["city"], "address": g["address"]}}
            elif template == "day" and 12 <= start_hour < 18:
                data["groups"][g["name"]] = {"type": "live", "info": {"city": g["city"], "address": g["address"]}}
            elif template == "evening" and start_hour >= 18:
                data["groups"][g["name"]] = {"type": "live", "info": {"city": g["city"], "address": g["address"]}}
            if template == "weekdays" and entry["day"] < 5:
                data["groups"][g["name"]] = {"type": "live", "info": {"city": g["city"], "address": g["address"]}}
            elif template == "weekends" and entry["day"] >= 5:
                data["groups"][g["name"]] = {"type": "live", "info": {"city": g["city"], "address": g["address"]}}
    set_user_sub(uid, data)

# ─── Клавиатуры ───
def online_menu_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📅 Сегодня", callback_data="online_today"),
        InlineKeyboardButton(text="📋 Полное", callback_data="online_full"),
    )
    builder.row(InlineKeyboardButton(text="📆 Выбрать день", callback_data="online_choose_day"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="main_menu"))
    return builder.as_markup()


def live_city_keyboard():
    builder = InlineKeyboardBuilder()
    for city in POPULAR_CITIES:
        cid = city_to_id.get(city, city)
        builder.button(text=city, callback_data=f"live_city_{cid}")
    builder.adjust(2)
    builder.row(InlineKeyboardButton(text="🔍 Найти свой город", callback_data="live_search_city"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="main_menu"))
    return builder.as_markup()


def live_period_keyboard(city: str):
    cid = city_to_id.get(city, city)
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📅 Сегодня", callback_data=f"live_today_{cid}"),
        InlineKeyboardButton(text="📋 Вся неделя", callback_data=f"live_week_{cid}"),
    )
    builder.row(InlineKeyboardButton(text="📆 Выбрать день", callback_data=f"live_choose_day_{cid}"))
    builder.row(InlineKeyboardButton(text="← К городам", callback_data="mode_live"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="main_menu"))
    return builder.as_markup()


def get_days_keyboard(
    prefix: str,
    back_callback: Optional[str] = None,
    back_text: str = "← Назад",
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for i, day_name in enumerate(DAYS):
        builder.button(text=day_name, callback_data=f"{prefix}{i}")
    builder.adjust(2)
    if back_callback:
        builder.row(InlineKeyboardButton(text=back_text, callback_data=back_callback))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="main_menu"))
    return builder.as_markup()


def main_menu_inline_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🌐 Онлайн", callback_data="main_online"),
        InlineKeyboardButton(text="🏙 Живые", callback_data="main_live"),
    )
    builder.row(
        InlineKeyboardButton(text="💫 Установка", callback_data="main_slogan"),
        InlineKeyboardButton(text="🔔 Подписка", callback_data="main_sub"),
    )
    builder.row(
        InlineKeyboardButton(text="⭐ Мои группы", callback_data="main_my_groups"),
        InlineKeyboardButton(text="🔕 Отписаться", callback_data="main_unsubscribe"),
    )
    builder.row(
        InlineKeyboardButton(text="🔜 Ближайшая", callback_data="main_nearest")
    )
    return builder.as_markup()


dp = Dispatcher(storage=MemoryStorage())

# ─── Главное меню и базовые колбэки ───
@dp.callback_query(F.data == "main_menu")
async def main_menu_callback(callback: CallbackQuery):
    await safe_edit_text(
        callback.message,
        "🏠 Главное меню\n\nВыберите раздел:",
        reply_markup=main_menu_inline_keyboard()
    )
    await safe_callback_answer(callback)


@dp.callback_query(F.data == "main_online")
async def main_online(callback: CallbackQuery):
    await safe_edit_text(
        callback.message,
        "🌐 Онлайн-расписание",
        parse_mode="HTML",
        reply_markup=online_menu_keyboard()
    )
    await safe_callback_answer(callback)


@dp.callback_query(F.data == "main_live")
async def main_live(callback: CallbackQuery):
    await safe_edit_text(
        callback.message,
        "🏙 Выберите город:",
        parse_mode="HTML",
        reply_markup=live_city_keyboard()
    )
    await safe_callback_answer(callback)


@dp.callback_query(F.data == "main_slogan")
async def main_slogan(callback: CallbackQuery):
    slogan = random.choice(SLOGANS_AND_AFFIRMATIONS)
    await safe_edit_text(
        callback.message,
        f"<b>Установка</b>\n<i>{escape_html(slogan)}</i>",
        parse_mode="HTML",
        reply_markup=back_markup("⬅️ Главное меню", "main_menu")
    )
    await safe_callback_answer(callback)


@dp.callback_query(F.data == "main_sub")
async def main_sub(callback: CallbackQuery):
    await show_sub_main(callback)


@dp.callback_query(F.data == "main_my_groups")
async def main_my_groups(callback: CallbackQuery):
    await show_my_groups_summary(callback.from_user.id, callback.message, edit=True)


@dp.callback_query(F.data == "main_unsubscribe")
async def main_unsubscribe(callback: CallbackQuery):
    remove_subscriber(str(callback.from_user.id))
    await safe_edit_text(
        callback.message,
        "🔕 Вы отписались от всего.",
        reply_markup=back_markup("⬅️ Главное меню", "main_menu")
    )
    await safe_callback_answer(callback)


# ─── Ближайшая группа ───
@dp.callback_query(F.data == "main_nearest")
async def main_nearest(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    now = moscow_now()
    online_today = get_today_online_subscriptions(data)
    live_today = get_today_live_subscriptions(data)

    upcoming = []
    for time_str, name, url in online_today:
        try:
            start_dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {time_str}", "%Y-%m-%d %H:%M")
        except:
            continue
        if start_dt >= now:
            upcoming.append((start_dt, "online", name, url, ""))
    for name, address, start, end, is_work_meeting in live_today:
        try:
            start_dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {start}", "%Y-%m-%d %H:%M")
        except:
            continue
        if start_dt >= now:
            upcoming.append((start_dt, "live", name, "", address))

    upcoming.sort(key=lambda x: x[0])
    if not upcoming:
        text = "Сегодня больше нет запланированных групп."
    else:
        text = "🔜 Ближайшие группы:\n\n"
        for i, (start_dt, typ, name, url, address) in enumerate(upcoming[:5]):
            delta = start_dt - now
            hours, rem = divmod(delta.seconds, 3600)
            minutes = rem // 60
            timer = f"через {hours} ч {minutes} мин" if hours > 0 else f"через {minutes} мин"
            if typ == "online":
                text += f"🌐 {name} — {timer} ({start_dt.strftime('%H:%M')})\n"
            else:
                text += f"🏙 {name} — {timer} ({start_dt.strftime('%H:%M')}) — {address}\n"
    await safe_edit_text(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=back_markup("⬅️ Главное меню", "main_menu")
    )
    await safe_callback_answer(callback)


# ─── Сводка «Мои группы» ───
async def show_my_groups_summary(uid: str, target: Message, edit: bool = False):
    data = get_user_sub(uid)
    text = "<b>Мои группы и настройки</b>\n\n"
    online_count = 0
    live_count = 0
    if data.get("all_online"):
        online_count = len(all_online_names)
    else:
        online_count = sum(1 for g in data.get("groups", {}).values() if g.get("type") == "online")
    text += f"🌐 Онлайн: {online_count} групп"
    if data.get("all_online"):
        text += " (все)"
    text += "\n"

    city = data.get("city", "не выбран")
    if data.get("all_live"):
        live_count = len(set(g["name"] for g in get_live_groups_for_city(city)))
    else:
        live_count = sum(1 for g in data.get("groups", {}).values() if g.get("type") == "live")
    text += f"🏙 Живые: {city}, {live_count} групп"
    if data.get("all_live"):
        text += " (все)"
    text += "\n\n"

    online_settings = get_online_settings(data)
    text += f"🕖 Утро (онлайн): {online_settings['daily_hour']:02d}:00\n"
    remind_set = set(online_settings['remind_before'])
    remind_str = "за 1 и 2 часа" if remind_set == {60,120} else ("за 2 часа" if 120 in remind_set else "за 1 час")
    text += f"⏰ Напоминания (онлайн): {remind_str}\n"

    live_settings = get_live_settings(data)
    text += f"🕖 Утро (живые): {live_settings['daily_hour']:02d}:00\n"
    remind_set_live = set(live_settings['remind_before'])
    remind_str_live = "за 1 и 2 часа" if remind_set_live == {60,120} else ("за 2 часа" if 120 in remind_set_live else "за 1 час")
    text += f"⏰ Напоминания (живые): {remind_str_live}\n\n"

    quiet = is_quiet_mode_active(data)
    text += f"🔕 Тихий режим: {'ВКЛ' if quiet else 'ВЫКЛ'}\n"

    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="✏️ Изменить подписки", callback_data="my_groups_edit_subs"))
    builder.row(InlineKeyboardButton(text="⚙️ Настройки уведомлений", callback_data="my_groups_settings"))
    if quiet:
        builder.row(InlineKeyboardButton(text="🔔 Отключить тихий режим", callback_data="quiet_toggle_off"))
    else:
        builder.row(InlineKeyboardButton(text="🔕 Включить тихий режим на сегодня", callback_data="quiet_toggle_on"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="main_menu"))

    if edit:
        await safe_edit_text(target, text, parse_mode="HTML", reply_markup=builder.as_markup())
    else:
        await target.answer(text, parse_mode="HTML", reply_markup=builder.as_markup())


@dp.callback_query(F.data == "my_groups_edit_subs")
async def my_groups_edit_subs(callback: CallbackQuery):
    await show_sub_main(callback)


@dp.callback_query(F.data == "my_groups_settings")
async def my_groups_settings(callback: CallbackQuery):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🌐 Онлайн", callback_data="sub_settings_online"))
    builder.row(InlineKeyboardButton(text="🏙 Живые", callback_data="sub_settings_live"))
    builder.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="main_my_groups"))
    await safe_edit_text(
        callback.message,
        "Выберите тип уведомлений для настройки:",
        reply_markup=builder.as_markup()
    )
    await safe_callback_answer(callback)


@dp.callback_query(F.data == "quiet_toggle_on")
async def quiet_toggle_on(callback: CallbackQuery):
    set_quiet_mode(str(callback.from_user.id), True)
    await safe_callback_answer(callback, "Тихий режим включён до завтра")
    await show_my_groups_summary(callback.from_user.id, callback.message, edit=True)


@dp.callback_query(F.data == "quiet_toggle_off")
async def quiet_toggle_off(callback: CallbackQuery):
    set_quiet_mode(str(callback.from_user.id), False)
    await safe_callback_answer(callback, "Тихий режим выключен")
    await show_my_groups_summary(callback.from_user.id, callback.message, edit=True)


# ─── Подписка (show_sub_main, онлайн, живые) ───
async def show_sub_main(target: CallbackQuery | Message):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🌐 Онлайн-группы", callback_data="sub_online"))
    builder.row(InlineKeyboardButton(text="🏙 Живые группы", callback_data="sub_live"))
    builder.row(InlineKeyboardButton(text="🌐 Уведомления для онлайн", callback_data="sub_settings_online"))
    builder.row(InlineKeyboardButton(text="🏙 Уведомления для живых", callback_data="sub_settings_live"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="main_menu"))
    text = "<b>Подписка</b>\n\nВыберите тип групп или настройки:"
    if isinstance(target, CallbackQuery):
        await safe_edit_text(target.message, text, parse_mode="HTML", reply_markup=builder.as_markup())
        await safe_callback_answer(target)
    else:
        await target.answer(text, parse_mode="HTML", reply_markup=builder.as_markup())


async def show_sub_online_list(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    builder = InlineKeyboardBuilder()
    # Шаблоны
    builder.row(
        InlineKeyboardButton(text="🌅 Утро", callback_data="online_template_morning"),
        InlineKeyboardButton(text="🌆 День", callback_data="online_template_day"),
        InlineKeyboardButton(text="🌙 Вечер", callback_data="online_template_evening"),
    )
    builder.row(
        InlineKeyboardButton(text="Пн–Пт", callback_data="online_template_weekdays"),
        InlineKeyboardButton(text="Сб–Вс", callback_data="online_template_weekends"),
    )
    builder.row(InlineKeyboardButton(text="❌ Очистить онлайн-подписки", callback_data="online_template_clear"))
    all_online_prefix = "🔔" if data.get("all_online") else "🔕"
    builder.row(InlineKeyboardButton(text=f"{all_online_prefix} Все онлайн", callback_data="sub_toggle_online_all"))
    for gid, name in sorted(ONLINE_GROUP_ID_TO_NAME.items(), key=lambda x: x[1]):
        subbed = is_user_subscribed_to_online(data, name)
        prefix = "🔔" if subbed else "🔕"
        builder.row(InlineKeyboardButton(text=f"{prefix} {name}", callback_data=f"sub_toggle_online_{gid}"))
    builder.row(InlineKeyboardButton(text="← К подписке", callback_data="sub_main_back"))
    builder.row(InlineKeyboardButton(text="⚙️ Настройки уведомлений", callback_data="sub_settings_online"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="main_menu"))
    await safe_edit_text(callback.message, "🌐 Онлайн-группы\n\n🔔 — подписаны\n🔕 — не подписаны", parse_mode="HTML", reply_markup=builder.as_markup())
    await safe_callback_answer(callback)


async def show_sub_live_city_selector(target: CallbackQuery | Message):
    builder = InlineKeyboardBuilder()
    for city in POPULAR_CITIES:
        cid = city_to_id.get(city, city)
        builder.button(text=city, callback_data=f"sub_live_city_{cid}")
    builder.adjust(2)
    builder.row(InlineKeyboardButton(text="🔍 Найти", callback_data="sub_live_city_search"))
    builder.row(InlineKeyboardButton(text="← К подписке", callback_data="sub_main_back"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="main_menu"))
    text = "🏙 Выберите город для подписки на живые группы:"
    if isinstance(target, CallbackQuery):
        await safe_edit_text(target.message, text, reply_markup=builder.as_markup())
        await safe_callback_answer(target)
    else:
        await target.answer(text, reply_markup=builder.as_markup())


async def show_sub_live_list(target: CallbackQuery | Message, city: str):
    uid = str(target.from_user.id) if isinstance(target, CallbackQuery) else str(target.from_user.id)
    data = get_user_sub(uid)
    builder = InlineKeyboardBuilder()
    # Шаблоны
    builder.row(
        InlineKeyboardButton(text="🌅 Утро", callback_data=f"live_template_morning_{city}"),
        InlineKeyboardButton(text="🌆 День", callback_data=f"live_template_day_{city}"),
        InlineKeyboardButton(text="🌙 Вечер", callback_data=f"live_template_evening_{city}"),
    )
    builder.row(
        InlineKeyboardButton(text="Пн–Пт", callback_data=f"live_template_weekdays_{city}"),
        InlineKeyboardButton(text="Сб–Вс", callback_data=f"live_template_weekends_{city}"),
    )
    builder.row(InlineKeyboardButton(text="❌ Очистить живые подписки", callback_data=f"live_template_clear_{city}"))
    city_group_names = sorted({g["name"] for g in get_live_groups_for_city(city)})
    all_live_prefix = "🔔" if data.get("all_live") else "🔕"
    builder.row(InlineKeyboardButton(text=f"{all_live_prefix} Все живые в своём городе", callback_data="sub_toggle_live_all"))
    for name in city_group_names:
        gid = make_short_id("l", name)
        subbed = is_user_subscribed_to_live(data, name)
        prefix = "🔔" if subbed else "🔕"
        builder.row(InlineKeyboardButton(text=f"{prefix} {name}", callback_data=f"sub_toggle_live_{gid}"))
    builder.row(InlineKeyboardButton(text="🏙 Сменить город", callback_data="sub_live_city_change"))
    builder.row(InlineKeyboardButton(text="← К подписке", callback_data="sub_main_back"))
    builder.row(InlineKeyboardButton(text="⚙️ Настройки уведомлений", callback_data="sub_settings_live"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="main_menu"))
    text = f"🏙 Живые группы в {escape_html(city)}\n\n🔔 — подписаны\n🔕 — не подписаны"
    if isinstance(target, CallbackQuery):
        await safe_edit_text(target.message, text, parse_mode="HTML", reply_markup=builder.as_markup())
        await safe_callback_answer(target)
    else:
        await target.answer(text, parse_mode="HTML", reply_markup=builder.as_markup())


# ─── Обработчики подписок (остальные) ───
@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer("Добро пожаловать! Используйте кнопки ниже или меню в сообщении.", reply_markup=reply_main_menu)
    await message.answer("🏠 Главное меню\n\nВыберите раздел:", reply_markup=main_menu_inline_keyboard())


@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "<b>Команды</b>\n\n/start — главное меню\n/help — помощь\n/slogan — случайная фраза поддержки",
        parse_mode="HTML",
        reply_markup=back_markup("⬅️ Главное меню", "main_menu")
    )


@dp.message(Command("slogan"))
async def cmd_slogan(message: Message):
    slogan = random.choice(SLOGANS_AND_AFFIRMATIONS)
    await message.answer(
        f"<b>Фраза поддержки:</b>\n<i>{escape_html(slogan)}</i>",
        parse_mode="HTML",
        reply_markup=back_markup("⬅️ Главное меню", "main_menu")
    )


@dp.message(F.text == "🌐 Онлайн")
async def btn_online(message: Message):
    await message.answer("🌐 Онлайн-расписание", parse_mode="HTML", reply_markup=online_menu_keyboard())


@dp.message(F.text == "🏙 Живые")
async def btn_live(message: Message):
    await message.answer("🏙 Выберите город:", parse_mode="HTML", reply_markup=live_city_keyboard())


@dp.message(F.text == "💫 Установка")
async def btn_slogan(message: Message):
    slogan = random.choice(SLOGANS_AND_AFFIRMATIONS)
    await message.answer(
        f"<b>Установка</b>\n<i>{escape_html(slogan)}</i>",
        parse_mode="HTML",
        reply_markup=back_markup("⬅️ Главное меню", "main_menu")
    )


@dp.message(F.text == "🔔 Подписка")
async def sub_main(message: Message):
    await show_sub_main(message)


@dp.message(F.text == "⭐ Мои группы")
async def btn_my_groups(message: Message):
    await show_my_groups_summary(str(message.from_user.id), message)


@dp.message(F.text == "🔕 Отписаться")
async def btn_unsubscribe_all(message: Message):
    remove_subscriber(str(message.from_user.id))
    await message.answer("🔕 Вы отписались от всего.", reply_markup=back_markup("⬅️ Главное меню", "main_menu"))


@dp.callback_query(F.data == "sub_online")
async def sub_online_list(callback: CallbackQuery):
    await show_sub_online_list(callback)


@dp.callback_query(F.data == "sub_live")
async def sub_live_start(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    city = data.get("city")
    if not city:
        await show_sub_live_city_selector(callback)
        return
    await show_sub_live_list(callback, city)


@dp.callback_query(F.data == "sub_live_city_search")
async def sub_live_city_search(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SubCitySearch.waiting_for_city)
    await safe_edit_text(
        callback.message,
        "Введите город для подписки:",
        reply_markup=back_markup("← К живым", "sub_live"),
    )
    await safe_callback_answer(callback)


@dp.message(StateFilter(SubCitySearch.waiting_for_city))
async def sub_live_city_input(message: Message, state: FSMContext):
    query = message.text.strip()
    matched = get_searchable_cities(query)
    await state.clear()
    if not matched:
        await message.answer("Город не найден.", reply_markup=back_markup("← К живым", "sub_live"))
        return
    city = matched[0]
    uid = str(message.from_user.id)
    data = get_user_sub(uid)
    data["city"] = city
    set_user_sub(uid, data)
    await message.answer(f"Выбран город: <b>{escape_html(city)}</b>", parse_mode="HTML")
    await show_sub_live_list(message, city)


@dp.callback_query(F.data == "sub_live_city_change")
async def sub_live_city_change(callback: CallbackQuery):
    await show_sub_live_city_selector(callback)


@dp.callback_query(F.data.startswith("sub_live_city_"))
async def sub_live_city_selected(callback: CallbackQuery):
    cid = callback.data[len("sub_live_city_"):]
    city = id_to_city.get(cid, cid)
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    data["city"] = city
    set_user_sub(uid, data)
    await show_sub_live_list(callback, city)


@dp.callback_query(F.data == "sub_toggle_online_all")
async def sub_toggle_online_all(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    data["all_online"] = not data.get("all_online", False)
    if data["all_online"]:
        data["groups"] = {k: v for k, v in data.get("groups", {}).items() if v.get("type") != "online"}
    set_user_sub(uid, data)
    await safe_callback_answer(callback, "Готово")
    await show_sub_online_list(callback)


@dp.callback_query(F.data.startswith("sub_toggle_online_"))
async def sub_toggle_online(callback: CallbackQuery):
    gid = callback.data[len("sub_toggle_online_"):]
    group_name = ONLINE_GROUP_ID_TO_NAME.get(gid)
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    groups = data.setdefault("groups", {})
    if not group_name:
        await safe_callback_answer(callback, "Ошибка ID")
        return
    if group_name in groups:
        del groups[group_name]
    else:
        info = {}
        for day_groups in ONLINE_SCHEDULE.values():
            for _, n, u in day_groups:
                if n == group_name:
                    info = {"url": u}
                    break
            if info:
                break
        groups[group_name] = {"type": "online", "info": info}
    set_user_sub(uid, data)
    await safe_callback_answer(callback, "Готово")
    await show_sub_online_list(callback)


@dp.callback_query(F.data == "sub_toggle_live_all")
async def sub_toggle_live_all(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    city = data.get("city")
    if not city:
        await safe_callback_answer(callback, "Сначала выберите город")
        await show_sub_live_city_selector(callback)
        return
    data["all_live"] = not data.get("all_live", False)
    if data["all_live"]:
        data["groups"] = {k: v for k, v in data.get("groups", {}).items() if v.get("type") != "live"}
    set_user_sub(uid, data)
    await safe_callback_answer(callback, "Готово")
    await show_sub_live_list(callback, city)


@dp.callback_query(F.data.startswith("sub_toggle_live_"))
async def sub_toggle_live(callback: CallbackQuery):
    gid = callback.data[len("sub_toggle_live_"):]
    group_name = LIVE_GROUP_ID_TO_NAME.get(gid)
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    groups = data.setdefault("groups", {})
    if not group_name:
        await safe_callback_answer(callback, "Ошибка ID")
        return
    if group_name in groups:
        del groups[group_name]
    else:
        info = {}
        for g in LIVE_GROUPS:
            if g["name"] == group_name:
                info = {"city": g["city"], "address": g["address"]}
                break
        groups[group_name] = {"type": "live", "info": info}
    set_user_sub(uid, data)
    await safe_callback_answer(callback, "Готово")
    city = data.get("city")
    if city:
        await show_sub_live_list(callback, city)
    else:
        await show_sub_live_city_selector(callback)


@dp.callback_query(F.data == "sub_main_back")
async def sub_main_back(callback: CallbackQuery):
    await show_sub_main(callback)


# ─── Шаблоны подписки (колбэки) ───
@dp.callback_query(F.data.startswith("online_template_"))
async def online_template(callback: CallbackQuery):
    template = callback.data.split("_", 2)[2]
    uid = str(callback.from_user.id)
    if template == "clear":
        data = get_user_sub(uid)
        data["all_online"] = False
        data["groups"] = {k: v for k, v in data.get("groups", {}).items() if v.get("type") != "online"}
        set_user_sub(uid, data)
    else:
        apply_online_subscription_template(uid, template)
    await safe_callback_answer(callback, "Шаблон применён")
    await show_sub_online_list(callback)


@dp.callback_query(F.data.startswith("live_template_"))
async def live_template(callback: CallbackQuery):
    parts = callback.data.split("_", 2)
    template = parts[2].rsplit("_", 1)[0]
    city = parts[2].rsplit("_", 1)[1]
    uid = str(callback.from_user.id)
    if template == "clear":
        data = get_user_sub(uid)
        data["all_live"] = False
        data["groups"] = {k: v for k, v in data.get("groups", {}).items() if v.get("type") != "live"}
        set_user_sub(uid, data)
    else:
        apply_live_subscription_template(uid, city, template)
    await safe_callback_answer(callback, "Шаблон применён")
    await show_sub_live_list(callback, city)


# ─── Настройки уведомлений ───
@dp.callback_query(F.data == "sub_settings_online")
async def sub_settings_online(callback: CallbackQuery):
    await settings_menu(callback, "online")


@dp.callback_query(F.data == "sub_settings_live")
async def sub_settings_live(callback: CallbackQuery):
    await settings_menu(callback, "live")


async def settings_menu(target, group_type: str):
    if isinstance(target, CallbackQuery):
        message = target.message
        uid = str(target.from_user.id)
        use_edit = True
        answer_func = lambda: safe_callback_answer(target)
    else:
        message = target
        uid = str(target.from_user.id)
        use_edit = False
        answer_func = lambda: None

    data = get_user_sub(uid)
    if group_type == "online":
        settings = get_online_settings(data)
        title = "🌐 Настройки уведомлений для онлайн"
        prefix = "online"
    else:
        settings = get_live_settings(data)
        title = "🏙 Настройки уведомлений для живых"
        prefix = "live"

    daily_hour = settings["daily_hour"]
    remind_before = settings["remind_before"]
    remind_set = set(remind_before)
    time_label = f"{daily_hour:02d}:00"
    if remind_set == {60, 120}:
        remind_label = "за 1 и 2 часа"
    elif remind_set == {120}:
        remind_label = "за 2 часа"
    else:
        remind_label = "за 1 час"

    text = (
        f"<b>{title}</b>\n\n"
        f"🕖 Утреннее расписание: <b>{time_label}</b>\n"
        f"⏰ Напоминания: <b>{remind_label}</b>\n\n"
        "Выберите, что изменить:"
    )

    builder = InlineKeyboardBuilder()
    for h in [5, 6, 7, 8, 9]:
        builder.button(
            text=f'{"✅ " if daily_hour == h else ""}{h:02d}:00',
            callback_data=f"set_daily_hour_{prefix}_{h}"
        )
    builder.adjust(5)
    remind_buttons = [
        ("✅ За 1 час" if remind_set == {60} else "За 1 час", f"set_remind_{prefix}_1"),
        ("✅ За 2 часа" if remind_set == {120} else "За 2 часа", f"set_remind_{prefix}_2"),
        ("✅ Оба" if remind_set == {60, 120} else "Оба", f"set_remind_{prefix}_both"),
    ]
    for label, cb in remind_buttons:
        builder.button(text=label, callback_data=cb)
    builder.adjust(3)
    builder.row(InlineKeyboardButton(text="← К подписке", callback_data="sub_main_back"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="main_menu"))

    if use_edit:
        await safe_edit_text(message, text, parse_mode="HTML", reply_markup=builder.as_markup())
        answer_func()
    else:
        await message.answer(text, parse_mode="HTML", reply_markup=builder.as_markup())


@dp.callback_query(F.data.startswith("set_daily_hour_"))
async def set_daily_hour(callback: CallbackQuery):
    parts = callback.data.split("_")
    group_type = parts[3]
    hour = int(parts[4])
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    if group_type == "online":
        data.setdefault("online_settings", {})["daily_hour"] = hour
    else:
        data.setdefault("live_settings", {})["daily_hour"] = hour
    set_user_sub(uid, data)
    await settings_menu(callback, group_type)


@dp.callback_query(F.data.startswith("set_remind_"))
async def set_remind(callback: CallbackQuery):
    parts = callback.data.split("_")
    group_type = parts[2]
    option = parts[3]
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    if option == "1":
        remind = [60]
    elif option == "2":
        remind = [120]
    elif option == "both":
        remind = [60, 120]
    else:
        remind = [60]
    if group_type == "online":
        data.setdefault("online_settings", {})["remind_before"] = remind
    else:
        data.setdefault("live_settings", {})["remind_before"] = remind
    set_user_sub(uid, data)
    await settings_menu(callback, group_type)


# ─── Обработчики расписания (онлайн и живые) ───
@dp.callback_query(F.data == "online_today")
async def online_today(callback: CallbackQuery):
    day_index = moscow_now().weekday()
    groups = get_online_by_day(day_index)
    uid = str(callback.from_user.id)
    text = f"{format_day_header(DAYS[day_index])}\n\n"
    text += "\n".join(format_online_group_with_sub(t, n, u, uid) for t, n, u in groups) if groups else "Нет групп."
    await safe_edit_text(callback.message, text, parse_mode="HTML", disable_web_page_preview=True,
                         reply_markup=back_markup("← К онлайн", "mode_online"))
    await safe_callback_answer(callback)


@dp.callback_query(F.data == "online_full")
async def online_full(callback: CallbackQuery):
    full_text = get_online_full()
    parts = split_long_message(full_text)
    await safe_edit_text(callback.message, "📋 Полное расписание:", parse_mode="HTML")
    for idx, part in enumerate(parts):
        kb = back_markup("← К онлайн", "mode_online") if idx == len(parts) - 1 else None
        await callback.message.answer(part, parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb)
    await safe_callback_answer(callback)


@dp.callback_query(F.data == "online_choose_day")
async def online_choose_day(callback: CallbackQuery):
    await safe_edit_text(callback.message, "📆 Выберите день:",
                         reply_markup=get_days_keyboard("online_day_", back_callback="mode_online", back_text="← К онлайн"))
    await safe_callback_answer(callback)


@dp.callback_query(F.data.startswith("online_day_"))
async def online_show_day(callback: CallbackQuery):
    day_index = int(callback.data.split("_")[-1])
    groups = get_online_by_day(day_index)
    uid = str(callback.from_user.id)
    text = f"{format_day_header(DAYS[day_index])}\n\n"
    text += "\n".join(format_online_group_with_sub(t, n, u, uid) for t, n, u in groups) if groups else "Нет групп."
    await safe_edit_text(callback.message, text, parse_mode="HTML", disable_web_page_preview=True,
                         reply_markup=back_markup("← К дням", "online_choose_day"))
    await safe_callback_answer(callback)


@dp.callback_query(F.data == "mode_online")
async def back_to_online_menu(callback: CallbackQuery):
    await safe_edit_text(callback.message, "🌐 Онлайн-расписание", parse_mode="HTML", reply_markup=online_menu_keyboard())
    await safe_callback_answer(callback)


@dp.callback_query(F.data.startswith("live_city_"))
async def process_city(callback: CallbackQuery):
    cid = callback.data[len("live_city_"):]
    city = id_to_city.get(cid, cid)
    await safe_edit_text(callback.message, f"🏙 <b>{escape_html(city)}</b>\nВыберите:", parse_mode="HTML", reply_markup=live_period_keyboard(city))
    await safe_callback_answer(callback)


@dp.callback_query(F.data == "mode_live")
async def back_to_live(callback: CallbackQuery):
    await safe_edit_text(callback.message, "🏙 Выберите город:", parse_mode="HTML", reply_markup=live_city_keyboard())
    await safe_callback_answer(callback)


@dp.callback_query(F.data.startswith("live_today_"))
async def live_today(callback: CallbackQuery):
    cid = callback.data[len("live_today_"):]
    city = id_to_city.get(cid, cid)
    day_index = moscow_now().weekday()
    groups = get_live_groups_for_day(city, day_index)
    uid = str(callback.from_user.id)
    text = f"🏙 <b>{escape_html(city)}</b>\n\n{format_day_header(DAYS[day_index])}\n\n"
    text += "\n".join(format_live_group_with_sub(n, a, s, e, w, uid) for n, a, s, e, w in groups) if groups else "Нет групп."
    await safe_edit_text(callback.message, text, parse_mode="HTML",
                         reply_markup=back_markup("← К городу", f"live_period_{cid}"))
    await safe_callback_answer(callback)


@dp.callback_query(F.data.startswith("live_week_"))
async def live_week(callback: CallbackQuery):
    cid = callback.data[len("live_week_"):]
    city = id_to_city.get(cid, cid)
    full_text = get_live_week(city)
    parts = split_long_message(full_text)
    await safe_edit_text(callback.message, "📋 Расписание на неделю:", parse_mode="HTML")
    for idx, part in enumerate(parts):
        kb = back_markup("← К городу", f"live_period_{cid}") if idx == len(parts) - 1 else None
        await callback.message.answer(part, parse_mode="HTML", reply_markup=kb)
    await safe_callback_answer(callback)


@dp.callback_query(F.data.startswith("live_period_"))
async def live_period_back(callback: CallbackQuery):
    cid = callback.data[len("live_period_"):]
    city = id_to_city.get(cid, cid)
    await safe_edit_text(callback.message, f"🏙 <b>{escape_html(city)}</b>\nВыберите:", parse_mode="HTML", reply_markup=live_period_keyboard(city))
    await safe_callback_answer(callback)


@dp.callback_query(F.data.startswith("live_choose_day_"))
async def live_choose_day(callback: CallbackQuery):
    cid = callback.data[len("live_choose_day_"):]
    city = id_to_city.get(cid, cid)
    await safe_edit_text(
        callback.message,
        f"📆 <b>{escape_html(city)}</b>\n\nВыберите день:",
        parse_mode="HTML",
        reply_markup=get_days_keyboard(f"live_day_{cid}_", back_callback=f"live_period_{cid}", back_text="← К городу"),
    )
    await safe_callback_answer(callback)


@dp.callback_query(F.data.startswith("live_day_"))
async def live_show_day(callback: CallbackQuery):
    parts = callback.data.split("_", 2)
    tail = parts[2]
    cid, idx_str = tail.rsplit("_", 1)
    city = id_to_city.get(cid, cid)
    day_index = int(idx_str)
    groups = get_live_groups_for_day(city, day_index)
    uid = str(callback.from_user.id)
    text = f"🏙 <b>{escape_html(city)}</b>\n\n{format_day_header(DAYS[day_index])}\n\n"
    text += "\n".join(format_live_group_with_sub(n, a, s, e, w, uid) for n, a, s, e, w in groups) if groups else "Нет групп."
    await safe_edit_text(callback.message, text, parse_mode="HTML",
                         reply_markup=back_markup("← К дням", f"live_choose_day_{cid}"))
    await safe_callback_answer(callback)


@dp.callback_query(F.data == "live_search_city")
async def live_search_city_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(LiveGroupSearch.waiting_for_city)
    await safe_edit_text(callback.message, "🔍 Введите город:", reply_markup=back_markup("← К городам", "mode_live"))
    await safe_callback_answer(callback)


@dp.message(StateFilter(LiveGroupSearch.waiting_for_city))
async def live_search_city_handle(message: Message, state: FSMContext):
    query = message.text.strip()
    matched = get_searchable_cities(query)
    await state.clear()
    if not matched:
        await message.answer("Город не найден.", reply_markup=back_markup("← К городам", "mode_live"))
        return
    if len(matched) == 1:
        city = matched[0]
        await message.answer(f"🏙 <b>{escape_html(city)}</b>\nВыберите:", parse_mode="HTML", reply_markup=live_period_keyboard(city))
    else:
        builder = InlineKeyboardBuilder()
        for c in matched[:20]:
            cid = city_to_id.get(c, c)
            builder.button(text=c, callback_data=f"live_city_{cid}")
        builder.adjust(1)
        builder.row(InlineKeyboardButton(text="← К городам", callback_data="mode_live"))
        await message.answer("🔍 Уточните:", reply_markup=builder.as_markup())


@dp.message()
async def fallback(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is not None:
        return
    await message.answer("Используйте кнопки меню 👇", reply_markup=reply_main_menu)


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




# ================== ADDED FEATURES ==================

def is_favorite(user_data: dict, name: str) -> bool:
    return name in user_data.get("favorites", [])


def get_urgent_groups(user_data: dict, now: datetime, window_minutes: int = 30):
    results = []

    for time_str, name, url in get_online_by_day(now.weekday()):
        try:
            dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {time_str}", "%Y-%m-%d %H:%M")
        except:
            continue

        delta = int((dt - now).total_seconds() / 60)

        if -5 <= delta <= window_minutes:
            results.append({
                "type": "online",
                "name": name,
                "url": url,
                "start": time_str,
                "delta": delta
            })

    city = user_data.get("city")
    if city:
        for name, address, start, end, is_work_meeting in get_live_groups_for_day(city, now.weekday()):
            try:
                dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {start}", "%Y-%m-%d %H:%M")
            except:
                continue

            delta = int((dt - now).total_seconds() / 60)

            if -5 <= delta <= window_minutes:
                results.append({
                    "type": "live",
                    "name": name,
                    "address": address,
                    "start": start,
                    "delta": delta
                })

    results.sort(key=lambda x: x["delta"])
    return results


def format_urgent_message(groups: list) -> str:
    if not groups:
        return "Сейчас нет групп в ближайшее время."

    text = ["🆘 <b>Ближайшие группы:</b>\n"]

    for g in groups[:5]:
        t = "идёт сейчас" if g["delta"] <= 0 else f"через {g['delta']} мин"

        if g["type"] == "online":
            text.append(
                f"🌐 <b>{g['start']}</b> — {escape_html(g['name'])}\n"
                f"{t}\n"
                f"<a href=\"{g['url']}\">▶ Перейти</a>\n"
            )
        else:
            text.append(
                f"🏙 <b>{g['start']}</b> — {escape_html(g['name'])}\n"
                f"{t}\n"
                f"{escape_html(g['address'])}\n"
            )

    return "\n".join(text)


@dp.callback_query(F.data == "main_urgent")
async def main_urgent(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    now = moscow_now()

    groups = get_urgent_groups(data, now, 30)
    if not groups:
        groups = get_urgent_groups(data, now, 60)

    text = format_urgent_message(groups)

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data="main_urgent"),
        InlineKeyboardButton(text="⬅️ Главное меню", callback_data="main_menu"),
    )

    await safe_edit_text(callback.message, text, parse_mode="HTML", reply_markup=builder.as_markup())
    await safe_callback_answer(callback)


def collect_urgent_push(user_data: dict, now: datetime):
    groups = get_urgent_groups(user_data, now, 10)
    groups = [g for g in groups if is_favorite(user_data, g["name"])]

    if not groups:
        return None

    groups = groups[:2]

    text = ["🆘 <b>Сейчас есть группы:</b>\n"]

    for g in groups:
        if g["type"] == "online":
            text.append(f"🌐 <b>{g['start']}</b> — {escape_html(g['name'])}\n<a href=\"{g['url']}\">▶ Перейти</a>\n")
        else:
            text.append(f"🏙 <b>{g['start']}</b> — {escape_html(g['name'])}\n{escape_html(g['address'])}\n")

    return "\n".join(text)

# ===================================================
