import asyncio
import os
import random
import re
from datetime import datetime, timedelta
from typing import List, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

BOT_TOKEN = os.getenv("BOT_TOKEN")

class LiveGroupSearch(StatesGroup):
    waiting_for_city = State()

reply_main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🌐 Онлайн"), KeyboardButton(text="🏙 Живые"), KeyboardButton(text="💫 Установка")],
    ],
    resize_keyboard=True,
    is_persistent=True,
)

# ==================== ВРЕМЯ ====================
def moscow_now():
    return datetime.utcnow() + timedelta(hours=3)

# ==================== ДАННЫЕ ====================
ONLINE_SCHEDULE = {
    0: [  # Понедельник
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
    1: [  # Вторник
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
    2: [  # Среда
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
    3: [  # Четверг
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
    4: [  # Пятница
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
    5: [  # Суббота
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
    6: [  # Воскресенье
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

DAYS = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]

# --- Загрузка живых групп из внешнего файла ---
def parse_live_schedule(raw_lines: str):
    groups = []
    for line in raw_lines.strip().split("\n"):
        if not line.strip():
            continue
        parts = [c.strip() for c in line.split("\t")]
        if len(parts) < 5:
            continue
        country, city, name, address, time_str = parts
        if country != "Россия":
            continue
        time_str = time_str.replace('"', '').replace('\n', ' ')
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
                short_map = {"пн":0,"пон":0,"вт":1,"вто":1,"ср":2,"сре":2,"чт":3,"чет":3,"пт":4,"пят":4,"сб":5,"суб":5,"вс":6,"вос":6}
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
                    "raw": entry.strip(),
                })
        if days:
            groups.append({
                "city": city.strip(),
                "name": name.strip(),
                "address": address.strip(),
                "days": days
            })
    return groups

try:
    with open("live_groups.tsv", "r", encoding="utf-8") as f:
        raw_excel = f.read()
except FileNotFoundError:
    print("❌ Файл live_groups.tsv не найден. Живые группы не загружены.")
    raw_excel = ""
LIVE_GROUPS = parse_live_schedule(raw_excel)

# Строим реестр городов на основе данных из файла
city_to_id = {}
id_to_city = {}
city_id_counter = 0

# Сначала добавляем популярные города в нужном порядке
POPULAR_CITIES = [
    "Москва", "Санкт-Петербург", "Ростов-на-Дону", "Казань", "Новосибирск",
    "Екатеринбург", "Самара", "Нижний Новгород", "Краснодар", "Омск",
    "Калининград", "Воронеж", "Челябинск", "Красноярск", "Уфа",
    "Тюмень", "Ижевск", "Тольятти", "Хабаровск", "Иркутск"
]

for city in POPULAR_CITIES:
    cid = str(city_id_counter)
    city_to_id[city] = cid
    id_to_city[cid] = city
    city_id_counter += 1

# Добавляем все остальные уникальные города из файла
for g in LIVE_GROUPS:
    city = g["city"]
    if city not in city_to_id:
        cid = str(city_id_counter)
        city_to_id[city] = cid
        id_to_city[cid] = city
        city_id_counter += 1

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
    "Стоп — не будь Голодным, Злым, Одиноким и Уставшим",
    "Возвращайтесь снова и снова",
    "Назови, но не обвиняй",
    "Попроси о помощи и прими её",
    "Без чувств нет исцеления",
    "Сегодня я люблю и принимаю себя таким, какой я есть",
    "Сегодня я принимаю свои чувства",
    "Сегодня я делюсь своими чувствами",
    "Сегодня я позволяю себе совершать ошибки",
    "Сегодня мне достаточно того, кто я есть",
    "Сегодня я принимаю тебя таким, какой ты есть",
    "Сегодня я позволю жить другим",
    "Сегодня я попрошу мою Высшую Силу о поддержке и руководстве мной",
    "Сегодня я не стану обвинять ни тебя, ни себя",
    "Сегодня я имею право оберегать свои мысли, чувства и заботиться о своём теле",
    "Сегодня я смогу сказать «Нет» без чувства вины",
    "Сегодня я смогу сказать «Да» без чувства стыда",
    "Сегодня я желанный ребёнок любящих родителей",
    "Нормально знать, кто я есть",
    "Нормально доверять себе",
    "Нормально сказать: я взрослый ребёнок из дисфункциональной семьи",
    "Нормально знать другой способ жить",
    "Нормально отказывать без чувства вины",
    "Нормально дать себе передышку",
    "Нормально плакать от фильма или песни",
    "Мои чувства нормальны, даже если я их только учусь различать",
    "Нормально злиться",
    "Нормально веселиться и праздновать",
    "Нормально мечтать и надеяться",
    "Нормально отделяться с любовью",
    "Нормально заново учиться заботиться о себе",
    "Нормально сказать: я люблю себя",
    "Нормально работать по программе ВДА",
]

# ==================== ФОРМАТИРОВАНИЕ ====================
def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def format_online_group(time: str, name: str, url: str) -> str:
    safe_name = escape_html(name)
    return f'🟠 <b>{time}</b> — <a href="{url}">{safe_name}</a>'

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

def format_live_group(name, address, start, end, is_work_meeting=False):
    label = " 🔧 рабочее собрание" if is_work_meeting else ""
    return f"• <b>{escape_html(name)}</b>{label} — {escape_html(address)} ({start}–{end})"

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

def get_online_full():
    parts = []
    for i, day_name in enumerate(DAYS):
        groups = get_online_by_day(i)
        if groups:
            lines = [f"<b>{day_name}:</b>"]
            lines.extend(format_online_group(t, n, u) for t, n, u in groups)
            parts.append("\n".join(lines))
    return "\n\n".join(parts) if parts else "Онлайн-групп нет."

# ==================== КЛАВИАТУРЫ ====================
def online_menu_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📅 Сегодня", callback_data="online_today"),
                InlineKeyboardButton(text="📋 Полное", callback_data="online_full"))
    builder.row(InlineKeyboardButton(text="📆 Выбрать день", callback_data="online_choose_day"))
    return builder.as_markup()

def live_city_keyboard():
    builder = InlineKeyboardBuilder()
    for city in POPULAR_CITIES:
        cid = city_to_id.get(city, city)
        builder.button(text=city, callback_data=f"live_city_{cid}")
    builder.adjust(2)
    builder.row(InlineKeyboardButton(text="🔍 Найти свой город", callback_data="live_search_city"))
    return builder.as_markup()

def live_period_keyboard(city: str):
    cid = city_to_id.get(city, city)
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📅 Сегодня", callback_data=f"live_today_{cid}"),
                InlineKeyboardButton(text="📋 Вся неделя", callback_data=f"live_week_{cid}"))
    builder.row(InlineKeyboardButton(text="📆 Выбрать день", callback_data=f"live_choose_day_{cid}"))
    builder.row(InlineKeyboardButton(text="← Назад", callback_data="mode_live"))
    return builder.as_markup()

def get_days_keyboard(prefix: str, back_callback: str = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for i, day_name in enumerate(DAYS):
        builder.button(text=day_name, callback_data=f"{prefix}{i}")
    builder.adjust(2)
    if back_callback:
        builder.row(InlineKeyboardButton(text="← Назад", callback_data=back_callback))
    return builder.as_markup()

# ==================== ДИСПЕТЧЕР ====================
dp = Dispatcher(storage=MemoryStorage())

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "🕊 <b>Добро пожаловать в бот ВДА!</b>\n\n"
        "Используйте кнопки ниже для навигации.",
        parse_mode="HTML",
        reply_markup=reply_main_menu
    )

@dp.message(F.text == "🌐 Онлайн")
async def btn_online(message: Message):
    await message.answer("🌐 <b>Онлайн-расписание ВДА</b>", parse_mode="HTML", reply_markup=online_menu_keyboard())

@dp.message(F.text == "🏙 Живые")
async def btn_live(message: Message):
    await message.answer("🏙 <b>Выберите город:</b>", parse_mode="HTML", reply_markup=live_city_keyboard())

@dp.message(F.text == "💫 Установка")
async def btn_slogan(message: Message):
    slogan = random.choice(SLOGANS_AND_AFFIRMATIONS)
    await message.answer(f"💫 <b>Установка на день:</b>\n\n<i>«{escape_html(slogan)}»</i>", parse_mode="HTML")

# --- Онлайн-расписание ---
@dp.callback_query(F.data == "online_today")
async def online_today(callback: CallbackQuery):
    day_index = moscow_now().weekday()
    groups = get_online_by_day(day_index)
    text = f"📅 <b>Онлайн-группы на сегодня ({DAYS[day_index]}):</b>\n\n"
    text += "\n".join(format_online_group(t, n, u) for t, n, u in groups) if groups else "Сегодня групп нет."
    await callback.message.edit_text(text, parse_mode="HTML", disable_web_page_preview=True,
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="← Назад", callback_data="mode_online")]
                                     ]))
    await callback.answer()

@dp.callback_query(F.data == "online_full")
async def online_full(callback: CallbackQuery):
    full_text = get_online_full()
    parts = split_long_message(full_text)
    await callback.message.edit_text("📋 Полное расписание:")
    for idx, part in enumerate(parts):
        if idx == len(parts) - 1:
            await callback.message.answer(part, parse_mode="HTML", disable_web_page_preview=True,
                                          reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                              [InlineKeyboardButton(text="← Назад", callback_data="mode_online")]
                                          ]))
        else:
            await callback.message.answer(part, parse_mode="HTML", disable_web_page_preview=True)
    await callback.answer()

@dp.callback_query(F.data == "online_choose_day")
async def online_choose_day(callback: CallbackQuery):
    await callback.message.edit_text("📆 Выберите день недели:", reply_markup=get_days_keyboard("online_day_"))
    await callback.answer()

@dp.callback_query(F.data.startswith("online_day_"))
async def online_show_day(callback: CallbackQuery):
    day_index = int(callback.data.split("_")[-1])
    groups = get_online_by_day(day_index)
    text = f"📅 <b>Онлайн-группы на {DAYS[day_index]}:</b>\n\n"
    text += "\n".join(format_online_group(t, n, u) for t, n, u in groups) if groups else "В этот день групп нет."
    await callback.message.edit_text(text, parse_mode="HTML", disable_web_page_preview=True,
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="← Назад", callback_data="online_choose_day")]
                                     ]))
    await callback.answer()

# --- Живые группы ---
@dp.callback_query(F.data.startswith("live_city_"))
async def process_city_selection(callback: CallbackQuery):
    cid = callback.data[len("live_city_"):]
    city = id_to_city.get(cid, cid)
    await callback.message.edit_text(
        f"🏙 Город: <b>{escape_html(city)}</b>\nВыберите период:",
        parse_mode="HTML",
        reply_markup=live_period_keyboard(city)
    )
    await callback.answer()

@dp.callback_query(F.data == "mode_live")
async def back_to_live_cities(callback: CallbackQuery):
    await callback.message.edit_text("🏙 <b>Выберите город:</b>", parse_mode="HTML", reply_markup=live_city_keyboard())
    await callback.answer()

# Живые: сегодня
@dp.callback_query(F.data.startswith("live_today_"))
async def live_today(callback: CallbackQuery):
    cid = callback.data[len("live_today_"):]
    city = id_to_city.get(cid, cid)
    day_index = moscow_now().weekday()
    groups = get_live_groups_for_day(city, day_index)
    text = f"📅 <b>Живые группы в {escape_html(city)} на сегодня ({DAYS[day_index]}):</b>\n\n"
    text += "\n".join(format_live_group(n, a, s, e, w) for n, a, s, e, w in groups) if groups else "Сегодня групп нет."
    await callback.message.edit_text(text, parse_mode="HTML",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="← Назад", callback_data=f"live_period_{cid}")]
                                     ]))
    await callback.answer()

# Живые: вся неделя
@dp.callback_query(F.data.startswith("live_week_"))
async def live_week(callback: CallbackQuery):
    cid = callback.data[len("live_week_"):]
    city = id_to_city.get(cid, cid)
    text = get_live_week(city)
    await callback.message.edit_text(text, parse_mode="HTML",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="← Назад", callback_data=f"live_period_{cid}")]
                                     ]))
    await callback.answer()

# Живые: выбор дня
@dp.callback_query(F.data.startswith("live_period_"))
async def live_period_back(callback: CallbackQuery):
    cid = callback.data[len("live_period_"):]
    city = id_to_city.get(cid, cid)
    await callback.message.edit_text(
        f"🏙 Город: <b>{escape_html(city)}</b>\nВыберите период:",
        parse_mode="HTML",
        reply_markup=live_period_keyboard(city)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("live_choose_day_"))
async def live_choose_day(callback: CallbackQuery):
    cid = callback.data[len("live_choose_day_"):]
    city = id_to_city.get(cid, cid)
    await callback.message.edit_text(f"📆 Выберите день недели ({escape_html(city)}):",
                                     reply_markup=get_days_keyboard(f"live_day_{cid}_", back_callback=f"live_period_{cid}"))
    await callback.answer()

@dp.callback_query(F.data.startswith("live_day_"))
async def live_show_day(callback: CallbackQuery):
    parts = callback.data.split("_", 2)
    tail = parts[2]
    cid, idx_str = tail.rsplit("_", 1)
    city = id_to_city.get(cid, cid)
    day_index = int(idx_str)
    groups = get_live_groups_for_day(city, day_index)
    text = f"📅 <b>Живые группы в {escape_html(city)} на {DAYS[day_index]}:</b>\n\n"
    text += "\n".join(format_live_group(n, a, s, e, w) for n, a, s, e, w in groups) if groups else "В этот день групп нет."
    await callback.message.edit_text(text, parse_mode="HTML",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="← Назад", callback_data=f"live_choose_day_{cid}")]
                                     ]))
    await callback.answer()

# Поиск города
@dp.callback_query(F.data == "live_search_city")
async def live_search_city_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(LiveGroupSearch.waiting_for_city)
    await callback.message.edit_text(
        "🔍 Введите название вашего города:\n\n<i>Например: Томск, Пермь, Владивосток...</i>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Назад", callback_data="mode_live")]
        ])
    )
    await callback.answer()

@dp.message(StateFilter(LiveGroupSearch.waiting_for_city))
async def live_search_city_handle(message: Message, state: FSMContext):
    query = message.text.strip()
    query_lower = query.lower()

    matched_cities = []
    seen = set()
    for g in LIVE_GROUPS:
        city = g["city"]
        if query_lower in city.lower() and city not in seen:
            matched_cities.append(city)
            seen.add(city)

    await state.clear()

    if not matched_cities:
        await message.answer(
            f"😔 Город <b>«{escape_html(query)}»</b> не найден в расписании живых групп.\n\n"
            "Возможно, в вашем городе пока нет очных собраний ВДА, или они ещё не добавлены в базу.\n\n"
            "Попробуйте поискать <b>онлайн-группы</b> — они доступны из любого города 🌐",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🌐 Онлайн-группы", callback_data="mode_online")],
                [InlineKeyboardButton(text="← Назад к городам", callback_data="mode_live")],
            ])
        )
        return

    if len(matched_cities) == 1:
        city = matched_cities[0]
        cid = city_to_id.get(city, city)
        await message.answer(
            f"🏙 Город: <b>{escape_html(city)}</b>\nВыберите период:",
            parse_mode="HTML",
            reply_markup=live_period_keyboard(city)
        )
    else:
        builder = InlineKeyboardBuilder()
        for city in matched_cities[:20]:
            cid = city_to_id.get(city, city)
            builder.button(text=city, callback_data=f"live_city_{cid}")
        builder.adjust(1)
        builder.row(InlineKeyboardButton(text="← Назад к городам", callback_data="mode_live"))
        await message.answer(
            f"🔍 По запросу <b>«{escape_html(query)}»</b> найдено несколько городов. Уточните:",
            parse_mode="HTML",
            reply_markup=builder.as_markup()
        )

# --- Общие колбэки ---
@dp.callback_query(F.data == "mode_online")
async def back_to_online_menu(callback: CallbackQuery):
    await callback.message.edit_text("🌐 <b>Онлайн-расписание ВДА</b>", parse_mode="HTML", reply_markup=online_menu_keyboard())
    await callback.answer()

# Команды
@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "ℹ️ <b>О боте</b>\n\n"
        "Используйте кнопки:\n"
        "🌐 Онлайн — расписание онлайн-групп\n"
        "🏙 Живые — очные собрания по городам\n"
        "💫 Установка — случайная аффирмация\n"
        "Команды: /start, /help, /slogan",
        parse_mode="HTML",
        reply_markup=reply_main_menu
    )

@dp.message(Command("slogan"))
async def cmd_slogan(message: Message):
    slogan = random.choice(SLOGANS_AND_AFFIRMATIONS)
    await message.answer(f"💫 <b>Установка на день:</b>\n\n<i>«{escape_html(slogan)}»</i>", parse_mode="HTML")

# Fallback-обработчик
@dp.message()
async def fallback_handler(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is not None:
        return
    await message.answer(
        "Используйте кнопки меню 👇",
        reply_markup=reply_main_menu
    )

# ==================== ЗАПУСК ====================
async def main():
    if not BOT_TOKEN:
        print("❌ BOT_TOKEN не задан")
        return
    bot = Bot(token=BOT_TOKEN)
    print("✅ Бот запущен")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
