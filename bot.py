import asyncio
import os
import random
import re
import json
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

# ==================== СОСТОЯНИЯ ====================
class LiveGroupSearch(StatesGroup):
    waiting_for_city = State()

class SubCitySetup(StatesGroup):
    waiting_for_city = State()

# ==================== ПОДПИСЧИКИ ====================
SUBSCRIBERS_FILE = "vda_subscribers.json"

def load_subscribers():
    try:
        with open(SUBSCRIBERS_FILE, "r") as f:
            return json.load(f)
    except:
        return {}

def save_subscribers(data):
    with open(SUBSCRIBERS_FILE, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_user_sub(uid: str) -> dict:
    subs = load_subscribers()
    return subs.get(uid, {"city": None, "all_online": False, "all_live": False, "groups": {}})

def set_user_sub(uid: str, data: dict):
    subs = load_subscribers()
    subs[uid] = data
    save_subscribers(subs)

def remove_subscriber(uid: str):
    subs = load_subscribers()
    subs.pop(uid, None)
    save_subscribers(subs)

def toggle_group_sub(uid: str, group_type: str, group_name: str, group_info: dict = None):
    """
    group_type: "online" или "live"
    group_name: название группы
    group_info: доп. информация (ссылка/адрес)
    """
    data = get_user_sub(uid)
    key = f"{group_type}:{group_name}"
    
    if group_name in data["groups"]:
        del data["groups"][group_name]
        added = False
    else:
        data["groups"][group_name] = {
            "type": group_type,
            "info": group_info or {},
        }
        added = True
    
    set_user_sub(uid, data)
    return added

def get_subscribed_groups(uid: str) -> list:
    """Возвращает список подписанных групп: [(type, name, info), ...]"""
    data = get_user_sub(uid)
    result = []
    for name, gdata in data["groups"].items():
        result.append((gdata["type"], name, gdata["info"]))
    return result

# ==================== КЛАВИАТУРЫ ====================
reply_main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🌐 Онлайн"), KeyboardButton(text="🏙 Живые"), KeyboardButton(text="💫 Установка")],
        [KeyboardButton(text="🔔 Подписка"), KeyboardButton(text="⭐ Мои группы"), KeyboardButton(text="🔕 Отписаться")],
    ],
    resize_keyboard=True,
    is_persistent=True,
)

# ==================== ВРЕМЯ ====================
def moscow_now():
    return datetime.utcnow() + timedelta(hours=3)

# ==================== ДАННЫЕ ====================
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
            elif "перв" in e and "первый" in e:
                occurrence = 1
            elif "втор" in e and "второй" in e:
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

# Строим реестр городов
city_to_id = {}
id_to_city = {}
city_id_counter = 0

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

for g in LIVE_GROUPS:
    city = g["city"]
    if city not in city_to_id:
        cid = str(city_id_counter)
        city_to_id[city] = cid
        id_to_city[cid] = city
        city_id_counter += 1

SPB_SUBURBS = [
    "Санкт-Петербург, город Пушкин", "Пушкин", "Петергоф", "Всеволожск",
    "Выборг", "Гатчина", "Колпино", "Кронштадт", "Ломоносов",
    "Павловск", "Сестрорецк", "Зеленогорск",
]

def get_searchable_cities(query: str) -> list:
    query_lower = query.lower().strip()
    is_spb_search = query_lower in ("санкт-петербург", "спб", "питер", "петербург")
    
    matched = []
    seen = set()
    
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

def format_online_group(time: str, name: str, url: str, add_sub_button: bool = False, uid: str = None) -> str:
    safe_name = escape_html(name)
    line = f'🟠 <b>{time}</b> — <a href="{url}">{safe_name}</a>'
    if add_sub_button and uid:
        is_sub = name in get_user_sub(uid)["groups"]
        emoji = "🔕" if is_sub else "🔔"
        line += f"  {emoji}"
    return line

def format_live_group(name, address, start, end, is_work_meeting=False, add_sub_button=False, uid=None):
    label = " 🔧 рабочее собрание" if is_work_meeting else ""
    line = f"• <b>{escape_html(name)}</b>{label} — {escape_html(address)} ({start}–{end})"
    if add_sub_button and uid:
        is_sub = name in get_user_sub(uid)["groups"]
        emoji = "🔕" if is_sub else "🔔"
        line += f"  {emoji}"
    return line

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

def get_live_groups_for_city(city: str):
    city_lower = city.lower().strip()
    return [g for g in LIVE_GROUPS if g["city"].lower() == city_lower]

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

# ==================== ФОНОВЫЕ ЗАДАЧИ ====================
async def background_tasks(bot: Bot):
    """Фоновая задача для утренней рассылки и напоминаний"""
    while True:
        now = moscow_now()
        subs = load_subscribers()
        
        if not subs:
            await asyncio.sleep(60)
            continue
        
        # Утренняя рассылка в 7:00
        if now.hour == 7 and now.minute == 0:
            for uid_str, data in subs.items():
                uid = int(uid_str)
                city = data.get("city")
                all_online = data.get("all_online", False)
                all_live = data.get("all_live", False)
                specific_groups = data.get("groups", {})
                
                text = f"☀️ <b>Доброе утро! Расписание на сегодня ({DAYS[now.weekday()]}):</b>\n\n"
                has_content = False
                
                # Все онлайн
                if all_online:
                    today_online = get_online_by_day(now.weekday())
                    if today_online:
                        text += "🌐 <b>Онлайн:</b>\n"
                        text += "\n".join(format_online_group(t, n, u) for t, n, u in today_online)
                        text += "\n\n"
                        has_content = True
                
                # Все живые
                if all_live and city:
                    live_groups = get_live_groups_for_day(city, now.weekday())
                    if live_groups:
                        text += f"🏙 <b>Живые в {escape_html(city)}:</b>\n"
                        text += "\n".join(format_live_group(n, a, s, e, w) for n, a, s, e, w in live_groups)
                        text += "\n\n"
                        has_content = True
                
                # Конкретные группы
                for gname, gdata in specific_groups.items():
                    gtype = gdata["type"]
                    info = gdata.get("info", {})
                    
                    if gtype == "online":
                        for day_groups in ONLINE_SCHEDULE.values():
                            for t, n, u in day_groups:
                                if n == gname:
                                    text += f"⭐ <b>{escape_html(n)}</b> 🌐\n"
                                    text += f"🕐 {t}\n🔗 {u}\n\n"
                                    has_content = True
                    elif gtype == "live" and city:
                        live_groups = get_live_groups_for_day(city, now.weekday())
                        for n, a, s, e, w in live_groups:
                            if n == gname:
                                work_label = " 🔧" if w else ""
                                text += f"⭐ <b>{escape_html(n)}{work_label}</b> 🏙\n"
                                text += f"🕐 {s}–{e}\n📍 {escape_html(a)}\n\n"
                                has_content = True
                
                if not has_content:
                    text += "Сегодня нет групп по вашей подписке."
                
                try:
                    await bot.send_message(uid, text, parse_mode="HTML", disable_web_page_preview=True)
                except:
                    pass
                await asyncio.sleep(0.05)
        
        # Напоминания за час
        if now.minute == 0:
            for uid_str, data in subs.items():
                uid = int(uid_str)
                specific_groups = data.get("groups", {})
                
                if not specific_groups:
                    continue
                
                # Проверяем онлайн-группы
                today_online = get_online_by_day(now.weekday())
                for time_str, name, url in today_online:
                    if name in specific_groups and specific_groups[name]["type"] == "online":
                        h, m = map(int, time_str.split(":"))
                        group_time = now.replace(hour=h, minute=m, second=0, microsecond=0)
                        delta = group_time - now
                        
                        if timedelta(hours=1) <= delta <= timedelta(hours=1, minutes=5):
                            try:
                                await bot.send_message(
                                    uid,
                                    f"🔔 <b>Через час — {escape_html(name)}</b> 🌐\n"
                                    f"🕐 Начало в {time_str} (МСК)\n"
                                    f"🔗 <a href='{url}'>Присоединиться</a>",
                                    parse_mode="HTML", disable_web_page_preview=True
                                )
                            except:
                                pass
                            await asyncio.sleep(0.05)
                
                # Проверяем живые группы
                city = data.get("city")
                if city:
                    live_groups = get_live_groups_for_day(city, now.weekday())
                    for name, address, start, end, is_work in live_groups:
                        if name in specific_groups and specific_groups[name]["type"] == "live":
                            h, m = map(int, start.split(":"))
                            group_time = now.replace(hour=h, minute=m, second=0, microsecond=0)
                            delta = group_time - now
                            
                            if timedelta(hours=1) <= delta <= timedelta(hours=1, minutes=5):
                                work_label = " 🔧" if is_work else ""
                                try:
                                    await bot.send_message(
                                        uid,
                                        f"🔔 <b>Через час — {escape_html(name)}{work_label}</b> 🏙\n"
                                        f"🕐 Начало в {start} (МСК)\n"
                                        f"📍 {escape_html(address)}",
                                        parse_mode="HTML"
                                    )
                                except:
                                    pass
                                await asyncio.sleep(0.05)
        
        await asyncio.sleep(60)

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

# --- Основные кнопки ---
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

# --- Подписка ---
@dp.message(F.text == "🔔 Подписка")
async def btn_subscribe_menu(message: Message):
    uid = str(message.from_user.id)
    data = get_user_sub(uid)
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text=f"🌐 Все онлайн {'✅' if data['all_online'] else '❌'}",
        callback_data="sub_toggle_all_online"
    ))
    builder.row(InlineKeyboardButton(
        text=f"🏙 Все живые в моём городе {'✅' if data['all_live'] else '❌'}",
        callback_data="sub_toggle_all_live"
    ))
    builder.row(InlineKeyboardButton(
        text="🏙 Выбрать город для подписки",
        callback_data="sub_set_city"
    ))
    builder.row(InlineKeyboardButton(
        text="⭐ Подписаться на конкретные группы",
        callback_data="sub_specific_groups"
    ))
    
    city = data.get("city")
    city_text = f"Текущий город: {escape_html(city)}" if city else "Город не выбран"
    
    await message.answer(
        f"🔔 <b>Настройка подписки</b>\n\n"
        f"{city_text}\n\n"
        f"Выберите, на что подписаться:",
        parse_mode="HTML",
        reply_markup=builder.as_markup()
    )

@dp.callback_query(F.data == "sub_toggle_all_online")
async def toggle_all_online(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    data["all_online"] = not data["all_online"]
    set_user_sub(uid, data)
    await callback.answer(f"Онлайн: {'✅ Вкл' if data['all_online'] else '❌ Выкл'}")
    await btn_subscribe_menu(callback.message)

@dp.callback_query(F.data == "sub_toggle_all_live")
async def toggle_all_live(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    if not data.get("city"):
        await callback.answer("⚠️ Сначала выберите город!", show_alert=True)
        return
    data["all_live"] = not data["all_live"]
    set_user_sub(uid, data)
    await callback.answer(f"Живые: {'✅ Вкл' if data['all_live'] else '❌ Выкл'}")
    await btn_subscribe_menu(callback.message)

@dp.callback_query(F.data == "sub_set_city")
async def sub_set_city(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SubCitySetup.waiting_for_city)
    await state.update_data(sub_mode=True, from_sub_menu=True)
    await callback.message.edit_text(
        "🔍 Введите название города для подписки на живые группы:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Назад", callback_data="sub_menu_back")]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data == "sub_specific_groups")
async def sub_specific_groups(callback: CallbackQuery):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🌐 Онлайн-группы", callback_data="sub_specific_online"))
    builder.row(InlineKeyboardButton(text="🏙 Живые группы", callback_data="sub_specific_live"))
    builder.row(InlineKeyboardButton(text="← Назад", callback_data="sub_menu_back"))
    await callback.message.edit_text(
        "⭐ <b>Подписка на конкретные группы</b>\n\nВыберите тип групп:",
        parse_mode="HTML",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data == "sub_menu_back")
async def sub_menu_back(callback: CallbackQuery):
    await callback.message.delete()
    await btn_subscribe_menu(callback.message)
    await callback.answer()

@dp.callback_query(F.data == "sub_specific_online")
async def sub_specific_online(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    builder = InlineKeyboardBuilder()
    
    all_online_names = set()
    for day_groups in ONLINE_SCHEDULE.values():
        for _, name, _ in day_groups:
            all_online_names.add(name)
    
    for name in sorted(all_online_names):
        is_sub = name in get_user_sub(uid)["groups"]
        emoji = "✅" if is_sub else "➕"
        builder.row(InlineKeyboardButton(
            text=f"{emoji} {name}",
            callback_data=f"sub_group_online_{name}"
        ))
    
    builder.row(InlineKeyboardButton(text="← Назад", callback_data="sub_specific_groups"))
    
    await callback.message.edit_text(
        "⭐ <b>Онлайн-группы</b>\n\n"
        "Нажмите на группу, чтобы подписаться/отписаться:",
        parse_mode="HTML",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data == "sub_specific_live")
async def sub_specific_live(callback: CallbackQuery, state: FSMContext):
    uid = str(callback.from_user.id)
    data = get_user_sub(uid)
    city = data.get("city")
    
    if not city:
        await callback.answer("⚠️ Сначала выберите город в настройках подписки!", show_alert=True)
        return
    
    city_groups = get_live_groups_for_city(city)
    builder = InlineKeyboardBuilder()
    
    all_names = set()
    for g in city_groups:
        all_names.add(g["name"])
    
    for name in sorted(all_names):
        is_sub = name in get_user_sub(uid)["groups"]
        emoji = "✅" if is_sub else "➕"
        builder.row(InlineKeyboardButton(
            text=f"{emoji} {name}",
            callback_data=f"sub_group_live_{name}"
        ))
    
    builder.row(InlineKeyboardButton(text="← Назад", callback_data="sub_specific_groups"))
    
    await callback.message.edit_text(
        f"⭐ <b>Живые группы в {escape_html(city)}</b>\n\n"
        "Нажмите на группу, чтобы подписаться/отписаться:",
        parse_mode="HTML",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("sub_group_"))
async def toggle_specific_group(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    parts = callback.data.split("_", 2)
    group_type = parts[2]  # "online" или "live"
    group_name = parts[3] if len(parts) > 3 else ""
    
    # Собираем имя группы (может содержать _)
    full_name = callback.data[len(f"sub_group_{group_type}_"):]
    
    info = {}
    if group_type == "online":
        for day_groups in ONLINE_SCHEDULE.values():
            for _, name, url in day_groups:
                if name == full_name:
                    info = {"url": url}
    else:
        for g in LIVE_GROUPS:
            if g["name"] == full_name:
                info = {"city": g["city"], "address": g["address"]}
    
    added = toggle_group_sub(uid, group_type, full_name, info)
    
    await callback.answer(f"{'✅ Подписаны' if added else '❌ Отписаны'} на {full_name}")
    
    # Обновляем текущее меню
    if group_type == "online":
        await sub_specific_online(callback)
    else:
        await sub_specific_live(callback)

@dp.message(StateFilter(SubCitySetup.waiting_for_city))
async def sub_city_input(message: Message, state: FSMContext):
    query = message.text.strip()
    matched = get_searchable_cities(query)
    await state.clear()
    
    if not matched:
        await message.answer("Город не найден. Попробуйте ещё раз.")
        return
    
    city = matched[0]  # Берем первый подходящий
    uid = str(message.from_user.id)
    data = get_user_sub(uid)
    data["city"] = city
    set_user_sub(uid, data)
    
    await message.answer(
        f"✅ Город для подписки: <b>{escape_html(city)}</b>\n\n"
        "Теперь вы можете подписаться на все живые группы в этом городе.",
        parse_mode="HTML"
    )

# --- Мои группы ---
@dp.message(F.text == "⭐ Мои группы")
async def btn_my_groups(message: Message):
    uid = str(message.from_user.id)
    data = get_user_sub(uid)
    
    text = "⭐ <b>Мои подписки</b>\n\n"
    
    if data["all_online"]:
        text += "🌐 Все онлайн-группы\n"
    if data["all_live"] and data.get("city"):
        text += f"🏙 Все живые в {escape_html(data['city'])}\n"
    
    specific = data.get("groups", {})
    if specific:
        text += "\n<b>Конкретные группы:</b>\n"
        for name, gdata in specific.items():
            gtype = gdata["type"]
            emoji = "🌐" if gtype == "online" else "🏙"
            text += f"{emoji} {escape_html(name)}\n"
    
    if not data["all_online"] and not data["all_live"] and not specific:
        text += "У вас пока нет подписок.\nНажмите «🔔 Подписка», чтобы настроить."
    
    await message.answer(text, parse_mode="HTML")

@dp.message(F.text == "🔕 Отписаться")
async def btn_unsubscribe_all(message: Message):
    uid = str(message.from_user.id)
    remove_subscriber(uid)
    await message.answer("🔕 Вы отписались от всех уведомлений.")

# --- Онлайн-расписание ---
@dp.callback_query(F.data == "online_today")
async def online_today(callback: CallbackQuery):
    day_index = moscow_now().weekday()
    groups = get_online_by_day(day_index)
    uid = str(callback.from_user.id)
    text = f"📅 <b>Онлайн-группы на сегодня ({DAYS[day_index]}):</b>\n\n"
    text += "\n".join(format_online_group(t, n, u, add_sub_button=True, uid=uid) for t, n, u in groups) if groups else "Сегодня групп нет."
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
    uid = str(callback.from_user.id)
    text = f"📅 <b>Онлайн-группы на {DAYS[day_index]}:</b>\n\n"
    text += "\n".join(format_online_group(t, n, u, add_sub_button=True, uid=uid) for t, n, u in groups) if groups else "В этот день групп нет."
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

@dp.callback_query(F.data.startswith("live_today_"))
async def live_today(callback: CallbackQuery):
    cid = callback.data[len("live_today_"):]
    city = id_to_city.get(cid, cid)
    day_index = moscow_now().weekday()
    groups = get_live_groups_for_day(city, day_index)
    uid = str(callback.from_user.id)
    text = f"📅 <b>Живые группы в {escape_html(city)} на сегодня ({DAYS[day_index]}):</b>\n\n"
    text += "\n".join(format_live_group(n, a, s, e, w, add_sub_button=True, uid=uid) for n, a, s, e, w in groups) if groups else "Сегодня групп нет."
    await callback.message.edit_text(text, parse_mode="HTML",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="← Назад", callback_data=f"live_period_{cid}")]
                                     ]))
    await callback.answer()

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
    uid = str(callback.from_user.id)
    text = f"📅 <b>Живые группы в {escape_html(city)} на {DAYS[day_index]}:</b>\n\n"
    text += "\n".join(format_live_group(n, a, s, e, w, add_sub_button=True, uid=uid) for n, a, s, e, w in groups) if groups else "В этот день групп нет."
    await callback.message.edit_text(text, parse_mode="HTML",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="← Назад", callback_data=f"live_choose_day_{cid}")]
                                     ]))
    await callback.answer()

@dp.callback_query(F.data == "live_search_city")
async def live_search_city_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(LiveGroupSearch.waiting_for_city)
    await state.update_data(sub_mode=False)
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
    matched_cities = get_searchable_cities(query)
    await state.clear()
    
    if not matched_cities:
        await message.answer(
            f"😔 Город <b>«{escape_html(query)}»</b> не найден.\n\n"
            "Попробуйте <b>онлайн-группы</b> — они доступны из любого города 🌐",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🌐 Онлайн-группы", callback_data="mode_online")],
                [InlineKeyboardButton(text="← Назад к городам", callback_data="mode_live")],
            ])
        )
        return
    
    if len(matched_cities) == 1:
        city = matched_cities[0]
        await message.answer(
            f"🏙 Город: <b>{escape_html(city)}</b>\nВыберите период:",
            parse_mode="HTML",
            reply_markup=live_period_keyboard(city)
        )
    else:
        builder = InlineKeyboardBuilder()
        for c in matched_cities[:20]:
            cid = city_to_id.get(c, c)
            builder.button(text=c, callback_data=f"live_city_{cid}")
        builder.adjust(1)
        builder.row(InlineKeyboardButton(text="← Назад к городам", callback_data="mode_live"))
        await message.answer(
            f"🔍 Найдено несколько городов. Уточните:",
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
        "🌐 Онлайн — расписание онлайн-групп\n"
        "🏙 Живые — очные собрания по городам\n"
        "💫 Установка — случайная аффирмация\n"
        "🔔 Подписка — настройка уведомлений\n"
        "⭐ Мои группы — просмотр подписок\n"
        "🔕 Отписаться — отключить уведомления\n\n"
        "Команды: /start, /help, /slogan",
        parse_mode="HTML",
        reply_markup=reply_main_menu
    )

@dp.message(Command("slogan"))
async def cmd_slogan(message: Message):
    slogan = random.choice(SLOGANS_AND_AFFIRMATIONS)
    await message.answer(f"💫 <b>Установка на день:</b>\n\n<i>«{escape_html(slogan)}»</i>", parse_mode="HTML")

@dp.message()
async def fallback_handler(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is not None:
        return
    await message.answer("Используйте кнопки меню 👇", reply_markup=reply_main_menu)

# ==================== ЗАПУСК ====================
async def main():
    if not BOT_TOKEN:
        print("❌ BOT_TOKEN не задан")
        return
    bot = Bot(token=BOT_TOKEN)

    # Запускаем фоновую задачу для уведомлений
    asyncio.create_task(background_tasks(bot))

    print("✅ Бот запущен с полной системой подписок")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
