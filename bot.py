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

class SubCitySearch(StatesGroup):
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
    data = get_user_sub(uid)
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
    0: [("05:00","Восход","https://t.me/+gdi_B_ctmVJkMTAy"),("07:00","ВДА Утро","https://t.me/+KBt9VaElvMA4NTcy"),("07:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("08:00","Единство утро","https://t.me/ACAgroupUnityMoscow"),("08:00","Говори Доверяй Чувствуй","https://t.me/govori_vda"),("09:00","Доверие","https://t.me/VDADoverie"),("12:00","День за днём","https://t.me/+BwAsiX1KsGljZjQy"),("17:00","Шаг за шагом","https://t.me/joinchat/4SFNdPrxumNkYzky"),("18:00","Весна","https://t.me/vdavesna_2021"),("19:00","Рассвет","https://t.me/+OOw9IMnM5x1hNDJi"),("19:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("21:00","ДЫШИ!","https://t.me/breathelivebe"),("21:00","ВДА ВЕЧЕР","https://t.me/vda_vecher"),("21:00","Свобода","https://t.me/vda_svoboda")],
    1: [("05:00","Восход","https://t.me/+gdi_B_ctmVJkMTAy"),("07:00","ВДА Утро","https://t.me/+KBt9VaElvMA4NTcy"),("07:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("09:00","Доверие","https://t.me/VDADoverie"),("12:00","День за днём","https://t.me/+BwAsiX1KsGljZjQy"),("15:00","ВДА вокруг света","https://t.me/+nFn14RqYkyozZmUy"),("18:00","Весна","https://t.me/vdavesna_2021"),("19:00","Артплей","https://t.me/VDAartPlay"),("19:00","Рассвет","https://t.me/+OOw9IMnM5x1hNDJi"),("19:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("20:00","Феникс","https://t.me/+1GAp8vi4hyNmMzUy"),("20:00","По шагам Тони А.","https://t.me/+ajasg4oH0SU3MjFi"),("21:00","ДЫШИ!","https://t.me/breathelivebe"),("21:00","ВДА ВЕЧЕР","https://t.me/vda_vecher"),("21:00","Свобода","https://t.me/vda_svoboda")],
    2: [("05:00","Восход","https://t.me/+gdi_B_ctmVJkMTAy"),("07:00","ВДА Утро","https://t.me/+KBt9VaElvMA4NTcy"),("07:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("08:00","Единство утро","https://t.me/ACAgroupUnityMoscow"),("09:00","Доверие","https://t.me/VDADoverie"),("12:00","День за днём","https://t.me/+BwAsiX1KsGljZjQy"),("14:00","Венеция","https://t.me/joinchat/AocB9y6QC_k2ZjJi"),("18:00","Весна","https://t.me/vdavesna_2021"),("19:00","Рассвет","https://t.me/+OOw9IMnM5x1hNDJi"),("19:00","Вместе","https://chat.whatsapp.com/0CvEyMffhB60ZHQcShdva7"),("19:00","Точка Опоры","https://us06web.zoom.us/j/88678026186?pwd=QYiLNtlro6gEZ3f6eVZdwu7CAHbVF3.1"),("19:00","Светский круг (жен.)","https://t.me/+Pyarr0R7MSEyMGIy"),("19:00","ВДА-ВЕРА","https://t.me/+J2m1MAbQ818zNTFi"),("19:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("19:30","Эффект бабочки","https://t.me/+FcaUkHDOuMpkMTI8"),("20:00","Мужская ВДА","https://t.me/+ewtjezZaCtM5YTdi"),("20:00","Доверие (вопросы)","https://t.me/VDADoverie"),("21:00","ДЫШИ!","https://t.me/breathelivebe"),("21:00","ВДА ВЕЧЕР","https://t.me/vda_vecher"),("21:00","Свобода","https://t.me/vda_svoboda"),("22:00","Восст. Люб. Род.","https://us02web.zoom.us/j/86893102645?pwd=d2N1UWFDY3Y5RXBpTUdQcWpDdEZVUT09UT09")],
    3: [("05:00","Восход","https://t.me/+gdi_B_ctmVJkMTAy"),("07:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("07:00","ВДА Утро","https://t.me/+KBt9VaElvMA4NTcy"),("09:00","Доверие","https://t.me/VDADoverie"),("12:00","День за днём","https://t.me/+BwAsiX1KsGljZjQy"),("14:00","Венеция","https://t.me/joinchat/AocB9y6QC_k2ZjJi"),("18:00","Весна","https://t.me/vdavesna_2021"),("19:00","праВДА","https://t.me/+ZYfdfXWBRltjZGEy"),("19:00","ВДА в Рязани","https://t.me/+MHSRTpkJliw5YzUy"),("19:00","Артплей (онлайн)","https://t.me/VDAartPlay"),("19:00","Рассвет","https://t.me/+OOw9IMnM5x1hNDJi"),("19:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("21:00","ДЫШИ!","https://t.me/breathelivebe"),("21:00","ВДА ВЕЧЕР","https://t.me/vda_vecher"),("21:00","Свобода","https://t.me/vda_svoboda")],
    4: [("05:00","Восход","https://t.me/+gdi_B_ctmVJkMTAy"),("07:00","ВДА Утро","https://t.me/+KBt9VaElvMA4NTcy"),("07:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("08:00","Говори Доверяй Чувствуй","https://t.me/govori_vda"),("08:00","Единство утро","https://t.me/ACAgroupUnityMoscow"),("09:00","Доверие","https://t.me/VDADoverie"),("12:00","День за днём","https://t.me/+BwAsiX1KsGljZjQy"),("14:00","Венеция","https://t.me/joinchat/AocB9y6QC_k2ZjJi"),("14:00","ВДА вокруг света","https://t.me/+nFn14RqYkyozZmUy"),("18:00","Весна","https://t.me/vdavesna_2021"),("19:00","Братский Круг","https://t.me/+uEG2E5FVndA0YTc6"),("19:00","Рассвет","https://t.me/+OOw9IMnM5x1hNDJi"),("19:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("20:00","Феникс","https://t.me/+1GAp8vi4hyNmMzUy"),("20:00","Доверие (Любящий Родитель)","https://t.me/VDADoverie"),("21:00","ДЫШИ!","https://t.me/breathelivebe"),("21:00","ВДА ВЕЧЕР","https://t.me/vda_vecher"),("21:00","Свобода","https://t.me/vda_svoboda")],
    5: [("05:00","Восход","https://t.me/+gdi_B_ctmVJkMTAy"),("08:00","ВДА Утро","https://t.me/+KBt9VaElvMA4NTcy"),("09:00","Доверие","https://t.me/VDADoverie"),("12:00","День за днём","https://t.me/+BwAsiX1KsGljZjQy"),("14:00","Венеция","https://t.me/joinchat/AocB9y6QC_k2ZjJi"),("18:00","ВДА «Весь мир»","https://t.me/+zfYoUgHPiVRhN2Iy"),("18:00","Весна","https://t.me/vdavesna_2021"),("19:00","Девчата","https://t.me/+FKs5HqhF711iZTli"),("19:00","ВДА-ВЕРА","https://t.me/+J2m1MAbQ818zNTFi"),("19:00","Рассвет","https://t.me/+OOw9IMnM5x1hNDJi"),("20:00","Восст. Люб. Род.","https://us02web.zoom.us/j/86893102645?pwd=d2N1UWFDY3Y5RXBpTUdQcWpDdEZVUT09UT09"),("21:00","ВДА ВЕЧЕР","https://t.me/vda_vecher"),("21:00","Свобода","https://t.me/vda_svoboda")],
    6: [("05:00","Восход","https://t.me/+gdi_B_ctmVJkMTAy"),("07:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("08:00","ВДА Утро","https://t.me/+KBt9VaElvMA4NTcy"),("10:00","ВДА НСК онлайн","https://t.me/VDANsk"),("12:00","Только сегодня (MAX)","https://max.ru/join/y25EwyRl_K_F1OeJv5JbewpRpww71IKfWS-gwtTB65Q"),("12:00","День за днём","https://t.me/+BwAsiX1KsGljZjQy"),("12:30","Мужская ВДА","https://t.me/+ewtjezZaCtM5YTdi"),("14:00","Венеция","https://t.me/joinchat/AocB9y6QC_k2ZjJi"),("18:00","Ежедневник ВДА","https://t.me/VDAOXOTNIRYAD"),("18:00","Весна","https://t.me/vdavesna_2021"),("19:00","Сила и Надежда (Watsup)","https://chat.whatsapp.com/CUc0VVemIvl7Aoe2cuYCav"),("19:00","Рассвет","https://t.me/+OOw9IMnM5x1hNDJi"),("19:30","Эффект бабочки","https://t.me/+FcaUkHDOuMpkMTI8"),("20:00","Огоньки","https://t.me/ogonki2025"),("20:00","Маяк ВДА","https://t.me/+1XGQ4SDkR8M0N2Yy"),("21:00","ВДА ВЕЧЕР","https://t.me/vda_vecher"),("21:00","Свобода","https://t.me/vda_svoboda")],
}

DAYS = ["Понедельник","Вторник","Среда","Четверг","Пятница","Суббота","Воскресенье"]

# --- Живые группы ---
def parse_live_schedule(raw_lines: str):
    groups = []
    for line in raw_lines.strip().split("\n"):
        if not line.strip(): continue
        parts = [c.strip() for c in line.split("\t")]
        if len(parts) < 5: continue
        country, city, name, address, time_str = parts
        if country != "Россия": continue
        time_str = time_str.replace('"','').replace('\n',' ')
        entries = re.split(r";|\n| и ", time_str)
        days = []
        for entry in entries:
            entry = entry.strip()
            if not entry: continue
            day_found = None
            for i, day_name in enumerate(DAYS):
                if day_name.lower() in entry.lower() or day_name[:3].lower() in entry.lower():
                    day_found = i; break
            if day_found is None:
                short_map = {"пн":0,"пон":0,"вт":1,"вто":1,"ср":2,"сре":2,"чт":3,"чет":3,"пт":4,"пят":4,"сб":5,"суб":5,"вс":6,"вос":6}
                for key, idx in short_map.items():
                    if key in entry.lower(): day_found = idx; break
            if day_found is None: continue
            occurrence = None
            e = entry.lower()
            if "последн" in e: occurrence = "last"
            elif "перв" in e and "первый" in e: occurrence = 1
            elif "втор" in e and "второй" in e: occurrence = 2
            elif "треть" in e: occurrence = 3
            elif "четверт" in e and "четверг" not in e: occurrence = 4
            is_work_meeting = "рабоч" in e or "рабочка" in e
            times = re.findall(r"(\d{1,2}[.:]\d{2})\s*[-–]\s*(\d{1,2}[.:]\d{2})", entry)
            if not times: times = re.findall(r"с\s*(\d{1,2}[.:\-]\d{2})\s*до\s*(\d{1,2}[.:\-]\d{2})", entry)
            if not times:
                single = re.findall(r"в\s*(\d{1,2}[.:]\d{2})", entry)
                if not single: single = re.findall(r"(?<!\d)(\d{1,2}[.:]\d{2})(?!\d)", entry)
                if single:
                    start = single[0].replace(".",":")
                    h, m = start.split(":")
                    end_h = int(h) + 1
                    end = f"{end_h:02d}:{m}"
                    times = [(start, end)]
            if times:
                start, end = times[0]
                start = start.replace(".",":").replace("-",":")
                end = end.replace(".",":").replace("-",":")
                days.append({"day":day_found,"start":start,"end":end,"occurrence":occurrence,"is_work_meeting":is_work_meeting})
        if days:
            groups.append({"city":city.strip(),"name":name.strip(),"address":address.strip(),"days":days})
    return groups

try:
    with open("live_groups.tsv","r",encoding="utf-8") as f: raw_excel = f.read()
except FileNotFoundError:
    print("❌ live_groups.tsv не найден")
    raw_excel = ""
LIVE_GROUPS = parse_live_schedule(raw_excel)

# Реестр городов
city_to_id, id_to_city, city_id_counter = {}, {}, 0
POPULAR_CITIES = ["Москва","Санкт-Петербург","Ростов-на-Дону","Казань","Новосибирск","Екатеринбург","Самара","Нижний Новгород","Краснодар","Омск","Калининград","Воронеж","Челябинск","Красноярск","Уфа","Тюмень","Ижевск","Тольятти","Хабаровск","Иркутск"]
for city in POPULAR_CITIES:
    cid = str(city_id_counter); city_to_id[city] = cid; id_to_city[cid] = city; city_id_counter += 1
for g in LIVE_GROUPS:
    city = g["city"]
    if city not in city_to_id:
        cid = str(city_id_counter); city_to_id[city] = cid; id_to_city[cid] = city; city_id_counter += 1

SPB_SUBURBS = ["Санкт-Петербург, город Пушкин","Пушкин","Петергоф","Всеволожск","Выборг","Гатчина","Колпино","Кронштадт","Ломоносов","Павловск","Сестрорецк","Зеленогорск"]

def get_searchable_cities(query: str) -> list:
    query_lower = query.lower().strip()
    is_spb_search = query_lower in ("санкт-петербург","спб","питер","петербург")
    matched, seen = [], set()
    for g in LIVE_GROUPS:
        city = g["city"]; city_lower = city.lower()
        if query_lower in city_lower:
            if is_spb_search:
                is_suburb = False
                for suburb in SPB_SUBURBS:
                    if suburb.lower() in city_lower and suburb.lower() != "санкт-петербург":
                        is_suburb = True; break
                if is_suburb: continue
            if city not in seen: matched.append(city); seen.add(city)
    if not matched and not is_spb_search:
        for g in LIVE_GROUPS:
            for suburb in SPB_SUBURBS:
                if query_lower in suburb.lower() and query_lower in g["city"].lower():
                    if g["city"] not in seen: matched.append(g["city"]); seen.add(g["city"])
    return matched

SLOGANS_AND_AFFIRMATIONS = [
    "Программа простая, но не лёгкая","Жизнь больше, чем просто выживание","Можно жить по-другому","Только сегодня","Не суетись","Не усложняй","Прогресс, а не совершенство","Первым делом — главное","И эта боль тоже пройдет","Отпусти. Пусти Бога","Стоп — не будь Голодным, Злым, Одиноким и Уставшим","Возвращайтесь снова и снова","Назови, но не обвиняй","Попроси о помощи и прими её","Без чувств нет исцеления","Сегодня я люблю и принимаю себя таким, какой я есть","Сегодня я принимаю свои чувства","Сегодня я делюсь своими чувствами","Сегодня я позволяю себе совершать ошибки","Сегодня мне достаточно того, кто я есть","Сегодня я принимаю тебя таким, какой ты есть","Сегодня я позволю жить другим","Сегодня я попрошу мою Высшую Силу о поддержке и руководстве мной","Сегодня я не стану обвинять ни тебя, ни себя","Сегодня я имею право оберегать свои мысли, чувства и заботиться о своём теле","Сегодня я смогу сказать «Нет» без чувства вины","Сегодня я смогу сказать «Да» без чувства стыда","Сегодня я желанный ребёнок любящих родителей","Нормально знать, кто я есть","Нормально доверять себе","Нормально сказать: я взрослый ребёнок из дисфункциональной семьи","Нормально знать другой способ жить","Нормально отказывать без чувства вины","Нормально дать себе передышку","Нормально плакать от фильма или песни","Мои чувства нормальны, даже если я их только учусь различать","Нормально злиться","Нормально веселиться и праздновать","Нормально мечтать и надеяться","Нормально отделяться с любовью","Нормально заново учиться заботиться о себе","Нормально сказать: я люблю себя","Нормально работать по программе ВДА",
]

# ==================== ФОРМАТИРОВАНИЕ ====================
def escape_html(text: str) -> str:
    return text.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def format_online_group(time: str, name: str, url: str) -> str:
    return f'🟠 <b>{time}</b> — <a href="{url}">{escape_html(name)}</a>'

def format_online_group_with_sub(time: str, name: str, url: str, uid: str) -> str:
    data = get_user_sub(uid)
    bell = " 🔔" if name in data.get("groups",{}) else ""
    return f'🟠 <b>{time}</b> — <a href="{url}">{escape_html(name)}</a>{bell}'

def format_live_group(name, address, start, end, is_work_meeting=False):
    label = " 🔧" if is_work_meeting else ""
    return f"• <b>{escape_html(name)}</b>{label} — {escape_html(address)} ({start}–{end})"

def format_live_group_with_sub(name, address, start, end, is_work_meeting, uid):
    label = " 🔧" if is_work_meeting else ""
    data = get_user_sub(uid)
    bell = " 🔔" if name in data.get("groups",{}) else ""
    return f"• <b>{escape_html(name)}</b>{label}{bell} — {escape_html(address)} ({start}–{end})"

def split_long_message(text: str, limit: int = 3800) -> List[str]:
    parts = []
    while len(text) > limit:
        cut = text.rfind("\n",0,limit)
        if cut == -1: cut = limit
        parts.append(text[:cut].strip())
        text = text[cut:].lstrip("\n")
    if text.strip(): parts.append(text.strip())
    return parts

def week_of_month(dt): return ((dt.day - 1) // 7) + 1
def is_last_weekday_of_month(dt):
    next_same = dt + timedelta(days=7)
    return next_same.month != dt.month

def day_entry_matches_date(day_entry, target_date):
    if day_entry["day"] != target_date.weekday(): return False
    occurrence = day_entry.get("occurrence")
    if occurrence is None: return True
    if occurrence == "last": return is_last_weekday_of_month(target_date)
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
                result.append((g["name"],g["address"],entry["start"],entry["end"],entry.get("is_work_meeting",False)))
    return sorted(result, key=lambda x: x[2])

def get_live_week(city: str):
    city_groups = get_live_groups_for_city(city)
    if not city_groups: return f"В городе «{escape_html(city)}» живых групп не найдено."
    parts = [f"🏙 <b>Живые группы в {escape_html(city)}:</b>"]
    today = moscow_now().date()
    for offset in range(7):
        target_date = today + timedelta(days=offset)
        day_name = DAYS[target_date.weekday()]
        day_groups = []
        for g in city_groups:
            for entry in g["days"]:
                if day_entry_matches_date(entry, target_date):
                    day_groups.append((g["name"],g["address"],entry["start"],entry["end"],entry.get("is_work_meeting",False)))
        if day_groups:
            parts.append(f"<b>{day_name} ({target_date.strftime('%d.%m')}):</b>")
            parts.extend(format_live_group(n,a,s,e,w) for n,a,s,e,w in sorted(day_groups, key=lambda x: x[2]))
    return "\n".join(parts)

def get_online_by_day(day_index: int):
    return sorted(ONLINE_SCHEDULE.get(day_index,[]), key=lambda x: x[0])

def get_online_full():
    parts = []
    for i, day_name in enumerate(DAYS):
        groups = get_online_by_day(i)
        if groups:
            lines = [f"<b>{day_name}:</b>"]
            lines.extend(format_online_group(t,n,u) for t,n,u in groups)
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
        cid = city_to_id.get(city,city)
        builder.button(text=city, callback_data=f"live_city_{cid}")
    builder.adjust(2)
    builder.row(InlineKeyboardButton(text="🔍 Найти свой город", callback_data="live_search_city"))
    return builder.as_markup()

def live_period_keyboard(city: str):
    cid = city_to_id.get(city,city)
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

# ==================== ЗАПУСК ====================
async def main():
    if not BOT_TOKEN:
        print("❌ BOT_TOKEN не задан"); return
    bot = Bot(token=BOT_TOKEN)
    print("✅ Бот запущен")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
