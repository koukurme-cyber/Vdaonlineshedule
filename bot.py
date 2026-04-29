import asyncio
import os
import random
import json
from datetime import datetime, timedelta
from typing import List, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from zoneinfo import ZoneInfo

BOT_TOKEN = os.getenv("BOT_TOKEN", "ваш_токен_здесь")

MOSCOW_TZ = ZoneInfo("Europe/Moscow")

# ==================== РАСПИСАНИЕ (обновлённое) ====================

SCHEDULE = {
    0: [  # Понедельник
        ("09:00", "Небо на Обводном", "CoDA", "наб. Обводного канала, 116, м. Балтийская, храм Воскресения Христова. +7 911 952 7073"),
        ("15:00", "Выход", "ВДА", "м. Технологический институт, ул. Егорова, 11, цоколь, помещение «Хаски». +7 911 995-25-35, Евгения"),
        ("18:30", "ЮТА", "UAA", "адрес уточняйте на uaarus.ru"),
        ("18:45", "Путь к себе", "CoDA", "Б. Подъяческая, 34, м. Сенная/Садовая/Спасская, РБОО «АЗАРИЯ». +7 911 928 67 10"),
        ("19:00", "Подруги", "ВДА", "м. Владимирская/Достоевская, Щербаков пер., 12, ДЦ Владимирский. t.me/spb_aca_girls"),
        ("19:00", "Феникс", "ВДА", "м. Василеостровская/Спортивная, 2-я линия В.О., 3, Информационный кабинет. +7 965 787-05-74"),
        ("20:15", "Замысел", "UAA", "м. Сенная/Садовая/Спасская, ул. Ефимова, 6"),
        ("20:20", "Невская", "ВДА", "м. Ломоносовская, пер. Челиева, 10Б, Подворье Свято-Троицкого монастыря. +7 911 964-73-58"),
    ],
    1: [  # Вторник
        ("16:00", "Чёрная речка", "ВДА", "м. Чёрная речка, Сестрорецкая ул., 2, вход через магазин «Рыбалка». t.me/+la-lJbgvZ..."),
        ("17:30", "Лотос", "ВДА", "м. Ленинский пр., Трамвайный пр., 12к2, 5 эт., ком. 33. +7 921 904-49-86"),
        ("18:45", "Вместе", "ВДА", "м. Технологический институт, 1-я Красноармейская ул., 11, цоколь. +7 921 633-43-32, Алёна"),
        ("18:45", "Черта", "ВДА", "м. Пл. Ленина/Пл. Мужества, Пискарёвский пр., 25, 4 эт., пом. 5. t.me/+pktT8AlhN..."),
        ("19:00", "Взрослые девочки", "ВДА", "м. Василеостровская, 13-я линия В.О., 2, кв. 46, синяя дверь во дворе. +7 911 156-54-77"),
        ("19:00", "Единство Невский проспект", "UAA", "м. Невский проспект/Гостиный двор, ул. Садовая, 11, 3 эт., зал 11, код #3239"),
        ("19:00", "Петрополис", "АНЗ", "м. Чкаловская, ул. Колпинская, 27, Малый зал"),
        ("19:00", "Небо на Обводном", "CoDA", "наб. Обводного канала, 116, м. Балтийская. +7 911 952 7073"),
        ("19:10", "Путь к себе", "CoDA", "Б. Подъяческая, 34, м. Сенная/Садовая/Спасская, РБОО «АЗАРИЯ». +7 911 928 67 10"),
        ("20:15", "Замысел", "UAA", "м. Сенная/Садовая/Спасская, ул. Ефимова, 6"),
        ("20:20", "Тепло (Азария)", "ВДА", "м. Садовая/Технологический институт, Б. Подъяческая, 34, вход со двора"),
        ("20:20", "Небо на Блюхера", "CoDA", "пр. Маршала Блюхера, 9, корп. 2, м. Лесная. +7 911 952 7073"),
    ],
    2: [  # Среда
        ("17:00", "13 линия", "ВДА", "м. Василеостровская, 13-я линия В.О., 2, кв. 46. vk.com/club117132895"),
        ("17:00", "Участие на Юго-Западе", "CoDA", "м. Автово, ул. Маршала Захарова, 13, 3 подъезд, цоколь. +7 981 945 54 37"),
        ("18:00", "Солнечная сторона", "ВДА", "м. Чернышевская, ул. Радищева, 30, Пространство «Просвет», зал Просвещение. t.me/+qFRjoIzA_..."),
        ("19:00", "Здравствуй, Я!", "ВДА", "м. Балтийская/Фрунзенская, наб. Обводного канала, 116, ауд. 121. +7 906 246-56-93, Марина"),
        ("19:00", "Прометей", "ВДА", "м. Московская/Пр. Славы, пр. Славы, 4, 2 эт. +7 911 277-49-11"),
        ("19:00", "Феникс", "ВДА", "м. Василеостровская/Спортивная, 2-я линия В.О., 3. +7 965 787-05-74"),
        ("19:10", "Путь к себе", "CoDA", "Б. Подъяческая, 34, м. Сенная/Садовая/Спасская, РБОО «АЗАРИЯ». +7 911 928 67 10"),
        ("19:15", "Ручей", "ВДА", "м. Чкаловская, ул. Колпинская, 27, пространство «Ручей». Рабочее собрание в посл. среду 20:15"),
        ("20:15", "Замысел", "UAA", "м. Сенная/Садовая/Спасская, ул. Ефимова, 6"),
        ("20:20", "Тепло (Азария)", "ВДА", "м. Садовая/Технологический институт, Б. Подъяческая, 34, вход со двора"),
        ("20:20", "Небо на В.О.", "CoDA", "наб. Лейтенанта Шмидта, 39, м. Василеостровская. +7 911 952 7073"),
    ],
    3: [  # Четверг
        ("18:00", "Свобода смысла", "ВДА", "м. Сенная/Садовая/Спасская, ул. Ефимова, 6, домофон 57, 2 эт. t.me/+b14S_QR3Q..."),
        ("18:45", "Черта", "ВДА", "м. Пл. Ленина/Пл. Мужества, Пискарёвский пр., 25, 4 эт., пом. 5"),
        ("19:00", "Эфир", "ВДА", "м. Московская, ул. Варшавская, 122, каб. 10. +7 999 037-13-05"),
        ("19:00", "PRO ЖИЗНЬ", "CoDA", "ул. Казанская, 52, м. Сенная/Садовая/Спасская. +7 911 928 67 10"),
        ("19:00", "Небо на Обводном", "CoDA", "наб. Обводного канала, 116, м. Балтийская. +7 911 952 7073"),
        ("20:00", "Тепло (Азария)", "ВДА", "м. Садовая/Технологический институт, Б. Подъяческая, 34, вход со двора"),
        ("20:15", "Замысел", "UAA", "м. Сенная/Садовая/Спасская, ул. Ефимова, 6"),
        ("20:20", "Небо на Блюхера", "CoDA", "пр. Маршала Блюхера, 9, корп. 2, м. Лесная. +7 911 952 7073"),
    ],
    4: [  # Пятница
        ("18:10", "Ручей", "ВДА", "м. Чкаловская, ул. Колпинская, 27, пространство «Ручей»"),
        ("18:30", "ЮТА", "UAA", "адрес уточняйте на uaarus.ru"),
        ("19:00", "Феникс", "ВДА", "м. Василеостровская/Спортивная, 2-я линия В.О., 3"),
        ("19:00", "Тепло (Азария)", "ВДА", "м. Садовая/Технологический институт, Б. Подъяческая, 34, вход со двора"),
        ("19:00", "Любящий родитель", "ВДА", "м. Балтийская, наб. Обводного канала, 116, Лекционка в храме. +7 952 236 77 44, Валерия"),
        ("19:00", "Сознание Пушкин", "CoDA", "г. Пушкин, ул. Магазейная, 22/30, полуподвал. +7 911 711 80 81"),
        ("19:00", "Небо на Обводном", "CoDA", "наб. Обводного канала, 116, м. Балтийская. +7 911 952 7073"),
        ("19:00", "Солнечная", "CoDA", "м. Гостиный двор/Невский проспект, ул. Садовая, 7-9-11, пространство «Ступени», 3 эт., оф. 38, каб. 7"),
        ("20:15", "Замысел", "UAA", "м. Сенная/Садовая/Спасская, ул. Ефимова, 6"),
        ("20:20", "Путь к себе", "CoDA", "Б. Подъяческая, 34, м. Сенная/Садовая/Спасская, РБОО «АЗАРИЯ». +7 911 928 67 10"),
    ],
    5: [  # Суббота
        ("11:00", "Взрослые девочки", "ВДА", "м. Василеостровская, 13-я линия В.О., 2, кв. 46. +7 911 156-54-77"),
        ("13:00", "13 линия", "ВДА", "м. Василеостровская, 13-я линия В.О., 2, кв. 46. vk.com/club117132895"),
        ("13:00", "Петрополис", "АНЗ", "м. Чкаловская, ул. Колпинская, 27, Малый зал"),
        ("14:00", "Свобода смысла", "ВДА", "м. Сенная/Садовая/Спасская, ул. Ефимова, 6. t.me/+b14S_QR3Q..."),
        ("14:30", "Участие на Юго-Западе", "CoDA", "м. Автово, ул. Маршала Захарова, 13, 3 подъезд, цоколь. +7 981 945 54 37"),
        ("15:00", "Солнечная", "CoDA", "м. Невский проспект, Инженерная ул., 6, бизнес-центр, 2 эт., каб. 14"),
        ("16:00", "Говори, доверяй, чувствуй", "ВДА", "м. Пр. Просвещения, пр. Энгельса, 132, подвал во дворе у «Бристоль». +7 905 274-55-50, Алёна"),
        ("17:00", "Парнас", "ВДА", "м. Парнас, 6-й Верхний пер., 12Б, 5 эт., оф. 2. +7 921 434-05-66"),
        ("18:30", "ЮТА", "UAA", "адрес уточняйте на uaarus.ru"),
        ("19:00", "Солнечная сторона", "ВДА", "м. Чернышевская, ул. Радищева, 30, Пространство «Просвет»"),
        ("19:00", "Феникс", "ВДА", "м. Василеостровская/Спортивная, 2-я линия В.О., 3"),
        ("20:15", "Замысел", "UAA", "м. Сенная/Садовая/Спасская, ул. Ефимова, 6"),
    ],
    6: [  # Воскресенье
        ("11:00", "Взрослые девочки", "ВДА", "м. Василеостровская, 13-я линия В.О., 2, кв. 46. +7 911 156-54-77"),
        ("12:00", "Мост", "CoDA", "г. Красное Село, ул. Массальского, 3. +7 952 236-39-81"),
        ("13:00", "Черта", "ВДА", "м. Пл. Ленина/Пл. Мужества, Пискарёвский пр., 25, 4 эт., пом. 5"),
        ("14:30", "Любящий родитель (практика)", "ВДА", "м. Пл. Ленина/Пл. Мужества, в рамках группы «Черта», Пискарёвский пр., 25"),
        ("15:00", "Воскресенье", "ВДА", "м. Автово, ул. Маршала Захарова, 13, 3 подъезд, цоколь. +7 918 351-04-44, Елена"),
        ("15:10", "Феникс", "ВДА", "м. Василеостровская/Спортивная, 2-я линия В.О., 3"),
        ("16:30", "Все свободны", "ВДА", "м. Чернышевская, ул. Таврическая, 5. t.me/vsefree_vda"),
        ("17:00", "проСВЕТ", "ВДА", "м. Пр. Просвещения, Выборгское ш., 15, ТЦ «Авеню», 3 эт. +7 999 202-91-71"),
        ("17:00", "Небо на В.О.", "CoDA", "наб. Лейтенанта Шмидта, 39, м. Василеостровская. +7 911 952 7073"),
        ("18:00", "Прометей", "ВДА", "м. Московская/Пр. Славы, пр. Славы, 4, 2 эт. +7 911 277-49-11"),
        ("18:30", "Путь к себе", "CoDA", "Б. Подъяческая, 34, м. Сенная/Садовая/Спасская. +7 911 928 67 10"),
        ("19:00", "Единство Невский проспект", "UAA", "м. Невский проспект/Гостиный двор, ул. Садовая, 11, 3 эт., зал 11, код #3239"),
        ("19:30", "PRO ЖИЗНЬ", "CoDA", "ул. Казанская, 52, м. Сенная/Садовая/Спасская. +7 911 928 67 10"),
        ("20:15", "Замысел", "UAA", "м. Сенная/Садовая/Спасская, ул. Ефимова, 6"),
    ],
}

DAYS = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]

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

TYPE_EMOJI = {
    "ВДА": "🟠",
    "CoDA": "🔵",
    "UAA": "🟢",
    "АНЗ": "🟡",
}

# ==================== ФОРМАТИРОВАНИЕ ====================

def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def format_group(time: str, name: str, kind: str, address: str) -> str:
    emoji = TYPE_EMOJI.get(kind, "⚪")
    safe_name = escape_html(name)
    safe_addr = escape_html(address)
    return f"{emoji} <b>{time}</b> — {safe_name} [{kind}]\n   📍 {safe_addr}"


def format_groups(groups: List[Tuple[str, str, str, str]], title: str = "") -> str:
    if not groups:
        return f"<i>{title}</i>\n\nГрупп не найдено."
    lines = [f"<b>{title}</b>\n"] if title else []
    lines.extend(format_group(*g) for g in groups)
    return "\n".join(lines)


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


# ==================== РАБОТА С ПОДПИСЧИКАМИ ====================

SUBSCRIBERS_FILE = "subscribers.json"

def load_subscribers() -> List[int]:
    try:
        with open(SUBSCRIBERS_FILE, "r") as f:
            data = json.load(f)
            return [int(uid) for uid in data]
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def save_subscribers(subscribers: List[int]):
    with open(SUBSCRIBERS_FILE, "w") as f:
        json.dump(subscribers, f)

def add_subscriber(user_id: int):
    subs = load_subscribers()
    if user_id not in subs:
        subs.append(user_id)
        save_subscribers(subs)

def remove_subscriber(user_id: int):
    subs = load_subscribers()
    if user_id in subs:
        subs.remove(user_id)
        save_subscribers(subs)


# ==================== БИЗНЕС-ЛОГИКА ====================

class ScheduleService:
    @staticmethod
    def get_today() -> Tuple[str, List]:
        day_index = datetime.now(MOSCOW_TZ).weekday()
        return DAYS[day_index], SCHEDULE.get(day_index, [])

    @staticmethod
    def get_by_day(day_index: int) -> Tuple[str, List]:
        if 0 <= day_index <= 6:
            return DAYS[day_index], SCHEDULE.get(day_index, [])
        return "", []

    @staticmethod
    def get_by_type(groups: List, kind: str) -> List:
        return [g for g in groups if g[2].upper() == kind.upper()]

    @staticmethod
    def get_by_metro(metro_station: str) -> List[Tuple[int, str, List]]:
        station_lower = metro_station.lower().strip()
        result = []
        for day_idx, groups in SCHEDULE.items():
            filtered = [g for g in groups if station_lower in g[3].lower()]
            if filtered:
                result.append((day_idx, DAYS[day_idx], filtered))
        return result

    @staticmethod
    def get_full_schedule() -> str:
        parts = []
        for i, day_name in enumerate(DAYS):
            entries = sorted(SCHEDULE[i], key=lambda x: x[0])
            if entries:
                parts.append(format_groups(entries, f"{day_name}:"))
        return "\n\n".join(parts)


# ==================== КЛАВИАТУРЫ ====================

def get_menu_keyboard() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="📅 Сегодня"), KeyboardButton(text="📋 Полное расписание"))
    builder.row(KeyboardButton(text="🟠 ВДА сегодня"), KeyboardButton(text="🔵 CoDA сегодня"))
    builder.row(KeyboardButton(text="🟢 UAA сегодня"), KeyboardButton(text="🟡 АНЗ сегодня"))
    builder.row(KeyboardButton(text="🚇 Поиск по метро"), KeyboardButton(text="💫 Установка на день"))
    builder.row(KeyboardButton(text="🔔 Подписаться"), KeyboardButton(text="🔕 Отписаться"))
    return builder.as_markup(resize_keyboard=True)


def get_days_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Пн", callback_data="day_0"),
                InlineKeyboardButton(text="Вт", callback_data="day_1"),
                InlineKeyboardButton(text="Ср", callback_data="day_2"),
                InlineKeyboardButton(text="Чт", callback_data="day_3"),
            ],
            [
                InlineKeyboardButton(text="Пт", callback_data="day_4"),
                InlineKeyboardButton(text="Сб", callback_data="day_5"),
                InlineKeyboardButton(text="Вс", callback_data="day_6"),
            ],
        ]
    )


# ==================== ДИСПЕТЧЕР ====================

dp = Dispatcher()


# ==================== КОМАНДЫ ====================

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "🕊 <b>Добро пожаловать в бот-помощник 12-шагового сообщества!</b>\n\n"
        "Здесь ты найдёшь расписание групп ВДА, CoDA, UAA и АНЗ в Санкт-Петербурге.\n\n"
        "<i>«Жизнь больше, чем просто выживание»</i>\n\n"
        "Выбери действие на клавиатуре или используй команды:",
        parse_mode="HTML",
        reply_markup=get_menu_keyboard(),
    )
    await message.answer(
        "/today — группы на сегодня\n"
        "/full — полное расписание\n"
        "/metro — поиск по станции метро\n"
        "/slogan — установка на день\n"
        "/subscribe — подписаться на уведомления\n"
        "/unsubscribe — отписаться\n"
        "/help — помощь"
    )


@dp.message(Command("today"))
async def cmd_today(message: Message):
    day_name, groups = ScheduleService.get_today()
    text = format_groups(groups, f"📅 Группы на сегодня ({day_name}):")
    await send_long_message(message, text)


@dp.message(Command("full"))
async def cmd_full(message: Message):
    text = "📋 <b>Полное расписание на неделю:</b>\n\n" + ScheduleService.get_full_schedule()
    await send_long_message(message, text)


@dp.message(Command("slogan"))
async def cmd_slogan(message: Message):
    slogan = random.choice(SLOGANS_AND_AFFIRMATIONS)
    await message.answer(
        f"💫 <b>Установка на день:</b>\n\n<i>«{escape_html(slogan)}»</i>",
        parse_mode="HTML"
    )


@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "📖 <b>Справка:</b>\n\n"
        "<b>Основные команды:</b>\n"
        "/start — перезапуск бота\n"
        "/today — группы на сегодня\n"
        "/full — полное расписание\n"
        "/metro [станция] — поиск по метро\n"
        "/slogan — установка на день\n\n"
        "<b>Уведомления:</b>\n"
        "/subscribe — подписаться на утреннюю рассылку и напоминания\n"
        "/unsubscribe — отписаться\n\n"
        "<b>Фильтры по типам:</b>\n"
        "/vda — ВДА сегодня\n"
        "/coda — CoDA сегодня\n"
        "/uaa — UAA сегодня\n"
        "/anz — АНЗ сегодня\n\n"
        "Или используй кнопки меню.",
        parse_mode="HTML",
        reply_markup=get_menu_keyboard(),
    )


# ==================== ФИЛЬТРЫ ПО ТИПАМ ====================

@dp.message(Command("vda"))
async def cmd_vda_today(message: Message):
    _, groups = ScheduleService.get_today()
    filtered = ScheduleService.get_by_type(groups, "ВДА")
    text = format_groups(filtered, "🟠 Группы ВДА сегодня:")
    await send_long_message(message, text)

@dp.message(Command("coda"))
async def cmd_coda_today(message: Message):
    _, groups = ScheduleService.get_today()
    filtered = ScheduleService.get_by_type(groups, "CoDA")
    text = format_groups(filtered, "🔵 Группы CoDA сегодня:")
    await send_long_message(message, text)

@dp.message(Command("uaa"))
async def cmd_uaa_today(message: Message):
    _, groups = ScheduleService.get_today()
    filtered = ScheduleService.get_by_type(groups, "UAA")
    text = format_groups(filtered, "🟢 Группы UAA сегодня:")
    await send_long_message(message, text)

@dp.message(Command("anz"))
async def cmd_anz_today(message: Message):
    _, groups = ScheduleService.get_today()
    filtered = ScheduleService.get_by_type(groups, "АНЗ")
    text = format_groups(filtered, "🟡 Группы АНЗ сегодня:")
    await send_long_message(message, text)


# ==================== ПОИСК ПО МЕТРО ====================

@dp.message(Command("metro"))
async def cmd_metro(message: Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer(
            "🚇 <b>Поиск групп по станции метро</b>\n\n"
            "Введите название станции после команды, например:\n"
            "<code>/metro Чкаловская</code>\n\n"
            "Или нажмите кнопку «🚇 Поиск по метро» и введите станцию в следующем сообщении.",
            parse_mode="HTML"
        )
        return
    station = args[1]
    await process_metro_search(message, station)


async def process_metro_search(message: Message, station: str):
    results = ScheduleService.get_by_metro(station)
    if not results:
        await message.answer(f"🚇 По запросу «{escape_html(station)}» групп не найдено.")
        return
    answer = f"🚇 <b>Группы рядом со станцией метро «{escape_html(station)}»:</b>\n\n"
    for day_idx, day_name, groups in results:
        answer += format_groups(groups, f"{day_name}:") + "\n\n"
    await send_long_message(message, answer.strip())


@dp.message(F.text == "🚇 Поиск по метро")
async def btn_metro_start(message: Message):
    await message.answer("Введите название станции метро (или часть названия) или используйте команду /metro <станция>.")


# ==================== ПОДПИСКА / ОТПИСКА ====================

@dp.message(Command("subscribe"))
async def cmd_subscribe(message: Message):
    add_subscriber(message.from_user.id)
    await message.answer("✅ Вы подписались на утреннюю рассылку и напоминания за 2 часа до групп.")


@dp.message(Command("unsubscribe"))
async def cmd_unsubscribe(message: Message):
    remove_subscriber(message.from_user.id)
    await message.answer("🔕 Вы отписались от уведомлений.")


@dp.message(F.text == "🔔 Подписаться")
async def btn_subscribe(message: Message):
    await cmd_subscribe(message)


@dp.message(F.text == "🔕 Отписаться")
async def btn_unsubscribe(message: Message):
    await cmd_unsubscribe(message)


# ==================== КНОПКИ МЕНЮ ====================

@dp.message(F.text == "📅 Сегодня")
async def btn_today(message: Message): await cmd_today(message)
@dp.message(F.text == "📋 Полное расписание")
async def btn_full(message: Message): await cmd_full(message)
@dp.message(F.text == "🟠 ВДА сегодня")
async def btn_vda(message: Message): await cmd_vda_today(message)
@dp.message(F.text == "🔵 CoDA сегодня")
async def btn_coda(message: Message): await cmd_coda_today(message)
@dp.message(F.text == "🟢 UAA сегодня")
async def btn_uaa(message: Message): await cmd_uaa_today(message)
@dp.message(F.text == "🟡 АНЗ сегодня")
async def btn_anz(message: Message): await cmd_anz_today(message)
@dp.message(F.text == "💫 Установка на день")
async def btn_slogan(message: Message): await cmd_slogan(message)

@dp.message(F.text == "📆 Выбрать день")
async def btn_choose_day(message: Message):
    await message.answer("📆 Выбери день недели:", reply_markup=get_days_keyboard())

@dp.callback_query(F.data.startswith("day_"))
async def process_day_callback(callback: CallbackQuery):
    await callback.answer()
    day_index = int(callback.data.split("_")[1])
    day_name, groups = ScheduleService.get_by_day(day_index)
    text = format_groups(groups, f"📅 {day_name}:") if groups else f"📅 <b>{day_name}:</b>\n\n<i>В этот день групп нет.</i>"
    await send_long_message(callback.message, text)


# ==================== ФОНОВЫЕ ЗАДАЧИ ====================

async def check_upcoming_groups(bot: Bot):
    now = datetime.now(MOSCOW_TZ)
    today_index = now.weekday()
    todays_groups = SCHEDULE.get(today_index, [])
    upcoming = []
    for group in todays_groups:
        time_str = group[0]
        hour, minute = map(int, time_str.split(":"))
        group_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        delta = group_time - now
        if timedelta(hours=2) <= delta <= timedelta(hours=2, minutes=15):
            upcoming.append(group)
    if upcoming:
        text = format_groups(upcoming, "🔔 Через 2 часа начинаются группы:")
        subscribers = load_subscribers()
        for uid in subscribers:
            try:
                await bot.send_message(uid, text, parse_mode="HTML")
                await asyncio.sleep(0.05)
            except Exception:
                pass

async def morning_mailing(bot: Bot):
    now = datetime.now(MOSCOW_TZ)
    if now.hour == 7 and now.minute == 0:
        day_name, groups = ScheduleService.get_today()
        text = f"☀️ <b>Доброе утро!</b>\n\n{format_groups(groups, f'Расписание на сегодня ({day_name}):')}"
        subscribers = load_subscribers()
        for uid in subscribers:
            try:
                await bot.send_message(uid, text, parse_mode="HTML")
                await asyncio.sleep(0.05)
            except Exception:
                pass

async def main():
    if not BOT_TOKEN:
        print("❌ Ошибка: BOT_TOKEN не задан")
        return
    bot = Bot(token=BOT_TOKEN)

    # Запуск планировщика
    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
    scheduler.add_job(check_upcoming_groups, 'interval', minutes=15, args=[bot])
    scheduler.add_job(morning_mailing, 'cron', hour=7, minute=0, args=[bot])
    scheduler.start()

    print("✅ Бот запущен")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())