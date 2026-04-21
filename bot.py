
Сгенерированный код
from pathlib import Path
code = '''import asyncio
import os
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

BOT_TOKEN = os.getenv("BOT_TOKEN")

SCHEDULE = {
    0: [
        ("15:00", "Выход", "https://example.com/1", "м. Технологический институт"),
        ("18:30", "ЮТА", "https://example.com/2", "адрес уточняйте"),
        ("18:45", "Путь к себе", "https://example.com/3", "Б. Подъяческая, 34"),
        ("19:00", "Подруги", "https://example.com/4", "Щербаков пер., 12"),
        ("19:00", "Феникс", "https://example.com/5", "2-я линия В.О., 3"),
        ("19:00", "Небо на Обводном", "https://example.com/6", "наб. Обводного канала, 116"),
        ("20:15", "Замысел", "https://example.com/7", "ул. Ефимова, 6"),
        ("20:20", "Невская", "https://example.com/8", "пер. Челиева, 10Б"),
    ],
    1: [
        ("16:00", "Чёрная речка", "https://example.com/9", "Сестрорецкая ул., 2"),
        ("17:30", "Лотос", "https://example.com/10", "Трамвайный пр., 12к2"),
        ("18:45", "Вместе", "https://example.com/11", "1-я Красноармейская ул., 11"),
        ("18:45", "Черта", "https://example.com/12", "Пискарёвский пр., 25"),
        ("19:00", "Взрослые девочки", "https://example.com/13", "13-я линия В.О., 2"),
        ("19:00", "Единство Невский проспект", "https://example.com/14", "ул. Садовая, 11"),
        ("19:00", "Небо на Обводном", "https://example.com/15", "наб. Обводного канала, 116"),
        ("19:00", "Петрополис", "https://example.com/16", "ул. Колпинская, 27"),
        ("19:10", "Путь к себе", "https://example.com/17", "Б. Подъяческая, 34"),
        ("20:15", "Замысел", "https://example.com/18", "ул. Ефимова, 6"),
        ("20:20", "Небо на Блюхера", "https://example.com/19", "пр. Маршала Блюхера, 9 корп. 2"),
    ],
    2: [
        ("17:00", "13 линия", "https://example.com/20", "13-я линия В.О., 2"),
        ("18:00", "Солнечная сторона", "https://example.com/21", "ул. Радищева, 30"),
        ("19:00", "Здравствуй, Я!", "https://example.com/22", "наб. Обводного канала, 116"),
        ("19:00", "Прометей", "https://example.com/23", "пр. Славы, 4"),
        ("19:00", "Феникс", "https://example.com/24", "2-я линия В.О., 3"),
        ("19:10", "Путь к себе", "https://example.com/25", "Б. Подъяческая, 34"),
        ("19:15", "Ручей", "https://example.com/26", "ул. Колпинская, 27"),
        ("20:15", "Замысел", "https://example.com/27", "ул. Ефимова, 6"),
        ("20:20", "Небо на В.О.", "https://example.com/28", "наб. Лейтенанта Шмидта, 39"),
        ("20:20", "Тепло (Азария)", "https://example.com/29", "Б. Подъяческая, 34"),
        ("21:30", "ЮТА", "https://example.com/30", "адрес уточняйте"),
    ],
    3: [
        ("18:00", "Свобода смысла", "https://example.com/31", "ул. Ефимова, 6"),
        ("18:45", "Черта", "https://example.com/32", "Пискарёвский пр., 25"),
        ("19:00", "Эфир", "https://example.com/33", "ул. Варшавская, 122"),
        ("19:00", "PRO ЖИЗНЬ", "https://example.com/34", "ул. Казанская, 52"),
        ("19:00", "Небо на Обводном", "https://example.com/35", "наб. Обводного канала, 116"),
        ("20:00", "Тепло (Азария)", "https://example.com/36", "Б. Подъяческая, 34"),
        ("20:15", "Замысел", "https://example.com/37", "ул. Ефимова, 6"),
        ("20:20", "Небо на Блюхера", "https://example.com/38", "пр. Маршала Блюхера, 9 корп. 2"),
    ],
    4: [
        ("18:10", "Ручей", "https://example.com/39", "ул. Колпинская, 27"),
        ("18:30", "ЮТА", "https://example.com/40", "адрес уточняйте"),
        ("19:00", "Феникс", "https://example.com/41", "2-я линия В.О., 3"),
        ("19:00", "Тепло (Азария)", "https://example.com/42", "Б. Подъяческая, 34"),
        ("19:00", "Любящий родитель", "https://example.com/43", "наб. Обводного канала, 116"),
        ("19:00", "Небо на Обводном", "https://example.com/44", "наб. Обводного канала, 116"),
        ("19:00", "Солнечная", "https://example.com/45", "Инженерная ул., 6"),
        ("20:15", "Замысел", "https://example.com/46", "ул. Ефимова, 6"),
        ("20:20", "Путь к себе", "https://example.com/47", "Б. Подъяческая, 34"),
    ],
    5: [
        ("11:00", "Взрослые девочки", "https://example.com/48", "13-я линия В.О., 2"),
        ("13:00", "13 линия", "https://example.com/49", "13-я линия В.О., 2"),
        ("13:00", "Петрополис", "https://example.com/50", "ул. Колпинская, 27"),
        ("14:00", "Свобода смысла", "https://example.com/51", "ул. Ефимова, 6"),
        ("15:00", "Солнечная", "https://example.com/52", "Инженерная ул., 6"),
        ("16:00", "Говори, доверяй, чувствуй", "https://example.com/53", "пр. Энгельса, 132"),
        ("17:00", "Парнас", "https://example.com/54", "6-й Верхний пер., 12Б"),
        ("18:15", "AdA (Abused Anonymus)", "https://example.com/55", "ул. Миргородская, 1Д"),
        ("18:30", "ЮТА", "https://example.com/56", "адрес уточняйте"),
        ("19:00", "Солнечная сторона", "https://example.com/57", "ул. Радищева, 30"),
        ("19:00", "Феникс", "https://example.com/58", "2-я линия В.О., 3"),
        ("20:15", "Замысел", "https://example.com/59", "ул. Ефимова, 6"),
    ],
    6: [
        ("11:00", "Взрослые девочки", "https://example.com/60", "13-я линия В.О., 2"),
        ("12:00", "Мост", "https://example.com/61", "г. Красное Село, ул. Массальского, 3"),
        ("13:00", "Черта", "https://example.com/62", "Пискарёвский пр., 25"),
        ("14:30", "Практика «Любящий родитель»", "https://example.com/63", "в рамках группы «Черта»"),
        ("15:00", "Воскресенье", "https://example.com/64", "ул. Маршала Захарова, 13"),
        ("15:10", "Феникс", "https://example.com/65", "2-я линия В.О., 3"),
        ("16:30", "Все свободны", "https://example.com/66", "ул. Таврическая, 5"),
        ("17:00", "проСВЕТ", "https://example.com/67", "Выборгское ш., 15"),
        ("17:00", "Небо на В.О.", "https://example.com/68", "наб. Лейтенанта Шмидта, 39"),
        ("18:00", "Прометей", "https://example.com/69", "пр. Славы, 4"),
        ("18:30", "Путь к себе", "https://example.com/70", "Б. Подъяческая, 34"),
        ("19:00", "Единство Невский проспект", "https://example.com/71", "ул. Садовая, 11"),
        ("19:30", "PRO ЖИЗНЬ", "https://example.com/72", "ул. Казанская, 52"),
        ("20:15", "Замысел", "https://example.com/73", "ул. Ефимова, 6"),
    ],
}

DAYS = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]

MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="📅 Сегодня"), KeyboardButton(text="📆 Полное расписание")]],
    resize_keyboard=True,
)

def get_inline_keyboard(groups):
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=f"{time} {name}", url=url)] for time, name, url, _ in groups]
    )

def get_today_groups():
    day = datetime.now().weekday()
    return DAYS[day], sorted(SCHEDULE.get(day, []), key=lambda x: x[0])

def get_full_schedule():
    lines = []
    for i, day_name in enumerate(DAYS):
        groups = sorted(SCHEDULE.get(i, []), key=lambda x: x[0])
        if groups:
            lines.append(f"{day_name}:")
            for time, name, url, address in groups:
                lines.append(f"{time} — {name} — {address} — {url}")
            lines.append("")
    return "\n".join(lines).strip()

async def send_groups(message: Message, groups, day_name):
    if not groups:
        await message.answer(f"На {day_name.lower()} групп нет.")
        return
    lines = [f"{day_name}:"]
    for time, name, url, address in groups:
        lines.append(f"{time} — {name} — {address}")
    await message.answer("\n".join(lines))
    await message.answer("Нажми на группу:", reply_markup=get_inline_keyboard(groups))

dp = Dispatcher()

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "Привет! Я бот с расписанием онлайн-групп ВДА.\n"
        "Выбирай команду на клавиатуре ниже.",
        reply_markup=MAIN_KB,
    )

@dp.message(Command("today"))
async def cmd_today(message: Message):
    day_name, groups = get_today_groups()
    await send_groups(message, groups, day_name)

@dp.message(Command("week"))
async def cmd_week(message: Message):
    text = "Расписание ВДА на неделю:\n\n" + get_full_schedule()
    for i in range(0, len(text), 3800):
        await message.answer(text[i:i + 3800])

@dp.message(F.text == "📅 Сегодня")
async def btn_today(message: Message):
    await cmd_today(message)

@dp.message(F.text == "📆 Полное расписание")
async def btn_week(message: Message):
    await cmd_week(message)

@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "Справка:\n"
        "/start — начать\n"
        "/today — группы на сегодня\n"
        "/week — полное расписание\n"
        "/help — помощь",
        reply_markup=MAIN_KB,
    )

async def main():
    if not BOT_TOKEN:
        print("Ошибка: BOT_TOKEN не задан в переменных окружения")
        return
    bot = Bot(token=BOT_TOKEN)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
'''
Path('output/bot.py').write_text(code, encoding='utf-8')
print('done')
