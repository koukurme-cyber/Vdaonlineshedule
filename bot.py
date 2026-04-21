import asyncio
import os
import random
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

BOT_TOKEN = os.getenv("BOT_TOKEN")

SCHEDULE = {
    0: [  # Понедельник
        ("05:00", "Восход", "https://t.me/+gdi_B_ctmVJkMTAy"),
        ("07:00", "ВДА Утро", "https://t.me/+KBt9VaElvMA4NTcy"),
        ("08:00", "Единство утро", "https://t.me/ACAgroupUnityMoscow"),
        ("08:00", "Говори Доверяй Чувствуй", "https://t.me/govori_vda"),
        ("09:00", "Доверие", "https://t.me/VDADoverie"),
        ("12:00", "День за днём", "https://t.me/+BwAsiX1KsGljZjQy"),
        ("17:00", "Шаг за шагом", "https://t.me/joinchat/4SFNdPrxumNkYzky"),
        ("18:00", "Весна", "https://t.me/vdavesna_2021"),
        ("19:00", "Рассвет", "https://t.me/+OOw9IMnM5x1hNDJi"),
        ("21:00", "ДЫШИ!", "https://t.me/breathelivebe"),
        ("21:00", "ВДА ВЕЧЕР", "https://t.me/vda_vecher"),
        ("21:00", "Свобода", "https://t.me/vda_svoboda"),
    ],
    1: [  # Вторник
        ("05:00", "Восход", "https://t.me/+gdi_B_ctmVJkMTAy"),
        ("07:00", "ВДА Утро", "https://t.me/+KBt9VaElvMA4NTcy"),
        ("09:00", "Доверие", "https://t.me/VDADoverie"),
        ("12:00", "День за днём", "https://t.me/+BwAsiX1KsGljZjQy"),
        ("14:00", "ВДА вокруг света", "https://t.me/+nFn14RqYkyozZmUy"),
        ("18:00", "Весна", "https://t.me/vdavesna_2021"),
        ("19:00", "Артплей", "https://t.me/VDAartPlay"),
        ("19:00", "Рассвет", "https://t.me/+OOw9IMnM5x1hNDJi"),
        ("20:00", "Феникс", "https://t.me/+1GAp8vi4hyNmMzUy"),
        ("20:00", "По шагам Тони А.", "https://t.me/+ajasg4oH0SU3MjFi"),
        ("21:00", "ДЫШИ!", "https://t.me/breathelivebe"),
        ("21:00", "ВДА ВЕЧЕР", "https://t.me/vda_vecher"),
        ("21:00", "Свобода", "https://t.me/vda_svoboda"),
    ],
    2: [  # Среда
        ("05:00", "Восход", "https://t.me/+gdi_B_ctmVJkMTAy"),
        ("07:00", "ВДА Утро", "https://t.me/+KBt9VaElvMA4NTcy"),
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
        ("07:00", "ВДА Утро", "https://t.me/+KBt9VaElvMA4NTcy"),
        ("09:00", "Доверие", "https://t.me/VDADoverie"),
        ("12:00", "День за днём", "https://t.me/+BwAsiX1KsGljZjQy"),
        ("14:00", "Венеция", "https://t.me/joinchat/AocB9y6QC_k2ZjJi"),
        ("18:00", "Весна", "https://t.me/vdavesna_2021"),
        ("19:00", "праВДА", "https://t.me/+ZYfdfXWBRltjZGEy"),
        ("19:00", "Артплей (онлайн)", "https://t.me/VDAartPlay"),
        ("19:00", "Рассвет", "https://t.me/+OOw9IMnM5x1hNDJi"),
        ("21:00", "ДЫШИ!", "https://t.me/breathelivebe"),
        ("21:00", "ВДА ВЕЧЕР", "https://t.me/vda_vecher"),
        ("21:00", "Свобода", "https://t.me/vda_svoboda"),
    ],
    4: [  # Пятница
        ("05:00", "Восход", "https://t.me/+gdi_B_ctmVJkMTAy"),
        ("07:00", "ВДА Утро", "https://t.me/+KBt9VaElvMA4NTcy"),
        ("08:00", "Единство утро", "https://t.me/ACAgroupUnityMoscow"),
        ("09:00", "Доверие", "https://t.me/VDADoverie"),
        ("12:00", "День за днём", "https://t.me/+BwAsiX1KsGljZjQy"),
        ("14:00", "Венеция", "https://t.me/joinchat/AocB9y6QC_k2ZjJi"),
        ("14:00", "ВДА вокруг света", "https://t.me/+nFn14RqYkyozZmUy"),
        ("18:00", "Весна", "https://t.me/vdavesna_2021"),
        ("19:00", "Братский Круг", "https://t.me/+uEG2E5FVndA0YTc6"),
        ("19:00", "Рассвет", "https://t.me/+OOw9IMnM5x1hNDJi"),
        ("20:00", "Феникс", "https://t.me/+1GAp8vi4hyNmMzUy"),
        ("20:00", "Доверие (Любящий Родитель)", "https://t.me/VDADoverie"),
        ("21:00", "ДЫШИ!", "https://t.me/breathelivebe"),
        ("21:00", "ВДА ВЕЧЕР", "https://t.me/vda_vecher"),
        ("21:00", "Свобода", "https://t.me/vda_svoboda"),
    ],
    5: [  # Суббота
        ("05:00", "Восход", "https://t.me/+gdi_B_ctmVJkMTAy"),
        ("07:00", "Маяк ВДА", "https://t.me/+1XGQ4SDkR8M0N2Yy"),
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
        ("21:00", "Свобода", "https://t.me/vda_svoboda"),
    ],
    6: [  # Воскресенье
        ("05:00", "Восход", "https://t.me/+gdi_B_ctmVJkMTAy"),
        ("08:00", "ВДА Утро", "https://t.me/+KBt9VaElvMA4NTcy"),
        ("10:00", "ВДА НСК онлайн", "https://t.me/VDANsk"),
        ("12:00", "День за днём", "https://t.me/+BwAsiX1KsGljZjQy"),
        ("12:30", "Мужская ВДА", "https://t.me/+ewtjezZaCtM5YTdi"),
        ("14:00", "Венеция", "https://t.me/joinchat/AocB9y6QC_k2ZjJi"),
        ("18:00", "Ежедневник ВДА", "https://t.me/VDAOXOTNIRYAD"),
        ("18:00", "Весна", "https://t.me/vdavesna_2021"),
        ("19:00", "Сила и Надежда", "https://chat.whatsapp.com/CUc0VVemIvl7Aoe2cuYCav"),
        ("19:00", "Рассвет", "https://t.me/+OOw9IMnM5x1hNDJi"),
        ("19:30", "Эффект бабочки", "https://t.me/+FcaUkHDOuMpkMTI8"),
        ("20:00", "Огоньки", "https://t.me/ogonki2025"),
        ("21:00", "ВДА ВЕЧЕР", "https://t.me/vda_vecher"),
        ("21:00", "Свобода", "https://t.me/vda_svoboda"),
    ],
}

DAYS = [
    "Понедельник",
    "Вторник",
    "Среда",
    "Четверг",
    "Пятница",
    "Суббота",
    "Воскресенье",
]

SLOGANS = [
    "Программа ВДА простая, но не лёгкая",
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
    "Молись и молись усердно",
    "Ничего не предпринимай. Подожди",
    "Будь спокоен и осознан",
    "Без чувств нет исцеления",
]

MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="🟠 Сегодня"),
            KeyboardButton(text="🟠 Полное расписание"),
        ],
        [
            KeyboardButton(text="📅 Выбрать день"),
            KeyboardButton(text="🟡 Случайный девиз ВДА"),
        ],
    ],
    resize_keyboard=True,
)

DAYS_KB = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="Понедельник"),
            KeyboardButton(text="Вторник"),
        ],
        [
            KeyboardButton(text="Среда"),
            KeyboardButton(text="Четверг"),
        ],
        [
            KeyboardButton(text="Пятница"),
            KeyboardButton(text="Суббота"),
        ],
        [
            KeyboardButton(text="Воскресенье"),
        ],
        [
            KeyboardButton(text="⬅️ Назад"),
        ],
    ],
    resize_keyboard=True,
)


def get_inline_keyboard(groups):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"{time} {name}", url=url)]
            for time, name, url in groups
        ]
    )


def get_groups_by_day(day_index: int):
    if day_index < 0 or day_index > 6:
        return None, []
    return DAYS[day_index], sorted(SCHEDULE.get(day_index, []), key=lambda x: x[0])


def get_today_groups():
    day = datetime.now().weekday()
    return get_groups_by_day(day)


def get_full_schedule():
    lines = []
    for i, day_name in enumerate(DAYS):
        groups = sorted(SCHEDULE.get(i, []), key=lambda x: x[0])
        if groups:
            lines.append(f"{day_name}:")
            for time, name, url in groups:
                lines.append(f"{time} — {name} — {url}")
            lines.append("")
    return "\n".join(lines).strip()


async def send_groups(message: Message, groups, day_name: str):
    if not groups:
        await message.answer(f"На {day_name.lower()} групп нет.")
        return
    lines = [f"{day_name}:"]
    for time, name, url in groups:
        lines.append(f"{time} — {name}")
    await message.answer("\n".join(lines))
    await message.answer("Нажми на группу:", reply_markup=get_inline_keyboard(groups))


dp = Dispatcher()


@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "Привет! Я бот с расписанием онлайн-групп ВДА.\n"
        "Выбирай команду на клавиатуре ниже.\n\n"
        "Обратите внимание, некоторые ссылки могут быть неактуальны. "
        "О расхождениях сообщайте, пожалуйста, админу.",
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
        await message.answer(text[i : i + 3800])


@dp.message(Command("slogan"))
async def cmd_slogan(message: Message):
    slogan = random.choice(SLOGANS)
    await message.answer(f"Девиз ВДА на сейчас:\n\n«{slogan}»")


@dp.message(F.text == "🟠 Сегодня")
async def btn_today(message: Message):
    await cmd_today(message)


@dp.message(F.text == "🟠 Полное расписание")
async def btn_week(message: Message):
    await cmd_week(message)


@dp.message(F.text == "🟡 Случайный девиз ВДА")
async def btn_slogan(message: Message):
    await cmd_slogan(message)


@dp.message(F.text == "📅 Выбрать день")
async def btn_choose_day(message: Message):
    await message.answer(
        "Выбери день недели:",
        reply_markup=DAYS_KB,
    )


@dp.message(F.text == "Понедельник")
async def btn_monday(message: Message):
    day_name, groups = get_groups_by_day(0)
    await send_groups(message, groups, day_name)


@dp.message(F.text == "Вторник")
async def btn_tuesday(message: Message):
    day_name, groups = get_groups_by_day(1)
    await send_groups(message, groups, day_name)


@dp.message(F.text == "Среда")
async def btn_wednesday(message: Message):
    day_name, groups = get_groups_by_day(2)
    await send_groups(message, groups, day_name)


@dp.message(F.text == "Четверг")
async def btn_thursday(message: Message):
    day_name, groups = get_groups_by_day(3)
    await send_groups(message, groups, day_name)


@dp.message(F.text == "Пятница")
async def btn_friday(message: Message):
    day_name, groups = get_groups_by_day(4)
    await send_groups(message, groups, day_name)


@dp.message(F.text == "Суббота")
async def btn_saturday(message: Message):
    day_name, groups = get_groups_by_day(5)
    await send_groups(message, groups, day_name)


@dp.message(F.text == "Воскресенье")
async def btn_sunday(message: Message):
    day_name, groups = get_groups_by_day(6)
    await send_groups(message, groups, day_name)


@dp.message(F.text == "⬅️ Назад")
async def btn_back(message: Message):
    await message.answer(
        "Главное меню:",
        reply_markup=MAIN_KB,
    )


@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "Справка:\n"
        "/start — открыть главное меню\n"
        "/today — группы на сегодня\n"
        "/week — полное расписание\n"
        "/slogan — случайный девиз ВДА\n\n"
        "Или пользуйся кнопками меню ниже.",
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
