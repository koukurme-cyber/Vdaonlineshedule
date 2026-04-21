DAYS = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]

MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="📅 Сегодня"), KeyboardButton(text="📆 Полное расписание")]],
    resize_keyboard=True,
)

def get_inline_keyboard(groups):
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=f"{time} {name}", url=url)] for time, name, url in groups]
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
            for time, name, _ in groups:
                lines.append(f"{time} — {name}")
            lines.append("")
    return "\n".join(lines).strip()

async def send_groups(message: Message, groups, day_name):
    if not groups:
        await message.answer(f"На {day_name.lower()} групп нет.")
        return
    lines = [f"{day_name}:"]
    for time, name, _ in groups:
        lines.append(f"{time} — {name}")
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
