# bot_final_full.py

import asyncio
import json
import os
import random
from datetime import datetime, timedelta
from typing import Dict, List

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.storage.memory import MemoryStorage

BOT_TOKEN = os.getenv("BOT_TOKEN")
SUBSCRIBERS_FILE = "subs.json"

# ---------------- TIME ----------------
def moscow_now():
    return datetime.utcnow() + timedelta(hours=3)

# ---------------- STORAGE ----------------
def load_subscribers():
    try:
        with open(SUBSCRIBERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_subscribers(data):
    with open(SUBSCRIBERS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def normalize_user(data):
    base = {
        "favorites": [],
        "groups": {},
        "city": None
    }
    if isinstance(data, dict):
        base.update(data)
    return base

def get_user(uid):
    subs = load_subscribers()
    return normalize_user(subs.get(uid))

def set_user(uid, data):
    subs = load_subscribers()
    subs[uid] = data
    save_subscribers(subs)

# ---------------- MOCK DATA ----------------
ONLINE_SCHEDULE = {
    0: [("19:00", "Доверие", "https://t.me/test")],
    1: [("20:00", "Рассвет", "https://t.me/test")]
}

# ---------------- FAVORITES ----------------
def is_favorite(user_data, name):
    return name in user_data.get("favorites", [])

# ---------------- URGENT ----------------
def get_urgent_groups(user_data, now, window=30):
    results = []
    for t, name, url in ONLINE_SCHEDULE.get(now.weekday(), []):
        try:
            dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {t}", "%Y-%m-%d %H:%M")
        except:
            continue

        delta = int((dt - now).total_seconds() / 60)

        if -5 <= delta <= window:
            results.append({
                "type": "online",
                "name": name,
                "url": url,
                "start": t,
                "delta": delta
            })

    results.sort(key=lambda x: x["delta"])
    return results

def format_urgent(groups):
    if not groups:
        return "Сейчас нет групп."

    lines = ["🆘 <b>Ближайшие группы:</b>\n"]

    for g in groups[:3]:
        if g["delta"] <= 0:
            time_label = "идёт сейчас"
        else:
            time_label = f"через {g['delta']} мин"

        lines.append(
            f"🌐 <b>{g['start']}</b> — {g['name']}\n"
            f"{time_label}\n"
            f"{g['url']}\n"
        )

    return "\n".join(lines)

def collect_auto(user_data, now):
    groups = get_urgent_groups(user_data, now, 10)
    groups = [g for g in groups if is_favorite(user_data, g["name"])]

    if not groups:
        return None

    return format_urgent(groups)

# ---------------- MENU ----------------
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

    # ДОБАВЛЕНО
    builder.row(
        InlineKeyboardButton(text="🆘 Мне сейчас нужна группа", callback_data="main_urgent")
    )

    return builder.as_markup()

# ---------------- BOT ----------------
dp = Dispatcher(storage=MemoryStorage())

@dp.message(F.text == "/start")
async def start(msg: Message):
    await msg.answer("Главное меню", reply_markup=main_menu_inline_keyboard())

# SOS
@dp.callback_query(F.data == "main_urgent")
async def urgent(cb: CallbackQuery):
    user = get_user(str(cb.from_user.id))
    now = moscow_now()

    groups = get_urgent_groups(user, now, 30)
    if not groups:
        groups = get_urgent_groups(user, now, 60)

    text = format_urgent(groups)

    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data="main_urgent"),
        InlineKeyboardButton(text="⬅️ Меню", callback_data="main_menu"),
    )

    await cb.message.edit_text(text, reply_markup=kb.as_markup())

# toggle favorite (упрощённо)
@dp.message(F.text.startswith("fav "))
async def fav(msg: Message):
    name = msg.text.replace("fav ", "")
    user = get_user(str(msg.from_user.id))

    favs = set(user["favorites"])
    if name in favs:
        favs.remove(name)
    else:
        favs.add(name)

    user["favorites"] = list(favs)
    set_user(str(msg.from_user.id), user)

    await msg.answer("Обновлено")

# ---------------- AUTO LOOP ----------------
async def worker(bot):
    while True:
        subs = load_subscribers()
        now = moscow_now()

        for uid, data in subs.items():
            text = collect_auto(data, now)

            if text:
                try:
                    await bot.send_message(uid, text, parse_mode="HTML")
                except:
                    pass

        await asyncio.sleep(60)

# ---------------- RUN ----------------
async def main():
    bot = Bot(token=BOT_TOKEN)
    asyncio.create_task(worker(bot))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
