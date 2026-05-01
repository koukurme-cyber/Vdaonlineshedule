import asyncio
import json
import os
import random
from datetime import datetime, timedelta
from typing import List

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
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

def get_user(uid):
    data = load_subscribers().get(uid, {})
    data.setdefault("favorites", [])
    data.setdefault("city", None)
    return data

def set_user(uid, data):
    all_data = load_subscribers()
    all_data[uid] = data
    save_subscribers(all_data)

# ---------------- MOCK DATA ----------------
ONLINE = {
    0: [("19:00", "Доверие", "https://t.me/test")],
    1: [("19:00", "Рассвет", "https://t.me/test")]
}

# ---------------- FAVORITES ----------------
def is_favorite(user, name):
    return name in user.get("favorites", [])

# ---------------- URGENT ----------------
def get_urgent_groups(user, now, window=30):
    results = []
    for t, name, url in ONLINE.get(now.weekday(), []):
        try:
            dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {t}", "%Y-%m-%d %H:%M")
        except:
            continue
        delta = int((dt - now).total_seconds() / 60)
        if -5 <= delta <= window:
            results.append({
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
    lines = ["🆘 Ближайшие группы:\n"]
    for g in groups[:3]:
        lines.append(f"{g['start']} — {g['name']}\n{g['url']}\n")
    return "\n".join(lines)

def collect_auto(user, now):
    groups = get_urgent_groups(user, now, 10)
    groups = [g for g in groups if is_favorite(user, g["name"])]
    if not groups:
        return None
    return format_urgent(groups)

# ---------------- BOT ----------------
dp = Dispatcher(storage=MemoryStorage())

def main_menu():
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🆘 Мне сейчас нужна группа", callback_data="urgent"))
    return kb.as_markup()

@dp.message(F.text == "/start")
async def start(msg: Message):
    await msg.answer("Меню", reply_markup=main_menu())

@dp.callback_query(F.data == "urgent")
async def urgent(cb: CallbackQuery):
    user = get_user(str(cb.from_user.id))
    now = moscow_now()
    groups = get_urgent_groups(user, now, 30)
    text = format_urgent(groups)
    await cb.message.edit_text(text)

# toggle favorite (пример)
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
    await msg.answer("OK")

# ---------------- AUTO LOOP ----------------
async def worker(bot):
    while True:
        subs = load_subscribers()
        now = moscow_now()
        for uid, data in subs.items():
            text = collect_auto(data, now)
            if text:
                try:
                    await bot.send_message(uid, text)
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
