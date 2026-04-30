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

# (здесь ONLINE_SCHEDULE без изменений – пропущена для экономии места, но в полном коде присутствует полностью)
ONLINE_SCHEDULE = { ... }  # ваш исходный словарь

SLOGANS_AND_AFFIRMATIONS = [ ... ]  # без изменений
SPB_SUBURBS = [ ... ]               # без изменений
POPULAR_CITIES = [ ... ]           # без изменений


# Вспомогательные функции (moscow_now, time_to_minutes, load/save_subscribers, escape_html, split_long_message, format_* и т.д.) – без изменений, только добавляются новые ниже.

# ------- НОВЫЕ ФУНКЦИИ ДЛЯ РАЗДЕЛЬНЫХ НАСТРОЕК -------
def normalize_user_sub(data: Optional[dict]) -> dict:
    base = {
        "city": None,
        "all_online": False,
        "all_live": False,
        "groups": {},
        # старые общие поля (оставлены для обратной совместимости)
        "daily_hour": 7,
        "remind_before": [60],
        "meta": {"last_daily_sent": None, "last_reminders": {}},
        # новые раздельные настройки
        "online_settings": {"daily_hour": 7, "remind_before": [60]},
        "live_settings": {"daily_hour": 7, "remind_before": [60]},
    }
    if not isinstance(data, dict):
        return base
    # копируем старые корневые ключи
    for key in ("city", "all_online", "all_live", "groups", "meta", "daily_hour", "remind_before"):
        if key in data:
            base[key] = data[key]
    # миграция старых значений в раздельные, если их нет
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
    # нормализация meta
    if not isinstance(base["meta"], dict):
        base["meta"] = {}
    if "last_daily_sent" not in base["meta"]:
        base["meta"]["last_daily_sent"] = None
    if "last_reminders" not in base["meta"]:
        base["meta"]["last_reminders"] = {}
    # нормализация remind_before в самих настройках
    for settings in (base["online_settings"], base["live_settings"]):
        if isinstance(settings.get("remind_before"), int):
            settings["remind_before"] = [settings["remind_before"]]
    return base

def get_online_settings(user_data: dict) -> dict:
    """Возвращает действующие настройки для онлайн-групп."""
    return user_data.get("online_settings", {
        "daily_hour": user_data.get("daily_hour", 7),
        "remind_before": user_data.get("remind_before", [60]),
    })

def get_live_settings(user_data: dict) -> dict:
    """Возвращает действующие настройки для живых групп."""
    return user_data.get("live_settings", {
        "daily_hour": user_data.get("daily_hour", 7),
        "remind_before": user_data.get("remind_before", [60]),
    })

# -----------------------------------------------


# Далее идут функции get_user_sub, set_user_sub, remove_subscriber, escape_html, split_long_message, format_* и т.д. без изменений.

# В функции build_daily_message теперь нужно использовать соответствующие настройки для подписанных групп.
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

# Функции напоминаний (build_reminder_key, build_*_reminder) без изменений, они уже принимают minutes_before.

def collect_due_reminders(user_data: dict, now_dt: datetime):
    due = []
    today_str = now_dt.strftime("%Y-%m-%d")
    now_minutes = now_dt.hour * 60 + now_dt.minute
    reminders_meta = user_data.setdefault("meta", {}).setdefault("last_reminders", {})

    # Онлайн
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

    # Живые
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


async def send_daily_notifications(bot: Bot, now_dt: datetime):
    subs = load_subscribers()
    today_str = now_dt.strftime("%Y-%m-%d")
    changed = False
    for uid, raw_data in subs.items():
        data = normalize_user_sub(raw_data)
        if not data.get("groups") and not data.get("all_online") and not data.get("all_live"):
            subs[uid] = data
            continue
        # проверяем, нужно ли отправлять для онлайн и живых отдельно
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


# Остальные функции (send_hourly_reminders, notifications_worker, клавиатуры, обработчики) ниже.
# Приведу только те, что изменились.

# Клавиатуры
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

def get_days_keyboard(prefix: str, back_callback: Optional[str] = None, back_text: str = "← Назад") -> InlineKeyboardMarkup:
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
    return builder.as_markup()


dp = Dispatcher(storage=MemoryStorage())

@dp.callback_query(F.data == "main_menu")
async def main_menu_callback(callback: CallbackQuery):
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.message.answer(
        "🏠 Главное меню\n\nВыберите раздел:",
        reply_markup=main_menu_inline_keyboard()
    )
    await safe_callback_answer(callback)

@dp.callback_query(F.data == "main_online")
async def main_online(callback: CallbackQuery):
    await btn_online(callback.message)
    await safe_callback_answer(callback)

@dp.callback_query(F.data == "main_live")
async def main_live(callback: CallbackQuery):
    await btn_live(callback.message)
    await safe_callback_answer(callback)

@dp.callback_query(F.data == "main_slogan")
async def main_slogan(callback: CallbackQuery):
    await btn_slogan(callback.message)
    await safe_callback_answer(callback)

@dp.callback_query(F.data == "main_sub")
async def main_sub(callback: CallbackQuery):
    await show_sub_main(callback)

@dp.callback_query(F.data == "main_my_groups")
async def main_my_groups(callback: CallbackQuery):
    await btn_my_groups(callback.message)
    await safe_callback_answer(callback)

@dp.callback_query(F.data == "main_unsubscribe")
async def main_unsubscribe(callback: CallbackQuery):
    await btn_unsubscribe_all(callback.message)
    await safe_callback_answer(callback)


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
    uid = str(target.from_user.id)
    data = get_user_sub(uid)
    builder = InlineKeyboardBuilder()
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


# --- Новые колбэки для раздельных настроек ---
@dp.callback_query(F.data == "sub_settings_online")
async def sub_settings_online(callback: CallbackQuery):
    await settings_menu(callback, "online")

@dp.callback_query(F.data == "sub_settings_live")
async def sub_settings_live(callback: CallbackQuery):
    await settings_menu(callback, "live")

# Обновлённый settings_menu с параметром типа
async def settings_menu(callback_or_msg, group_type: str):
    if isinstance(callback_or_msg, CallbackQuery):
        target = callback_or_msg.message
        uid = str(callback_or_msg.from_user.id)
        answer_func = safe_callback_answer
        use_edit = True
    else:
        target = callback_or_msg
        uid = str(callback_or_msg.from_user.id)
        answer_func = lambda: None
        use_edit = False

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
    # часы
    for h in [5, 6, 7, 8, 9]:
        builder.button(
            text=f'{"✅ " if daily_hour == h else ""}{h:02d}:00',
            callback_data=f"set_daily_hour_{prefix}_{h}"
        )
    builder.adjust(5)
    # напоминания
    remind_buttons = []
    remind_buttons.append(("✅ За 1 час" if remind_set == {60} else "За 1 час", f"set_remind_{prefix}_1"))
    remind_buttons.append(("✅ За 2 часа" if remind_set == {120} else "За 2 часа", f"set_remind_{prefix}_2"))
    remind_buttons.append(("✅ Оба" if remind_set == {60, 120} else "Оба", f"set_remind_{prefix}_both"))
    for label, cb in remind_buttons:
        builder.button(text=label, callback_data=cb)
    builder.adjust(3)
    builder.row(InlineKeyboardButton(text="← К подписке", callback_data="sub_main_back"))
    builder.row(InlineKeyboardButton(text="⬅️ Главное меню", callback_data="main_menu"))

    if use_edit:
        await safe_edit_text(target, text, parse_mode="HTML", reply_markup=builder.as_markup())
        await answer_func(callback_or_msg)
    else:
        await target.answer(text, parse_mode="HTML", reply_markup=builder.as_markup())

# Обработчики изменений для раздельных настроек
@dp.callback_query(F.data.startswith("set_daily_hour_"))
async def set_daily_hour(callback: CallbackQuery):
    parts = callback.data.split("_")
    group_type = parts[3]  # "online" или "live"
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
    group_type = parts[2]  # "online" или "live"
    option = parts[3]      # "1", "2", "both"
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

# Убираем старый колбэк sub_settings (или перенаправляем на выбор) – в полноценном коде его можно удалить.
# В данном примере он не используется.

# Остальные обработчики (cmd_start, help, slogan, btn_online, btn_live, btn_slogan, sub_main, sub_online, sub_live, ...) остаются без изменений, кроме тех, что уже показаны.
# Важно: в sub_main больше нет вызова sub_settings, вместо него новые кнопки.

# Запуск бота
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
