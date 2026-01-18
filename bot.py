import asyncio
import sqlite3
import logging
import json
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    InputMediaPhoto, InputMediaVideo, InputMediaDocument,
    BotCommand, BotCommandScopeDefault, BotCommandScopeChat,
)

# ===================== CONFIG =====================
BOT_TOKEN = "0E"

# Каналы назначения (публикуем ВСЕГДА во все)
CHAT_BY_ID = 0  # 🇧🇾 твой
CHAT_DE_ID = 0 # 🇩🇪 немецкий
CHAT_RU_ID = 0                # 🇷🇺 русский (0 = нет)

# Один админ
ADMIN_ID = 0


# SQLite файл (права доступа сохраняются после рестарта)
DB_PATH = "bot.db"
# ================================================

dp = Dispatcher()

# ===================== LOGGING =====================
logger = logging.getLogger("bot")
logger.setLevel(logging.INFO)

_fmt = logging.Formatter(
    fmt="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_sh = logging.StreamHandler()
_sh.setFormatter(_fmt)
logger.addHandler(_sh)

_fh = RotatingFileHandler("bot.log", maxBytes=2_000_000, backupCount=5, encoding="utf-8")
_fh.setFormatter(_fmt)
logger.addHandler(_fh)


def user_repr(u) -> str:
    if not u:
        return "unknown_user"
    un = f"@{u.username}" if u.username else "(no_username)"
    fn = f"{u.first_name or ''} {u.last_name or ''}".strip()
    return f"{un} id={u.id} name='{fn}'"


def log_event(event: str, *, user=None, chat_id=None, message_id=None, **payload):
    data = {
        "event": event,
        "user": user_repr(user),
        "chat_id": chat_id,
        "message_id": message_id,
        **payload,
    }
    logger.info(json.dumps(data, ensure_ascii=False))


async def safe_answer(cb: CallbackQuery, text: str = "", alert: bool = False):
    """Не падаем на 'query is too old' и похожих ошибках."""
    try:
        await cb.answer(text, show_alert=alert)
    except Exception:
        pass


# Черновики в памяти
DRAFTS: Dict[int, "Draft"] = {}
MEDIA_GROUPS: Dict[Tuple[int, str], List[Message]] = {}

# Админский flow: /allow или /deny без аргумента -> ждём username следующим сообщением
ADMIN_PENDING: Dict[int, str] = {}  # admin_id -> "allow" | "deny"


# ---------- SQLite (persist allowed usernames) ----------
def db_init():
    with sqlite3.connect(DB_PATH) as con:
        con.execute("CREATE TABLE IF NOT EXISTS allowed (username TEXT PRIMARY KEY)")
        con.commit()


def db_allow(username: str):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("INSERT OR IGNORE INTO allowed(username) VALUES(?)", (username,))
        con.commit()


def db_deny(username: str):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("DELETE FROM allowed WHERE username=?", (username,))
        con.commit()


def db_list_allowed() -> List[str]:
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute("SELECT username FROM allowed ORDER BY username")
        return [r[0] for r in cur.fetchall()]


def db_is_allowed(username: str) -> bool:
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute("SELECT 1 FROM allowed WHERE username=? LIMIT 1", (username,))
        return cur.fetchone() is not None


# ---------- Access helpers ----------
def is_admin_id(user_id: int) -> bool:
    return user_id == ADMIN_ID and ADMIN_ID != 0


def username_key(m: Message) -> Optional[str]:
    u = m.from_user.username if m.from_user else None
    return u.lower() if u else None


def has_access_user_id(m: Message) -> bool:
    if not m.from_user:
        return False
    if is_admin_id(m.from_user.id):
        return True
    u = username_key(m)
    return bool(u and db_is_allowed(u))


def has_access_cb(cb: CallbackQuery) -> bool:
    if not cb.from_user:
        return False
    if is_admin_id(cb.from_user.id):
        return True
    u = (cb.from_user.username or "").lower()
    return bool(u and db_is_allowed(u))


async def deny_access_reply(m: Message):
    log_event(
        "deny_access",
        user=m.from_user,
        chat_id=m.chat.id if m.chat else None,
        message_id=m.message_id,
        text=(m.text or "")[:200],
    )
    await m.answer(
        "⛔️ У вас нет доступа к созданию объявлений.\n"
        "Попросите администратора выдать доступ командой:\n"
        "/allow @username\n\n"
        "Важно: у вас должен быть установлен username в Telegram."
    )


# ---------- Draft / Wizard ----------
@dataclass
class Draft:
    mode: str = ""  # "wizard" | "ready"
    step: int = 0
    data: Dict[str, str] = field(default_factory=dict)
    ready_text: str = ""
    extra_text: str = ""
    media: List[dict] = field(default_factory=list)  # [{"type": "...", "file_id": "..."}]
    finalized: bool = False
    awaiting_edit_field: Optional[str] = None
    awaiting_ready_text: bool = False


FIELDS = [
    ("brand_model", "🚗 Марка и модель", "Kia Sportage"),
    ("price", "💰 Стоимость (Brutto/Netto)", "31295 Brutto 26298 Netto"),
    ("reg_date", "📅 Дата первичной регистрации", "01.2023"),
    ("mileage", "📏 Пробег", "19.972 км."),
    ("engine", "🛠 Объём двигателя", "1598см³"),
    ("fuel", "⛽️ Вид топлива", "Бензин"),
    ("gearbox", "⚙️ Коробка передач", "Автоматическая"),
    ("hybrid", "🔋 Гибрид / Электро", "Да"),
    ("inspection", "🛡 Технический осмотр", "Новый"),
    ("owners", "👥 Колличество владельцев", "2"),
    ("trim", "🧩 Комплектация", "Выше среднего"),
    ("seller", "👤 Продавец", "Оф. дилер."),
    ("callcheck", "📌 Прозвон / инфо от продавца (Да/Нет)", "Да"),
    ("link", "🔗 Ссылка на объявление", "https://m.mobile.de/..."),
    ("extra", "📝 Доп. комментарий (опционально)", "Напишите текст или '-' чтобы пропустить"),
]


def targets() -> List[tuple[str, int]]:
    res = [("🇧🇾", CHAT_BY_ID), ("🇩🇪", CHAT_DE_ID)]
    if CHAT_RU_ID != 0:
        res.append(("🇷🇺", CHAT_RU_ID))
    return res


# ---------- Keyboards ----------
def kbd_new_mode() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧾 У меня уже готовый текст", callback_data="new:ready")],
        [InlineKeyboardButton(text="🧩 Заполнить по шагам", callback_data="new:wizard")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="new:cancel")],
    ])


def kbd_after_preview(d: Draft) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="✅ Опубликовать во все каналы", callback_data="act:publish")],
        [InlineKeyboardButton(text="➕ Добавить ещё фото", callback_data="act:add_more")],
        [InlineKeyboardButton(text="🧹 Очистить медиа", callback_data="act:clear_media")],
    ]
    if d.mode == "ready":
        rows.append([InlineKeyboardButton(text="✏️ Подправить текст", callback_data="act:edit_ready")])
    else:
        rows.append([InlineKeyboardButton(text="✏️ Изменить поле", callback_data="act:edit_menu")])

    rows += [
        [InlineKeyboardButton(text="🔁 Сменить режим", callback_data="act:switch_mode")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="act:cancel")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kbd_edit_fields() -> InlineKeyboardMarkup:
    rows, row = [], []
    for k, title, _ in FIELDS:
        text = "📝 Дополнительно" if k == "extra" else title
        row.append(InlineKeyboardButton(text=text, callback_data=f"edit:{k}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="act:back_preview")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ---------- Render & prompts ----------
def prompt_for(key: str) -> str:
    _, title, example = next(x for x in FIELDS if x[0] == key)
    if key == "extra":
        return (
            f"{title}\n"
            f"— Можно добавить любые нюансы.\n"
            f"— Если не нужно: отправьте -"
        )
    return f"{title}\nПример: {example}"


def render_wizard_post(d: Draft) -> str:
    v = d.data
    extra_block = ""
    extra = (d.extra_text or "").strip()
    if extra and extra != "-":
        extra_block = f"\n\n📝 Дополнительно:\n{extra}"
    return (
        f"🚗 {v.get('brand_model', '')}\n"
        f"💰 Стоимость: {v.get('price', '')}\n"
        f"📅 Дата первичной регистрации: {v.get('reg_date', '')}\n"
        f"📏 Пробег: {v.get('mileage', '')}\n"
        f"🛠 Объём двигателя: {v.get('engine', '')}\n"
        f"⛽️ Вид топлива: {v.get('fuel', '')}\n"
        f"⚙️ Коробка передач: {v.get('gearbox', '')}\n"
        f"🔋 Гибрид / Электро: {v.get('hybrid', '')}\n"
        f"🛡 Технический осмотр: {v.get('inspection', '')}\n"
        f"👥 Колличество владельцев: {v.get('owners', '')}\n"
        f"🧩 Комплектация: {v.get('trim', '')}\n"
        f"👤 Продавец: {v.get('seller', '')}\n"
        f"📌 Прозвон. Получена подробная информация от продавца: {v.get('callcheck', '')}"
        f"{extra_block}\n\n"
        f"💬 Заинтересовал автомобиль?\n"
        f"Напишите в ДИРЕКТ, чтобы получить детальный расчёт итоговой стоимости и полный обзор возможных нюансов и подводных камней при покупке этого автомобиля.\n\n"
        f"ℹ️  Пример автомобиля, доступного к приобретению.\n"
        f"Информация приведена на основе открытых данных объявления.\n\n"
        f"🔗 Ссылка может быть недоступна в отдельных регионах — это связано с локальными ограничениями доступа к сайту.\n\n"
        f"{v.get('link', '')}"
    )


def render_final_text(d: Draft) -> str:
    return d.ready_text.strip() if d.mode == "ready" else render_wizard_post(d)


async def send_preview(bot: Bot, user_id: int, d: Draft) -> None:
    text = render_final_text(d)
    kb = kbd_after_preview(d)

    log_event(
        "send_preview",
        user=None,
        chat_id=user_id,
        message_id=None,
        mode=d.mode,
        finalized=d.finalized,
        media_count=len(d.media),
        text_len=len(text),
    )

    if not d.media:
        await bot.send_message(user_id, "Предпросмотр:\n\n" + text, reply_markup=kb)
        return

    media_group = []
    cap = ("Предпросмотр:\n\n" + text)[:1024]
    for i, item in enumerate(d.media[:10]):
        c = cap if i == 0 else None
        if item["type"] == "photo":
            media_group.append(InputMediaPhoto(media=item["file_id"], caption=c))
        elif item["type"] == "video":
            media_group.append(InputMediaVideo(media=item["file_id"], caption=c))
        elif item["type"] == "document":
            media_group.append(InputMediaDocument(media=item["file_id"], caption=c))

    await bot.send_media_group(chat_id=user_id, media=media_group)
    rest = ("Предпросмотр:\n\n" + text)[1024:]
    if rest.strip():
        await bot.send_message(user_id, rest)
    await bot.send_message(user_id, "Выберите действие:", reply_markup=kb)


# ---------- Bot commands (подсказки по /) ----------
async def setup_commands(bot: Bot):
    # Команды для всех
    await bot.set_my_commands(
        commands=[
            BotCommand(command="start", description="Старт / справка"),
            BotCommand(command="new", description="Создать объявление (нужен доступ)"),
            BotCommand(command="cancel", description="Отменить текущий черновик"),
        ],
        scope=BotCommandScopeDefault()
    )

    # Команды для админа — только если admin_id задан
    if ADMIN_ID and ADMIN_ID > 0:
        await bot.set_my_commands(
            commands=[
                BotCommand(command="start", description="Старт / справка"),
                BotCommand(command="new", description="Создать объявление"),
                BotCommand(command="cancel", description="Отменить черновик"),
                BotCommand(command="allow", description="Выдать доступ: /allow @username"),
                BotCommand(command="deny", description="Забрать доступ: /deny @username"),
                BotCommand(command="list", description="Список пользователей с доступом"),
            ],
            scope=BotCommandScopeChat(chat_id=ADMIN_ID)
        )


# ---------- Commands ----------
@dp.message(Command("start"))
async def start(m: Message):
    db_init()
    log_event("cmd", user=m.from_user, chat_id=m.chat.id, message_id=m.message_id, command="/start")

    if not has_access_user_id(m):
        await deny_access_reply(m)
        return

    await m.answer(
        "✅ Доступ подтверждён.\n\n"
        "Команды:\n"
        "/new — создать объявление\n"
        "/cancel — отменить\n\n"
        "Можно прикреплять фото/альбом на любом этапе."
    )


@dp.message(Command("cancel"))
async def cancel(m: Message):
    log_event("cmd", user=m.from_user, chat_id=m.chat.id, message_id=m.message_id, command="/cancel")

    if not has_access_user_id(m):
        await deny_access_reply(m)
        return
    if m.from_user:
        DRAFTS.pop(m.from_user.id, None)
    await m.answer("Ок, черновик отменён.")


@dp.message(Command("new"))
async def new(m: Message):
    log_event("cmd", user=m.from_user, chat_id=m.chat.id, message_id=m.message_id, command="/new")

    if not has_access_user_id(m):
        await deny_access_reply(m)
        return

    if not m.from_user:
        return
    DRAFTS[m.from_user.id] = Draft()

    await m.answer(
        "Как хотите создать объявление?\n\n"
        "🧾 Готовый текст - вставите и сразу предпросмотр.\n"
        "🧩 По шагам - соберём из полей.\n\n"
        "Фото/альбом можно отправить в любой момент.",
        reply_markup=kbd_new_mode()
    )


# Команда allow
@dp.message(Command("allow"))
async def allow(m: Message):
    if not m.from_user or not is_admin_id(m.from_user.id):
        await m.answer("⛔️ У вас нет доступа к этой команде.")
        return

    parts = (m.text or "").split()
    if len(parts) == 1:
        ADMIN_PENDING[m.from_user.id] = "allow"
        await m.answer("Введите username для доступа. Пример: @username")
        return

    u = parts[1].lstrip("@").lower().strip()
    if not u:
        await m.answer("Укажите username.")
        return
    db_allow(u)
    await m.answer(f"✅ Доступ выдан: @{u}")


# Команда deny
@dp.message(Command("deny"))
async def deny(m: Message):
    if not m.from_user or not is_admin_id(m.from_user.id):
        await m.answer("⛔️ У вас нет доступа к этой команде.")
        return

    parts = (m.text or "").split()
    if len(parts) == 1:
        ADMIN_PENDING[m.from_user.id] = "deny"
        await m.answer("Введите username для забора доступа. Пример: @username")
        return

    u = parts[1].lstrip("@").lower().strip()
    db_deny(u)
    await m.answer(f"❌ Доступ убран: @{u}")


@dp.message(Command("list"))
async def list_allowed(m: Message):
    if not m.from_user or not is_admin_id(m.from_user.id):
        await m.answer("⛔️ У вас нет доступа к этой команде.")
        return
    items = db_list_allowed()
    if not items:
        await m.answer("❌ Список пуст.")
        return
    await m.answer("✅ Список пользователей с доступом:\n" + "\n".join(f"@{u}" for u in items))


# ---------- CALLBACKS ----------

@dp.callback_query(F.data.startswith("new:"))
async def on_new_mode(cb: CallbackQuery):
    log_event(
        "callback",
        user=cb.from_user,
        chat_id=cb.message.chat.id if cb.message and cb.message.chat else None,
        message_id=cb.message.message_id if cb.message else None,
        cb_data=cb.data,
    )

    if not has_access_cb(cb):
        await safe_answer(cb, "⛔️ Нет доступа", alert=True)
        return

    uid = cb.from_user.id
    d = DRAFTS.get(uid)
    if not d:
        await safe_answer(cb, "Нет активного /new.", alert=True)
        return

    mode = cb.data.split(":", 1)[1]

    if mode == "cancel":
        DRAFTS.pop(uid, None)
        log_event("draft_cancel", user=cb.from_user, chat_id=cb.message.chat.id if cb.message else None)
        await cb.message.edit_text("Отменено.")
        await safe_answer(cb, "Ок")
        return

    d.awaiting_edit_field = None
    d.awaiting_ready_text = False
    d.finalized = False

    if mode == "ready":
        d.mode = "ready"
        d.step = 0
        d.ready_text = ""
        d.data.clear()
        d.extra_text = ""
        log_event("draft_mode_set", user=cb.from_user, mode="ready")
        await cb.message.edit_text(
            "Ок. Вставьте ГОТОВЫЙ текст объявления одним сообщением.\n\n"
            "Фото/альбом можно прислать в любой момент.\n"
            "После текста я покажу предпросмотр."
        )
        await safe_answer(cb, "Ок")
        return

    if mode == "wizard":
        d.mode = "wizard"
        d.step = 0
        d.ready_text = ""
        d.data.clear()
        d.extra_text = ""
        first_key = FIELDS[0][0]
        log_event("draft_mode_set", user=cb.from_user, mode="wizard")
        await cb.message.edit_text(
            "Ок. Заполняем по шагам.\n"
            "Фото/альбом можно прислать в любой момент.\n\n"
            + prompt_for(first_key)
        )
        await safe_answer(cb, "Ок")
        return

    await safe_answer(cb, "Неизвестный режим.", alert=True)


@dp.callback_query(F.data.startswith("act:"))
async def on_act(cb: CallbackQuery, bot: Bot):
    if not has_access_cb(cb):
        await safe_answer(cb, "⛔️ Нет доступа", alert=True)
        return

    uid = cb.from_user.id
    d = DRAFTS.get(uid)
    if not d:
        await safe_answer(cb, "Черновик не найден.", alert=True)
        return

    action = cb.data.split(":", 1)[1]

    if action == "add_more":
        await safe_answer(cb, "Ок")
        await bot.send_message(uid, "➕ Просто отправьте ещё фото или целый альбом сюда в чат. Я прикреплю их к объявлению.")
        return

    if action == "clear_media":
        d.media.clear()
        await safe_answer(cb, "Медиа очищено.")
        await bot.send_message(uid, "🧹 Медиа очищено. Можете прикрепить новые фото/альбом.")
        if d.finalized:
            await send_preview(bot, uid, d)
        return

    if action == "edit_ready":
        if d.mode != "ready":
            await safe_answer(cb, "Только для режима 'готовый текст'.", alert=True)
            return
        d.awaiting_ready_text = True
        await safe_answer(cb, "Ок")
        await bot.send_message(uid, "✏️ Пришлите новый текст объявления одним сообщением. Я заменю текущий и покажу предпросмотр.")
        return

    if action == "cancel":
        DRAFTS.pop(uid, None)
        await cb.message.edit_text("Отменено.")
        await safe_answer(cb, "Ок")
        return

    if action == "switch_mode":
        d.mode = ""
        d.finalized = False
        d.step = 0
        d.ready_text = ""
        d.data.clear()
        d.extra_text = ""
        d.awaiting_edit_field = None
        d.awaiting_ready_text = False
        await cb.message.edit_text("Выберите режим создания:", reply_markup=kbd_new_mode())
        await safe_answer(cb, "Ок")
        return

    if action == "edit_menu":
        if d.mode != "wizard":
            await safe_answer(cb, "Поля доступны только в режиме 'по шагам'.", alert=True)
            return
        if not d.finalized:
            await safe_answer(cb, "Сначала сформируйте предпросмотр.", alert=True)
            return
        await cb.message.edit_text("Что изменить?", reply_markup=kbd_edit_fields())
        await safe_answer(cb, "Ок")
        return

    if action == "back_preview":
        if not d.finalized:
            await safe_answer(cb, "Сначала сформируйте предпросмотр.", alert=True)
            return
        await cb.message.edit_text("Ок. Предпросмотр отправляю ещё раз.")
        await send_preview(bot, uid, d)
        await safe_answer(cb, "Ок")
        return

    if action == "publish":
        if not d.finalized:
            await safe_answer(cb, "Сначала сформируйте предпросмотр.", alert=True)
            return

        text = render_final_text(d)
        published_flags = []

        for flag, chat_id in targets():
            if d.media:
                media_group = []
                cap = text[:1024]
                for i, item in enumerate(d.media[:10]):
                    c = cap if i == 0 else None
                    if item["type"] == "photo":
                        media_group.append(InputMediaPhoto(media=item["file_id"], caption=c))
                    elif item["type"] == "video":
                        media_group.append(InputMediaVideo(media=item["file_id"], caption=c))
                    elif item["type"] == "document":
                        media_group.append(InputMediaDocument(media=item["file_id"], caption=c))

                await bot.send_media_group(chat_id=chat_id, media=media_group)
                rest = text[1024:]
                if rest.strip():
                    await bot.send_message(chat_id, rest)
            else:
                await bot.send_message(chat_id, text)

            published_flags.append(flag)

        DRAFTS.pop(uid, None)
        await cb.message.edit_text("Опубликовано.")
        await bot.send_message(uid, "Добавлен пост в каналы: " + " ".join(published_flags))
        await safe_answer(cb, "Готово")
        return

    await safe_answer(cb, "Неизвестное действие.", alert=True)


@dp.callback_query(F.data.startswith("edit:"))
async def on_edit_field(cb: CallbackQuery):
    log_event(
        "callback",
        user=cb.from_user,
        chat_id=cb.message.chat.id if cb.message and cb.message.chat else None,
        message_id=cb.message.message_id if cb.message else None,
        cb_data=cb.data,
    )

    if not has_access_cb(cb):
        await safe_answer(cb, "⛔️ Нет доступа", alert=True)
        return

    uid = cb.from_user.id
    d = DRAFTS.get(uid)
    if not d:
        await safe_answer(cb, "Черновик не найден.", alert=True)
        return

    if d.mode != "wizard":
        await safe_answer(cb, "Редактирование полей работает только в режиме 'по шагам'.", alert=True)
        return

    field_key = cb.data.split(":", 1)[1]
    d.awaiting_edit_field = field_key
    d.finalized = False  # вернёмся к заполнению (одно поле)

    await cb.message.edit_text("Введите новое значение:\n\n" + prompt_for(field_key))
    await safe_answer(cb, "Ок")


# ---------- TEXT INPUT ----------
@dp.message(F.text)
async def on_text(m: Message, bot: Bot):
    if not m.text:
        return

    # команды не трогаем
    if m.text.startswith("/"):
        return

    # ---- ADMIN PENDING (allow/deny без аргумента) ----
    if m.from_user and is_admin_id(m.from_user.id) and m.from_user.id in ADMIN_PENDING:
        action = ADMIN_PENDING.get(m.from_user.id)
        raw = (m.text or "").strip()

        log_event(
            "admin_pending_username",
            user=m.from_user,
            chat_id=m.chat.id,
            message_id=m.message_id,
            action=action,
            raw=raw[:200],
        )

        u = raw.lstrip("@").lower().strip()
        if not u or " " in u:
            await m.answer("Нужен один username. Пример: @username\n/cancel — отмена")
            return

        if action == "allow":
            db_allow(u)
            await m.answer(f"✅ Доступ выдан: @{u}")
        else:
            db_deny(u)
            await m.answer(f"❌ Доступ убран: @{u}")

        ADMIN_PENDING.pop(m.from_user.id, None)
        return
    # -----------------------------------------------

    log_event(
        "text_in",
        user=m.from_user,
        chat_id=m.chat.id,
        message_id=m.message_id,
        allowed=has_access_user_id(m),
        has_draft=bool(m.from_user and m.from_user.id in DRAFTS),
        text=m.text[:200],
    )

    if not has_access_user_id(m):
        return

    if not m.from_user or m.from_user.id not in DRAFTS:
        return

    uid = m.from_user.id
    d = DRAFTS[uid]
    text = (m.text or "").strip()

    if d.awaiting_ready_text:
        d.awaiting_ready_text = False
        d.ready_text = text
        d.finalized = True
        await send_preview(bot, uid, d)
        return

    if d.awaiting_edit_field:
        key = d.awaiting_edit_field
        d.awaiting_edit_field = None
        if key == "extra":
            d.extra_text = text
        else:
            d.data[key] = text
        d.finalized = True
        await send_preview(bot, uid, d)
        return

    if not d.mode:
        await m.answer("Выберите режим кнопками после /new.")
        return

    if d.mode == "ready":
        d.ready_text = text
        d.finalized = True
        await send_preview(bot, uid, d)
        return

    if d.mode == "wizard":
        if d.finalized:
            await m.answer("Предпросмотр уже сформирован. Используйте кнопки.")
            return

        key = FIELDS[d.step][0]
        if key == "extra":
            d.extra_text = text
        else:
            d.data[key] = text

        log_event("wizard_step_value", user=m.from_user, chat_id=m.chat.id, step=d.step, field=key)

        d.step += 1
        if d.step < len(FIELDS):
            await m.answer(prompt_for(FIELDS[d.step][0]))
            return

        d.finalized = True
        log_event("wizard_finalized", user=m.from_user, chat_id=m.chat.id, total_fields=len(FIELDS))
        await send_preview(bot, uid, d)
        return


# ---------- Media handlers ----------
@dp.message(F.media_group_id)
async def handle_album(m: Message, bot: Bot):
    if not has_access_user_id(m):
        return
    if not m.from_user or m.from_user.id not in DRAFTS:
        return

    log_event(
        "media_album_piece",
        user=m.from_user,
        chat_id=m.chat.id,
        message_id=m.message_id,
        media_group_id=m.media_group_id,
        has_photo=bool(m.photo),
        has_video=bool(m.video),
        has_document=bool(m.document),
    )

    uid = m.from_user.id
    key = (uid, m.media_group_id)
    MEDIA_GROUPS.setdefault(key, []).append(m)

    async def finalize():
        await asyncio.sleep(1.0)
        msgs = MEDIA_GROUPS.pop(key, [])
        if not msgs:
            return
        d = DRAFTS.get(uid)
        if not d:
            return
        msgs.sort(key=lambda x: x.message_id)

        added = 0
        for mm in msgs:
            if mm.photo:
                d.media.append({"type": "photo", "file_id": mm.photo[-1].file_id})
                added += 1
            elif mm.video:
                d.media.append({"type": "video", "file_id": mm.video.file_id})
                added += 1
            elif mm.document:
                d.media.append({"type": "document", "file_id": mm.document.file_id})
                added += 1

        d.media = d.media[:10]

        log_event(
            "media_album_finalized",
            user=msgs[0].from_user if msgs else None,
            chat_id=msgs[0].chat.id if msgs else None,
            message_id=msgs[0].message_id if msgs else None,
            media_group_id=m.media_group_id,
            added=added,
            total_media=len(d.media),
        )

        await bot.send_message(uid, "✅ Альбом добавлен.")
        if d.finalized:
            await send_preview(bot, uid, d)

    asyncio.create_task(finalize())


@dp.message(F.photo | F.video | F.document)
async def handle_single_media(m: Message, bot: Bot):
    if not has_access_user_id(m):
        return
    if not m.from_user or m.from_user.id not in DRAFTS:
        return

    uid = m.from_user.id
    d = DRAFTS[uid]

    kind = "unknown"
    if m.photo:
        d.media.append({"type": "photo", "file_id": m.photo[-1].file_id})
        kind = "photo"
    elif m.video:
        d.media.append({"type": "video", "file_id": m.video.file_id})
        kind = "video"
    elif m.document:
        d.media.append({"type": "document", "file_id": m.document.file_id})
        kind = "document"

    d.media = d.media[:10]

    log_event(
        "media_single",
        user=m.from_user,
        chat_id=m.chat.id,
        message_id=m.message_id,
        kind=kind,
        total_media=len(d.media),
        finalized=d.finalized,
    )

    await m.answer("✅ Медиа добавлено.")
    if d.finalized:
        await send_preview(bot, uid, d)


# ---------- Main ----------
async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN пуст")

    db_init()
    bot = Bot(BOT_TOKEN)
    await setup_commands(bot)

    log_event("bot_started", user=None, chat_id=None, message_id=None)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
