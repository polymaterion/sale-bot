"""
Барахолка-бот v2  —  aiogram 3.x + APScheduler + SQLite
Все данные хранятся в /data/bot.db (монтируется как Docker volume).
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import BaseStorage, StorageKey
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaVideo,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import config
import texts

# ══════════════════════════════════════════════════════════════════════════════
#  Логирование
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  SQLite — персистентность постов и FSM
# ══════════════════════════════════════════════════════════════════════════════

DB_PATH = "/data/bot.db"
_db: Optional[aiosqlite.Connection] = None


async def init_db() -> None:
    global _db
    _db = await aiosqlite.connect(DB_PATH)
    _db.row_factory = aiosqlite.Row
    await _db.execute("PRAGMA journal_mode=WAL")
    await _db.execute("PRAGMA synchronous=NORMAL")
    await _db.executescript("""
        CREATE TABLE IF NOT EXISTS posts (
            post_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            content_type TEXT    NOT NULL,
            media        TEXT    NOT NULL DEFAULT '[]',
            body         TEXT    NOT NULL DEFAULT '',
            hashtags     TEXT    NOT NULL DEFAULT '[]',
            user_id      INTEGER,
            user_chat_id INTEGER,
            username     TEXT,
            final_msg_id INTEGER,
            admin_cards  TEXT    NOT NULL DEFAULT '{}',
            status       TEXT    NOT NULL DEFAULT 'pending',
            sched_time   TEXT,
            job_id       TEXT
        );
        CREATE TABLE IF NOT EXISTS fsm_data (
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            destiny TEXT    NOT NULL DEFAULT '',
            state   TEXT,
            data    TEXT    NOT NULL DEFAULT '{}',
            PRIMARY KEY (chat_id, user_id, destiny)
        );
    """)
    await _db.commit()
    log.info("SQLite ready: %s", DB_PATH)


async def close_db() -> None:
    global _db
    if _db:
        await _db.close()
        _db = None


# ── Сериализация пост ↔ row ───────────────────────────────────────────────────

def _pack_post(p: dict) -> tuple:
    sched = p.get("scheduled_time")
    cards = {str(k): v for k, v in (p.get("admin_cards") or {}).items()}
    return (
        p["content_type"],
        json.dumps(p.get("media") or [],               ensure_ascii=False),
        (p.get("text") or "").strip(),
        json.dumps(p.get("selected_hashtags") or [],   ensure_ascii=False),
        p.get("user_id"),
        p.get("user_chat_id"),
        p.get("username"),
        p.get("user_final_msg_id"),
        json.dumps(cards,                               ensure_ascii=False),
        p.get("status", "pending"),
        sched.isoformat() if sched else None,
        p.get("job_id"),
    )


def _unpack_post(row) -> dict:
    tz    = ZoneInfo(config.TIMEZONE)
    sched = None
    if row["sched_time"]:
        sched = datetime.fromisoformat(row["sched_time"])
        if sched.tzinfo is None:
            sched = sched.replace(tzinfo=tz)
    cards_raw = json.loads(row["admin_cards"] or "{}")
    return {
        "post_id":           row["post_id"],
        "content_type":      row["content_type"],
        "media":             json.loads(row["media"]    or "[]"),
        "text":              row["body"],
        "selected_hashtags": json.loads(row["hashtags"] or "[]"),
        "user_id":           row["user_id"],
        "user_chat_id":      row["user_chat_id"],
        "username":          row["username"],
        "user_final_msg_id": row["final_msg_id"],
        "admin_cards":       {int(k): v for k, v in cards_raw.items()},
        "status":            row["status"],
        "scheduled_time":    sched,
        "job_id":            row["job_id"],
    }


# ── CRUD ──────────────────────────────────────────────────────────────────────

async def db_insert_post(p: dict) -> int:
    async with _db.execute(
        """INSERT INTO posts
           (content_type,media,body,hashtags,user_id,user_chat_id,
            username,final_msg_id,admin_cards,status,sched_time,job_id)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        _pack_post(p),
    ) as cur:
        pid = cur.lastrowid
    await _db.commit()
    return pid


async def db_get_post(post_id: int) -> Optional[dict]:
    async with _db.execute(
        "SELECT * FROM posts WHERE post_id=?", (post_id,)
    ) as cur:
        row = await cur.fetchone()
    return _unpack_post(row) if row else None


async def db_set_status(post_id: int, status: str) -> None:
    await _db.execute(
        "UPDATE posts SET status=? WHERE post_id=?", (status, post_id)
    )
    await _db.commit()


async def db_save_cards(post_id: int, cards: dict) -> None:
    s = json.dumps({str(k): v for k, v in cards.items()}, ensure_ascii=False)
    await _db.execute(
        "UPDATE posts SET admin_cards=? WHERE post_id=?", (s, post_id)
    )
    await _db.commit()


async def db_set_scheduled(post_id: int, sched: datetime, job_id: str) -> None:
    await _db.execute(
        """UPDATE posts
           SET status='scheduled', sched_time=?, job_id=?
           WHERE post_id=?""",
        (sched.isoformat(), job_id, post_id),
    )
    await _db.commit()


async def db_reset_pending(post_id: int) -> None:
    await _db.execute(
        """UPDATE posts
           SET status='pending', sched_time=NULL, job_id=NULL
           WHERE post_id=?""",
        (post_id,),
    )
    await _db.commit()


async def db_get_scheduled() -> list[dict]:
    async with _db.execute(
        "SELECT * FROM posts WHERE status='scheduled'"
    ) as cur:
        rows = await cur.fetchall()
    return [_unpack_post(r) for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
#  SQLite FSM Storage
# ══════════════════════════════════════════════════════════════════════════════

class SQLiteFSMStorage(BaseStorage):
    """FSM хранилище поверх глобального _db соединения."""

    async def set_state(self, key: StorageKey, state: Any = None) -> None:
        val = state.state if hasattr(state, "state") else state
        await _db.execute(
            """INSERT INTO fsm_data (chat_id,user_id,destiny,state)
               VALUES (?,?,?,?)
               ON CONFLICT(chat_id,user_id,destiny)
               DO UPDATE SET state=excluded.state""",
            (key.chat_id, key.user_id, key.destiny, val),
        )
        await _db.commit()

    async def get_state(self, key: StorageKey) -> Optional[str]:
        async with _db.execute(
            "SELECT state FROM fsm_data WHERE chat_id=? AND user_id=? AND destiny=?",
            (key.chat_id, key.user_id, key.destiny),
        ) as cur:
            row = await cur.fetchone()
        return row[0] if row else None

    async def set_data(self, key: StorageKey, data: Dict[str, Any]) -> None:
        s = json.dumps(data, ensure_ascii=False, default=str)
        await _db.execute(
            """INSERT INTO fsm_data (chat_id,user_id,destiny,data)
               VALUES (?,?,?,?)
               ON CONFLICT(chat_id,user_id,destiny)
               DO UPDATE SET data=excluded.data""",
            (key.chat_id, key.user_id, key.destiny, s),
        )
        await _db.commit()

    async def get_data(self, key: StorageKey) -> Dict[str, Any]:
        async with _db.execute(
            "SELECT data FROM fsm_data WHERE chat_id=? AND user_id=? AND destiny=?",
            (key.chat_id, key.user_id, key.destiny),
        ) as cur:
            row = await cur.fetchone()
        return json.loads(row[0]) if (row and row[0]) else {}

    async def close(self) -> None:
        pass  # управляется через init_db / close_db


# ══════════════════════════════════════════════════════════════════════════════
#  Основные объекты
# ══════════════════════════════════════════════════════════════════════════════

bot       = Bot(token=config.BOT_TOKEN,
                default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp        = Dispatcher(storage=SQLiteFSMStorage())
router    = Router()
dp.include_router(router)
scheduler = AsyncIOScheduler(timezone=config.TIMEZONE)

# буфер для сборки альбомов
_album_buf: dict[str, dict] = {}


# ══════════════════════════════════════════════════════════════════════════════
#  FSM-состояния
# ══════════════════════════════════════════════════════════════════════════════

class Form(StatesGroup):
    waiting_content   = State()
    choosing_hashtags = State()
    preview           = State()


# ══════════════════════════════════════════════════════════════════════════════
#  Клавиатуры
# ══════════════════════════════════════════════════════════════════════════════

def kb_subscribe() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="Подписаться",    url=config.CHANNEL_URL)
    b.button(text="Я подписан ✅", callback_data="check_sub")
    b.adjust(2)
    return b.as_markup()


def kb_hashtags(selected: list[str]) -> InlineKeyboardMarkup:
    """
    По 2 хэштега в строке.
    Последняя строка: [Без категории]  или  [Без категории] [Готово ✅]
    """
    b    = InlineKeyboardBuilder()
    tags = config.HASHTAGS
    n    = len(tags)

    for t in tags:
        b.button(
            text=f"✅ {t}" if t in selected else t,
            callback_data=f"ht:{t}",
        )

    b.button(text="Без категории", callback_data="ht:none")
    if selected:
        b.button(text="Готово ✅", callback_data="ht:done")

    # Размеры рядов: по 2 для тегов + финальный ряд
    rows = [min(2, n - i) for i in range(0, n, 2)]
    rows.append(2 if selected else 1)
    b.adjust(*rows)
    return b.as_markup()


def kb_preview() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить на модерацию", callback_data="pre:submit")
    b.button(text="✏️ Изменить категорию",     callback_data="pre:tags")
    b.button(text="❌ Отменить",               callback_data="pre:cancel")
    b.adjust(1)
    return b.as_markup()


def kb_new_post() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="📝 Новое объявление", callback_data="new_post")
    return b.as_markup()


def kb_admin_mod(post_id: int) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="✅ Опубликовать", callback_data=f"a:pub:{post_id}")
    b.button(text="🕐 Отложить",    callback_data=f"a:sch:{post_id}")
    b.button(text="❌ Отклонить",   callback_data=f"a:rej:{post_id}")
    b.adjust(1)
    return b.as_markup()


def kb_admin_date(post_id: int) -> InlineKeyboardMarkup:
    b   = InlineKeyboardBuilder()
    tz  = ZoneInfo(config.TIMEZONE)
    now = datetime.now(tz)
    for i, label in enumerate(("Сегодня", "Завтра", "Послезавтра")):
        d = now + timedelta(days=i)
        b.button(
            text=f"{label}  {d.strftime('%d.%m')}",
            callback_data=f"a:dt:{post_id}:{d.strftime('%Y%m%d')}",
        )
    b.button(text="❌ Отмена", callback_data=f"a:sc:{post_id}")
    b.adjust(1)
    return b.as_markup()


def kb_admin_time(post_id: int, date_code: str) -> InlineKeyboardMarkup:
    b     = InlineKeyboardBuilder()
    hours = list(range(7, 24)) + [0]   # 07:00 … 23:00, 00:00  → 18 кнопок
    for h in hours:
        b.button(
            text=f"{h:02d}:00",
            callback_data=f"a:tm:{post_id}:{date_code}:{h:02d}",
        )
    b.button(text="⬅️ Назад", callback_data=f"a:sch:{post_id}")
    n = len(hours)
    b.adjust(*([4] * (n // 4) + ([n % 4] if n % 4 else []) + [1]))
    return b.as_markup()


# ══════════════════════════════════════════════════════════════════════════════
#  Утилиты
# ══════════════════════════════════════════════════════════════════════════════

async def is_subscribed(user_id: int) -> bool:
    """Проверяет, подписан ли пользователь на канал."""
    try:
        m = await bot.get_chat_member(config.CHANNEL_ID, user_id)
        return m.status not in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED)
    except Exception as exc:
        log.warning("is_subscribed(%s): %s", user_id, exc)
        return False


async def safe_delete(chat_id: int, msg_id: Optional[int]) -> None:
    """Удаляет сообщение, игнорируя любые ошибки."""
    if not msg_id:
        return
    try:
        await bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except TelegramBadRequest:
        pass


async def safe_edit(
    chat_id: int,
    msg_id: int,
    text: str,
    kb: Optional[InlineKeyboardMarkup] = None,
) -> None:
    """Редактирует текстовое сообщение, игнорируя ошибки."""
    try:
        await bot.edit_message_text(
            text=text,
            chat_id=chat_id,
            message_id=msg_id,
            reply_markup=kb,
        )
    except TelegramBadRequest:
        pass


def build_caption(post: dict) -> str:
    """Итоговый текст объявления: тело + хэштеги через двойной перенос."""
    body = (post.get("text") or "").strip()
    tags = " ".join(post.get("selected_hashtags") or [])
    if body and tags:
        return f"{body}\n\n{tags}"
    return body or tags


async def send_content(chat_id: int, post: dict) -> list[int]:
    """Публикует контент поста в указанный чат. Возвращает список message_id."""
    cap  = build_caption(post) or None
    ct   = post["content_type"]
    meds = post.get("media") or []

    if ct == "text":
        m = await bot.send_message(chat_id=chat_id, text=cap or "")
        return [m.message_id]

    if ct == "photo":
        m = await bot.send_photo(
            chat_id=chat_id, photo=meds[0]["file_id"], caption=cap
        )
        return [m.message_id]

    if ct == "video":
        m = await bot.send_video(
            chat_id=chat_id, video=meds[0]["file_id"], caption=cap
        )
        return [m.message_id]

    if ct == "animation":
        m = await bot.send_animation(
            chat_id=chat_id, animation=meds[0]["file_id"], caption=cap
        )
        return [m.message_id]

    if ct == "album":
        group: list = []
        for i, it in enumerate(meds):
            c = cap if i == 0 else None
            if it["type"] == "photo":
                group.append(InputMediaPhoto(media=it["file_id"], caption=c))
            elif it["type"] == "video":
                group.append(InputMediaVideo(media=it["file_id"], caption=c))
        sent = await bot.send_media_group(chat_id=chat_id, media=group)
        return [m.message_id for m in sent]

    return []


def _cancel_job(post: dict) -> None:
    """Снимает APScheduler-задачу поста (если была)."""
    jid = post.get("job_id")
    if jid:
        try:
            scheduler.remove_job(jid)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
#  Работа с карточками администраторов
# ══════════════════════════════════════════════════════════════════════════════

async def broadcast_to_admins(post_id: int) -> None:
    """Рассылает контент + карточку модерации всем администраторам."""
    post = await db_get_post(post_id)
    if not post:
        return

    uname  = post.get("username")
    header = (texts.ADMIN_NEW_POST.format(username=uname)
               if uname else texts.ADMIN_NEW_POST_ANON)
    cards: dict[int, dict] = {}

    for aid in config.ADMIN_IDS:
        try:
            cids = await send_content(aid, post)
            card = await bot.send_message(
                chat_id=aid,
                text=header,
                reply_to_message_id=cids[0] if cids else None,
                reply_markup=kb_admin_mod(post_id),
            )
            cards[aid] = {"content_ids": cids, "card_id": card.message_id}
        except Exception as exc:
            log.error("broadcast → admin %s: %s", aid, exc)

    await db_save_cards(post_id, cards)


async def update_admin_cards(
    post_id: int,
    acting_id: int,
    my_text: str,
    their_text: str,
) -> None:
    """Редактирует карточки у всех администраторов после действия одного из них."""
    post = await db_get_post(post_id)
    if not post:
        return
    for aid, card in post["admin_cards"].items():
        txt = my_text if aid == acting_id else their_text
        await safe_edit(aid, card["card_id"], txt)


async def notify_user(post_id: int, *, published: bool) -> None:
    """Обновляет финальное сообщение пользователя результатом модерации."""
    post = await db_get_post(post_id)
    if not post:
        return
    cid = post.get("user_chat_id")
    mid = post.get("user_final_msg_id")
    if cid and mid:
        txt = texts.POST_PUBLISHED if published else texts.POST_REJECTED
        await safe_edit(cid, mid, txt, kb=kb_new_post())


async def publish_to_channel(post_id: int) -> bool:
    """Публикует пост в канал. При ошибке уведомляет всех администраторов."""
    post = await db_get_post(post_id)
    if not post:
        return False
    try:
        await send_content(config.CHANNEL_ID, post)
        return True
    except Exception as exc:
        log.error("publish_to_channel post=%s: %s", post_id, exc)
        for aid in config.ADMIN_IDS:
            try:
                await bot.send_message(
                    chat_id=aid,
                    text=texts.CHANNEL_ERROR.format(error=exc),
                )
            except Exception:
                pass
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  Планировщик
# ══════════════════════════════════════════════════════════════════════════════

async def _run_scheduled(post_id: int) -> None:
    """APScheduler вызывает эту функцию в нужное время."""
    post = await db_get_post(post_id)
    if not post or post["status"] != "scheduled":
        return
    if await publish_to_channel(post_id):
        await db_set_status(post_id, "published")
        await notify_user(post_id, published=True)


async def restore_scheduled() -> None:
    """При старте восстанавливает или немедленно публикует запланированные посты."""
    tz    = ZoneInfo(config.TIMEZONE)
    now   = datetime.now(tz)
    posts = await db_get_scheduled()

    restored = missed = 0
    for p in posts:
        pid   = p["post_id"]
        sched = p.get("scheduled_time")
        if not sched:
            continue
        if sched <= now:
            # Бот был выключен — публикуем немедленно
            asyncio.create_task(_publish_missed(pid))
            missed += 1
        else:
            job = scheduler.add_job(
                _run_scheduled,
                trigger="date",
                run_date=sched,
                kwargs={"post_id": pid},
                id=f"post_{pid}",
                replace_existing=True,
            )
            await db_set_scheduled(pid, sched, job.id)
            restored += 1

    log.info(
        "Scheduled posts: %d restored, %d missed (publishing now)",
        restored, missed,
    )


async def _publish_missed(post_id: int) -> None:
    if await publish_to_channel(post_id):
        await db_set_status(post_id, "published")
        await notify_user(post_id, published=True)


# ══════════════════════════════════════════════════════════════════════════════
#  Разбор входящего контента
# ══════════════════════════════════════════════════════════════════════════════

async def _ingest(
    message: Message,
    state: FSMContext,
    album: Optional[list[Message]] = None,
) -> None:
    """Принимает контент, убирает старое сообщение бота, показывает выбор категории."""
    data = await state.get_data()
    await safe_delete(message.chat.id, data.get("bot_msg_id"))
    for mid in data.get("preview_media_ids") or []:
        await safe_delete(message.chat.id, mid)

    # ── Определяем тип контента ──────────────────────────────────────────────
    if album:
        ct: str          = "album"
        media: list[dict] = []
        body              = ""
        for m in album:
            if m.photo:
                media.append({"type": "photo", "file_id": m.photo[-1].file_id})
            elif m.video:
                media.append({"type": "video", "file_id": m.video.file_id})
            if not body and m.caption:
                body = m.caption

    elif message.text:
        ct = "text"; media = []; body = message.text

    elif message.photo:
        ct    = "photo"
        media = [{"type": "photo", "file_id": message.photo[-1].file_id}]
        body  = message.caption or ""

    elif message.video:
        ct    = "video"
        media = [{"type": "video", "file_id": message.video.file_id}]
        body  = message.caption or ""

    elif message.animation:
        ct    = "animation"
        media = [{"type": "animation", "file_id": message.animation.file_id}]
        body  = message.caption or ""

    else:
        m = await message.answer(texts.UNSUPPORTED)
        await state.update_data(bot_msg_id=m.message_id)
        return

    await state.update_data(
        current_post=dict(
            content_type=ct,
            media=media,
            text=body,
            user_id=message.from_user.id,
            user_chat_id=message.chat.id,
            username=message.from_user.username,
        ),
        selected_hashtags=[],
        preview_media_ids=[],
    )
    await state.set_state(Form.choosing_hashtags)

    msg = await message.answer(
        texts.HASHTAG_PROMPT,
        reply_to_message_id=message.message_id,
        reply_markup=kb_hashtags([]),
    )
    await state.update_data(bot_msg_id=msg.message_id)


# ══════════════════════════════════════════════════════════════════════════════
#  /start и проверка подписки
# ══════════════════════════════════════════════════════════════════════════════

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    m = await message.answer(texts.START, reply_markup=kb_subscribe())
    await state.update_data(bot_msg_id=m.message_id)


@router.callback_query(F.data == "check_sub")
async def cb_check_sub(call: CallbackQuery, state: FSMContext) -> None:
    if await is_subscribed(call.from_user.id):
        await call.message.edit_text(texts.READY)
        await state.update_data(bot_msg_id=call.message.message_id)
        await state.set_state(Form.waiting_content)
    else:
        await call.message.edit_text(
            texts.NOT_SUBSCRIBED, reply_markup=kb_subscribe()
        )
    await call.answer()


@router.callback_query(F.data == "new_post")
async def cb_new_post(call: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    for mid in data.get("preview_media_ids") or []:
        await safe_delete(call.message.chat.id, mid)
    await call.message.edit_text(texts.READY)
    await state.update_data(
        bot_msg_id=call.message.message_id,
        current_post=None,
        preview_media_ids=[],
        selected_hashtags=[],
    )
    await state.set_state(Form.waiting_content)
    await call.answer()


# ══════════════════════════════════════════════════════════════════════════════
#  Приём контента (Form.waiting_content)
# ══════════════════════════════════════════════════════════════════════════════

@router.message(Form.waiting_content, F.media_group_id)
async def msg_album_part(message: Message, state: FSMContext) -> None:
    """Собирает части альбома в буфер и запускает отложенную обработку."""
    mgid = message.media_group_id
    if mgid not in _album_buf:
        _album_buf[mgid] = {"messages": [], "state": state}
        _album_buf[mgid]["task"] = asyncio.create_task(_flush_album(mgid))
    _album_buf[mgid]["messages"].append(message)


async def _flush_album(mgid: str) -> None:
    await asyncio.sleep(0.5)
    info = _album_buf.pop(mgid, None)
    if not info:
        return
    msgs = sorted(info["messages"], key=lambda m: m.message_id)
    await _ingest(msgs[0], info["state"], album=msgs)


@router.message(Form.waiting_content, F.text | F.photo | F.video | F.animation)
async def msg_single(message: Message, state: FSMContext) -> None:
    await _ingest(message, state)


@router.message(Form.waiting_content)
async def msg_unsupported(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    await safe_delete(message.chat.id, data.get("bot_msg_id"))
    m = await message.answer(texts.UNSUPPORTED)
    await state.update_data(bot_msg_id=m.message_id)


# ══════════════════════════════════════════════════════════════════════════════
#  Выбор хэштегов (Form.choosing_hashtags)
# ══════════════════════════════════════════════════════════════════════════════

@router.callback_query(Form.choosing_hashtags, F.data.startswith("ht:"))
async def cb_hashtag(call: CallbackQuery, state: FSMContext) -> None:
    tag  = call.data[3:]
    data = await state.get_data()
    sel: list[str] = list(data.get("selected_hashtags") or [])

    if tag == "none":
        sel = []
        await state.update_data(selected_hashtags=sel)
        await _render_preview(call, state)
        await call.answer()
        return

    if tag == "done":
        await _render_preview(call, state)
        await call.answer()
        return

    # Переключаем тег
    if tag in sel:
        sel.remove(tag)
    elif tag in config.HASHTAGS:
        sel.append(tag)

    await state.update_data(selected_hashtags=sel)
    try:
        await call.message.edit_reply_markup(reply_markup=kb_hashtags(sel))
    except TelegramBadRequest:
        pass
    await call.answer()


async def _render_preview(call: CallbackQuery, state: FSMContext) -> None:
    """Показывает превью объявления после выбора категории."""
    data  = await state.get_data()
    post  = dict(data.get("current_post") or {})
    sel   = data.get("selected_hashtags") or []
    post["selected_hashtags"] = sel
    await state.update_data(current_post=post)
    await state.set_state(Form.preview)

    chat_id   = call.message.chat.id
    old_id    = data.get("bot_msg_id")
    formatted = build_caption(post)
    ct        = post.get("content_type")
    meds      = post.get("media") or []
    cap       = formatted or None

    # Текстовый пост — редактируем то же сообщение
    if ct == "text":
        await safe_edit(
            chat_id, old_id,
            formatted or "\u200b",  # zero-width space если текст пустой
            kb=kb_preview(),
        )
        return

    # Медиа — удаляем старое MSG-2, отправляем медиа + кнопки
    await safe_delete(chat_id, old_id)
    media_ids: list[int] = []

    if ct == "photo":
        m = await bot.send_photo(
            chat_id=chat_id, photo=meds[0]["file_id"], caption=cap
        )
        media_ids = [m.message_id]

    elif ct == "video":
        m = await bot.send_video(
            chat_id=chat_id, video=meds[0]["file_id"], caption=cap
        )
        media_ids = [m.message_id]

    elif ct == "animation":
        m = await bot.send_animation(
            chat_id=chat_id, animation=meds[0]["file_id"], caption=cap
        )
        media_ids = [m.message_id]

    elif ct == "album":
        group: list = []
        for i, it in enumerate(meds):
            c = cap if i == 0 else None
            if it["type"] == "photo":
                group.append(InputMediaPhoto(media=it["file_id"], caption=c))
            elif it["type"] == "video":
                group.append(InputMediaVideo(media=it["file_id"], caption=c))
        sent = await bot.send_media_group(chat_id=chat_id, media=group)
        media_ids = [m.message_id for m in sent]

    # Отдельное сообщение с кнопками (под медиа)
    btn = await bot.send_message(
        chat_id=chat_id,
        text=texts.PREVIEW_PROMPT,
        reply_markup=kb_preview(),
    )
    await state.update_data(
        bot_msg_id=btn.message_id,
        preview_media_ids=media_ids,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  Действия в превью (Form.preview)
# ══════════════════════════════════════════════════════════════════════════════

@router.callback_query(Form.preview, F.data == "pre:submit")
async def cb_submit(call: CallbackQuery, state: FSMContext) -> None:
    """Пользователь подтверждает отправку на модерацию."""
    data    = await state.get_data()
    chat_id = call.message.chat.id

    # Удаляем медиа-превью (если было)
    for mid in data.get("preview_media_ids") or []:
        await safe_delete(chat_id, mid)

    # Редактируем текстовое сообщение → финальный статус
    final_msg_id = data["bot_msg_id"]
    await safe_edit(chat_id, final_msg_id, texts.SENT, kb=kb_new_post())
    await call.answer()

    # Сохраняем пост в БД
    post = dict(data.get("current_post") or {})
    post.update(
        selected_hashtags = data.get("selected_hashtags") or [],
        user_chat_id      = chat_id,
        user_final_msg_id = final_msg_id,
        admin_cards       = {},
        status            = "pending",
        scheduled_time    = None,
        job_id            = None,
    )
    post_id = await db_insert_post(post)

    await state.update_data(current_post=None, preview_media_ids=[])
    await state.set_state(Form.waiting_content)

    # Рассылаем карточки модерации в фоне
    asyncio.create_task(broadcast_to_admins(post_id))


@router.callback_query(Form.preview, F.data == "pre:tags")
async def cb_retag(call: CallbackQuery, state: FSMContext) -> None:
    """Изменить категорию — возврат к выбору хэштегов."""
    data    = await state.get_data()
    chat_id = call.message.chat.id

    for mid in data.get("preview_media_ids") or []:
        await safe_delete(chat_id, mid)

    sel = data.get("selected_hashtags") or []
    await state.update_data(preview_media_ids=[])
    await state.set_state(Form.choosing_hashtags)
    await safe_edit(
        chat_id, data["bot_msg_id"],
        texts.HASHTAG_PROMPT,
        kb=kb_hashtags(sel),
    )
    await call.answer()


@router.callback_query(Form.preview, F.data == "pre:cancel")
async def cb_cancel(call: CallbackQuery, state: FSMContext) -> None:
    """Отмена объявления."""
    data    = await state.get_data()
    chat_id = call.message.chat.id

    for mid in data.get("preview_media_ids") or []:
        await safe_delete(chat_id, mid)

    await safe_edit(chat_id, data["bot_msg_id"], texts.CANCELLED, kb=kb_new_post())
    await state.update_data(current_post=None, preview_media_ids=[])
    await state.set_state(Form.waiting_content)
    await call.answer()


# ══════════════════════════════════════════════════════════════════════════════
#  Обработчики администраторов
# ══════════════════════════════════════════════════════════════════════════════

def _is_admin(call: CallbackQuery) -> bool:
    return call.from_user.id in config.ADMIN_IDS


def _admin_uname(call: CallbackQuery) -> str:
    return call.from_user.username or str(call.from_user.id)


# ── Опубликовать ──────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("a:pub:"))
async def cb_pub(call: CallbackQuery) -> None:
    if not _is_admin(call):
        await call.answer("Нет доступа.", show_alert=True); return

    post_id = int(call.data.split(":")[-1])
    post    = await db_get_post(post_id)
    if not post:
        await call.answer("Пост не найден.", show_alert=True); return

    if post["status"] in ("published", "rejected"):
        await safe_edit(
            call.message.chat.id, call.message.message_id,
            texts.ADMIN_ALREADY_DONE,
        )
        await call.answer(); return

    _cancel_job(post)
    if await publish_to_channel(post_id):
        await db_set_status(post_id, "published")
        un = _admin_uname(call)
        await update_admin_cards(
            post_id, call.from_user.id,
            texts.ADMIN_PUB_ME,
            texts.ADMIN_PUB_OTHER.format(username=un),
        )
        await notify_user(post_id, published=True)
    await call.answer()


# ── Отклонить ─────────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("a:rej:"))
async def cb_rej(call: CallbackQuery) -> None:
    if not _is_admin(call):
        await call.answer("Нет доступа.", show_alert=True); return

    post_id = int(call.data.split(":")[-1])
    post    = await db_get_post(post_id)
    if not post:
        await call.answer("Пост не найден.", show_alert=True); return

    if post["status"] in ("published", "rejected"):
        await safe_edit(
            call.message.chat.id, call.message.message_id,
            texts.ADMIN_ALREADY_DONE,
        )
        await call.answer(); return

    _cancel_job(post)
    await db_set_status(post_id, "rejected")
    un = _admin_uname(call)
    await update_admin_cards(
        post_id, call.from_user.id,
        texts.ADMIN_REJ_ME,
        texts.ADMIN_REJ_OTHER.format(username=un),
    )
    await notify_user(post_id, published=False)
    await call.answer()


# ── Отложить: выбор даты ──────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("a:sch:"))
async def cb_sch(call: CallbackQuery) -> None:
    """Открывает выбор даты. Также используется как «⬅️ Назад» из экрана времени."""
    if not _is_admin(call):
        await call.answer("Нет доступа.", show_alert=True); return
    post_id = int(call.data.split(":")[-1])
    if not await db_get_post(post_id):
        await call.answer("Пост не найден.", show_alert=True); return
    await safe_edit(
        call.message.chat.id, call.message.message_id,
        texts.ADMIN_DATE_PICK,
        kb=kb_admin_date(post_id),
    )
    await call.answer()


# ── Отложить: отмена (возврат к кнопкам модерации) ───────────────────────────

@router.callback_query(F.data.startswith("a:sc:"))
async def cb_sc(call: CallbackQuery) -> None:
    if not _is_admin(call):
        await call.answer("Нет доступа.", show_alert=True); return
    post_id = int(call.data.split(":")[-1])
    post    = await db_get_post(post_id)
    if not post:
        await call.answer("Пост не найден.", show_alert=True); return

    _cancel_job(post)
    if post["status"] == "scheduled":
        await db_reset_pending(post_id)

    uname  = post.get("username")
    header = (texts.ADMIN_NEW_POST.format(username=uname)
               if uname else texts.ADMIN_NEW_POST_ANON)
    await safe_edit(
        call.message.chat.id, call.message.message_id,
        header,
        kb=kb_admin_mod(post_id),
    )
    await call.answer()


# ── Отложить: выбор времени ───────────────────────────────────────────────────

@router.callback_query(F.data.startswith("a:dt:"))
async def cb_dt(call: CallbackQuery) -> None:
    if not _is_admin(call):
        await call.answer("Нет доступа.", show_alert=True); return
    _, _, pid_s, dc = call.data.split(":", 3)
    post_id = int(pid_s)
    tz = ZoneInfo(config.TIMEZONE)
    d  = datetime.strptime(dc, "%Y%m%d").replace(tzinfo=tz)
    await safe_edit(
        call.message.chat.id, call.message.message_id,
        texts.ADMIN_TIME_PICK.format(date=d.strftime("%d.%m.%Y")),
        kb=kb_admin_time(post_id, dc),
    )
    await call.answer()


# ── Отложить: подтверждение времени ──────────────────────────────────────────

@router.callback_query(F.data.startswith("a:tm:"))
async def cb_tm(call: CallbackQuery) -> None:
    if not _is_admin(call):
        await call.answer("Нет доступа.", show_alert=True); return

    parts   = call.data.split(":")
    post_id = int(parts[2])
    dc      = parts[3]
    hh      = int(parts[4])

    tz  = ZoneInfo(config.TIMEZONE)
    run = datetime.strptime(dc, "%Y%m%d").replace(
        hour=hh, minute=0, second=0, microsecond=0, tzinfo=tz,
    )
    if run <= datetime.now(tz):
        await call.answer(texts.ADMIN_TIME_PAST, show_alert=True); return

    post = await db_get_post(post_id)
    if not post:
        await call.answer("Пост не найден.", show_alert=True); return

    _cancel_job(post)
    job = scheduler.add_job(
        _run_scheduled,
        trigger="date",
        run_date=run,
        kwargs={"post_id": post_id},
        id=f"post_{post_id}",
        replace_existing=True,
    )
    await db_set_scheduled(post_id, run, job.id)

    ds, ts, un = run.strftime("%d.%m.%Y"), run.strftime("%H:%M"), _admin_uname(call)
    await update_admin_cards(
        post_id, call.from_user.id,
        texts.ADMIN_SCH_ME.format(date=ds, time=ts),
        texts.ADMIN_SCH_OTHER.format(date=ds, time=ts, username=un),
    )
    await call.answer()


# ══════════════════════════════════════════════════════════════════════════════
#  Точка входа
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    await init_db()
    scheduler.start()
    await restore_scheduled()
    log.info("Bot starting…")
    try:
        await dp.start_polling(bot, skip_updates=True)
    finally:
        scheduler.shutdown()
        await close_db()
        await bot.session.close()
        log.info("Bot stopped")


if __name__ == "__main__":
    asyncio.run(main())
