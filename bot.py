import os, sys, asyncio, traceback, logging
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest

import psycopg
import psycopg.errors
from psycopg_pool import AsyncConnectionPool
from psycopg import OperationalError
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo

# ─── Настройка логов ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

# ─── Конфигурация ───────────────────────────────────────────────
BOT_TOKEN = os.environ["BOT_TOKEN"]
TIMEZONE = os.environ.get("TIMEZONE", "Europe/Moscow")
TZ = ZoneInfo(TIMEZONE)

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

pool: AsyncConnectionPool | None = None

logging.warning(
    "PG env -> host=%s user=%s db=%s sslmode=%s",
    os.getenv("PGHOST"),
    os.getenv("PGUSER"),
    os.getenv("PGDATABASE"),
    os.getenv("PGSSLMODE"),
)

# ─── Вспомогательные функции ─────────────────────────────────────
WDAY_RU = ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]

def fmt_dt(dt):
    d = dt.astimezone(TZ)
    return f"{WDAY_RU[d.weekday()]} {d:%d.%m %H:%M}"

def _pg_env_conninfo() -> str:
    host = os.environ["PGHOST"]
    port = os.environ.get("PGPORT", "5432")
    db = os.environ.get("PGDATABASE", "postgres")
    user = os.environ["PGUSER"]
    pwd = os.environ["PGPASSWORD"]
    return (
        f"host={host} port={port} dbname={db} user={user} password={pwd} "
        f"sslmode=require gssencmode=disable channel_binding=disable "
        f"target_session_attrs=any connect_timeout=10"
    )

# ─── Безопасные алерты ───────────────────────────────────────────
ALERT_LIMIT = 190
def clip_for_alert(text: str, limit: int = ALERT_LIMIT) -> str:
    s = str(text)
    return (s[:limit - 1] + "…") if len(s) > limit else s

async def safe_alert(c: CallbackQuery, text: str, *, show_alert: bool = True):
    short = clip_for_alert(text)
    try:
        await c.answer(short, show_alert=show_alert)
    except Exception:
        try:
            await c.answer("Произошла ошибка", show_alert=True)
        except Exception:
            pass

# ─── Управление соединениями ─────────────────────────────────────
@asynccontextmanager
async def get_conn():
    async with pool.connection() as conn:
        try:
            try:
                conn.prepare_threshold = None
            except Exception:
                pass
            yield conn
        finally:
            pass

# ─── SQL helpers с защитой от DuplicatePreparedStatement ─────────
async def q1(conn, sql, *args):
    async with conn.cursor() as cur:
        try:
            await cur.execute(sql, args, prepare=False)
        except psycopg.errors.DuplicatePreparedStatement:
            await conn.execute("DEALLOCATE ALL;")
            await cur.execute(sql, args, prepare=False)
        return await cur.fetchone()

async def qn(conn, sql, *args):
    async with conn.cursor() as cur:
        try:
            await cur.execute(sql, args, prepare=False)
        except psycopg.errors.DuplicatePreparedStatement:
            await conn.execute("DEALLOCATE ALL;")
            await cur.execute(sql, args, prepare=False)
        return await cur.fetchall()

async def ensure_user(conn, tg_id: int, name: str):
    row = await q1(conn, "SELECT id FROM tennis.users WHERE tg_id=%s", tg_id)
    if row:
        return row[0]
    row = await q1(conn,
        "INSERT INTO tennis.users(phone, tg_id, name) VALUES (%s,%s,%s) RETURNING id",
        f"tg:{tg_id}", tg_id, name
    )
    return row[0]

# ─── UI ───────────────────────────────────────────────────────────
def kb(sid: int, can_book: bool, can_cancel: bool):
    row1 = []
    if can_book:
        row1 += [
            InlineKeyboardButton(text="➕ Разово", callback_data=f"book:{sid}:single"),
            InlineKeyboardButton(text="✅ Абонемент", callback_data=f"book:{sid}:pass"),
        ]
    row2 = [InlineKeyboardButton(text="👥 Участники", callback_data=f"who:{sid}")]
    if can_cancel:
        row2.append(InlineKeyboardButton(text="↩️ Отменить", callback_data=f"cancel:{sid}"))
    rows = []
    if row1: rows.append(row1)
    rows.append(row2)
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ─── Команды ─────────────────────────────────────────────────────
@dp.message(CommandStart())
async def start(m: Message):
    await m.answer("Привет! Запись на теннис.\n• /week — расписание\n• /me — мои записи\n• /rules — правила")

@dp.message(Command("rules"))
async def rules(m: Message):
    await m.answer("Отмена без списания — не позднее чем за 12 часов до начала.")

@dp.message(Command("week"))
async def week(m: Message):
    async with get_conn() as conn:
        rows = await qn(conn, """
            WITH w AS (SELECT date_trunc('week', now()) AS ws)
            SELECT s.id, s.starts_at, v.free_left, COALESCE(s.capacity, 8) AS cap
            FROM tennis.sessions s
            JOIN tennis.v_session_load v USING(id)
            WHERE s.starts_at >= (SELECT ws FROM w)
              AND s.starts_at < (SELECT ws + interval '7 day' FROM w)
              AND s.starts_at > now()
              AND (
                    (
                      extract(dow from (s.starts_at at time zone 'Europe/Moscow')) in (1,4)
                      AND extract(hour from (s.starts_at at time zone 'Europe/Moscow')) = 20
                    )
                 OR (
                      extract(dow from (s.starts_at at time zone 'Europe/Moscow')) = 0
                      AND extract(hour from (s.starts_at at time zone 'Europe/Moscow')) in (20,21)
                    )
                  )
            ORDER BY s.starts_at
        """)
    if not rows:
        await m.answer("На эту неделю слоты ещё не созданы.")
        return

    lines = ["<b>Расписание недели</b>"]
    for _, st, free_left, cap in rows:
        lines.append(f"• {fmt_dt(st)}  ({free_left}/{cap})")

    buttons = [
        InlineKeyboardButton(
            text=f"{st.astimezone(TZ):%d.%m %H:%M} ({free_left}/{cap})",
            callback_data=f"open:{sid}"
        )
        for sid, st, free_left, cap in rows
    ]
    kb_rows = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
    markup = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await m.answer("\n".join(lines), reply_markup=markup)

@dp.callback_query(F.data.startswith("open:"))
async def cb_open(c: CallbackQuery):
    sid = int(c.data.split(":")[1])
    try:
        await open_session(c.from_user.id, None, c, sid)
        await c.answer()
    except Exception as e:
        logging.exception("open_session failed: %s", e)
        await safe_alert(c, f"Ошибка открытия слота: {e}")

@dp.message(Command("me"))
async def me(m: Message):
    async with get_conn() as conn:
        uid = await ensure_user(conn, m.from_user.id, m.from_user.full_name)
        rows = await qn(conn, """
            SELECT b.id, s.starts_at, b.kind
            FROM tennis.bookings b
            JOIN tennis.sessions s ON s.id=b.session_id
            WHERE b.user_id=%s AND b.status='booked' AND s.starts_at>now()
            ORDER BY s.starts_at
        """, uid)
    if not rows:
        await m.answer("У вас нет будущих записей.")
        return
    lines = ["<b>Мои записи</b>"]
    for _, st, kind in rows:
        lines.append(f"• {fmt_dt(st)} — {kind}")
    await m.answer("\n".join(lines))

async def open_session(tg_user_id: int, m: Message|None, c: CallbackQuery|None, sid: int):
    try:
        async with get_conn() as conn:
            uid = await ensure_user(conn, tg_user_id, (m.from_user if m else c.from_user).full_name)
            row = await q1(conn, """
                SELECT s.id, s.starts_at, s.ends_at, v.free_left,
                       COALESCE(s.capacity, 8) AS cap,
                       (s.starts_at > now()) AS is_future,
                       (now() < s.cancel_deadline) AS can_cancel_deadline,
                       EXISTS(SELECT 1 FROM tennis.bookings b
                              WHERE b.session_id=s.id AND b.user_id=%s AND b.status='booked') AS is_booked
                FROM tennis.sessions s
                JOIN tennis.v_session_load v USING(id)
                WHERE s.id=%s
            """, uid, sid)
        if not row:
            text, markup = "Слот не найден.", None
        else:
            _sid, st, en, free_left, cap, is_future, can_cancel_deadline, is_booked = row
            can_book = bool(is_future and free_left > 0 and not is_booked)
            can_cancel = bool(is_future and is_booked)
            text = (
                f"<b>Слот</b>\n{fmt_dt(st)}–{en.astimezone(TZ):%H:%M}\n"
                f"Свободно: {free_left} из {cap}"
                + ("" if is_future else "\n<b>Запись закрыта</b>")
            )
            markup = kb(_sid, can_book, can_cancel)

        if m:
            await m.answer(text, reply_markup=markup)
        else:
            try:
                await c.message.edit_text(text, reply_markup=markup)
            except TelegramBadRequest as e:
                if "message is not modified" not in str(e).lower():
                    await safe_alert(c, f"Ошибка показа: {e}")
    except Exception as e:
        logging.exception("open_session error: %s", e)
        if c:
            await safe_alert(c, f"Ошибка показа слота: {e}")

@dp.callback_query(F.data.startswith("who:"))
async def cb_who(c: CallbackQuery):
    sid = int(c.data.split(":")[1])
    try:
        async with get_conn() as conn:
            rows = await qn(conn, """
                SELECT u.name, b.kind
                FROM tennis.bookings b
                JOIN tennis.users u ON u.id = b.user_id
                WHERE b.session_id=%s AND b.status='booked'
                ORDER BY u.name
            """, sid)
            cap_row = await q1(conn, "SELECT COALESCE(capacity,8) FROM tennis.sessions WHERE id=%s", sid)
            cap = cap_row[0] if cap_row else 8
        if not rows:
            await c.answer("Пока никто не записан.", show_alert=True)
            return
        lines = [f"<b>Участники ({len(rows)}/{cap})</b>"]
        for name, kind in rows:
            lines.append(f"• {name} — {kind}")
        await c.message.reply("\n".join(lines))
        await c.answer()
    except Exception as e:
        await safe_alert(c, f"Ошибка получения списка: {e}")

@dp.callback_query(F.data.startswith("book:"))
async def cb_book(c: CallbackQuery):
    _, sid, kind = c.data.split(":"); sid = int(sid)
    try:
        async with get_conn() as conn:
            uid = await ensure_user(conn, c.from_user.id, c.from_user.full_name)
            try:
                async with conn.transaction():
                    msg = (await q1(conn, "SELECT tennis.book_session(%s,%s,%s)", uid, sid, kind))[0]
            except Exception as e:
                msg = f"Ошибка записи: {e}"
        if msg == "OK":
            await c.answer("Записано ✅", show_alert=False)
        else:
            await safe_alert(c, msg)
        await open_session(c.from_user.id, None, c, sid)
    except Exception as e:
        await safe_alert(c, f"Не удалось записаться: {e}")

@dp.callback_query(F.data.startswith("cancel:"))
async def cb_cancel(c: CallbackQuery):
    sid = int(c.data.split(":")[1])
    try:
        async with get_conn() as conn:
            uid = await ensure_user(conn, c.from_user.id, c.from_user.full_name)
            row = await q1(conn,
                "SELECT id FROM tennis.bookings WHERE user_id=%s AND session_id=%s AND status='booked'",
                uid, sid)
            if not row:
                await c.answer("У вас нет активной записи.", show_alert=True)
                return
            bid = row[0]
            msg = (await q1(conn, "SELECT tennis.cancel_booking(%s)", bid))[0]
        if msg == "OK":
            await c.answer("Отменено ✅", show_alert=False)
        else:
            await safe_alert(c, msg)
        await open_session(c.from_user.id, None, c, sid)
    except Exception as e:
        await safe_alert(c, f"Не удалось отменить: {e}")

# ─── DB Check ───────────────────────────────────────────────────
@dp.message(Command("db"))
async def db_check(m: Message):
    try:
        async with get_conn() as conn:
            info = conn.info
            async with conn.cursor() as cur:
                await cur.execute("select version(), now(), current_database();", prepare=False)
                ver, ts, dbname = await cur.fetchone()
        await m.answer(
            f"DB OK ✅\n<code>{dbname}</code>\n{ver}\nnow: {ts:%Y-%m-%d %H:%M:%S %Z}",
            parse_mode=None
        )
    except Exception as e:
        await m.answer(f"DB ERROR ❌: {e}")

@dp.message(Command("ping"))
async def ping(m: Message):
    await m.answer("pong")

# ─── MAIN ────────────────────────────────────────────────────────
async def main():
    global pool
    print(">>> Bot container started", flush=True)
    conninfo = _pg_env_conninfo()
    masked = conninfo.replace(f"password={os.environ.get('PGPASSWORD','')}", "password=***")
    print(f">>> Using conninfo: {masked}", flush=True)

    pool = AsyncConnectionPool(conninfo=conninfo, min_size=1, max_size=5, open=False)
    await pool.open()

    try:
        print(">>> Starting polling...", flush=True)
        await dp.start_polling(bot)
    except Exception as e:
        print("Unhandled exception:", e, file=sys.stderr, flush=True)
        traceback.print_exc()
    finally:
        if pool:
            await pool.close()
        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
