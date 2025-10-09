import os, sys, asyncio, traceback, logging
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest

import psycopg
from psycopg_pool import AsyncConnectionPool
from psycopg import OperationalError

from zoneinfo import ZoneInfo

# ─── Логи ───────────────────────────────────────────────────────
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(levelname)s:%(name)s:%(message)s",
)

# ─── Базовые переменные ─────────────────────────────────────────
BOT_TOKEN = os.environ["BOT_TOKEN"]
TIMEZONE  = os.environ.get("TIMEZONE", "Europe/Moscow")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

# ─── Пул создаётся позже, внутри main() ────────────────────────
pool: AsyncConnectionPool | None = None

# Диагностика окружения (без пароля)
logging.warning(
    "PG env -> host=%s user=%s db=%s sslmode=%s",
    os.getenv("PGHOST"),
    os.getenv("PGUSER"),
    os.getenv("PGDATABASE"),
    os.getenv("PGSSLMODE"),
)

# ─── Время и форматирование дат ─────────────────────────────────
TZ = ZoneInfo(TIMEZONE)
WDAY_RU = ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]

def fmt_dt(dt):
    d = dt.astimezone(TZ)
    return f"{WDAY_RU[d.weekday()]} {d:%d.%m %H:%M}"

# ─── Безопасные алерты для Telegram ────────────────────────────
ALERT_LIMIT = 190  # запас к лимиту телеги ~200

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
    if len(short) < len(str(text)) and show_alert:
        try:
            await c.message.reply(str(text))
        except Exception:
            pass

# ─── Conninfo из PG* переменных ─────────────────────────────────
def _pg_env_conninfo() -> str:
    """Собираем conninfo строго из PG* переменных Railway."""
    host = os.environ["PGHOST"]
    port = os.environ.get("PGPORT", "5432")
    db   = os.environ.get("PGDATABASE", "postgres")
    user = os.environ["PGUSER"]
    pwd  = os.environ["PGPASSWORD"]
    return (
        f"host={host} port={port} dbname={db} "
        f"user={user} password={pwd} "
        f"sslmode=require gssencmode=disable channel_binding=disable "
        f"target_session_attrs=any connect_timeout=10"
    )

# ─── UI ─────────────────────────────────────────────────────────
def kb(sid: int, can_book: bool, can_cancel: bool):
    row1 = []
    if can_book:
        row1 += [
            InlineKeyboardButton(text="➕ Разово",    callback_data=f"book:{sid}:single"),
            InlineKeyboardButton(text="✅ Абонемент", callback_data=f"book:{sid}:pass"),
        ]
    row2 = [InlineKeyboardButton(text="👥 Участники", callback_data=f"who:{sid}")]
    if can_cancel:
        row2.append(InlineKeyboardButton(text="↩️ Отменить", callback_data=f"cancel:{sid}"))
    rows = []
    if row1: rows.append(row1)
    rows.append(row2)
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ─── SQL helpers (ВАЖНО: prepare=False, чтобы PgBouncer не падал) ────────────
async def q1(conn, sql, *args):
    async with conn.cursor() as cur:
        await cur.execute(sql, args, prepare=False)
        return await cur.fetchone()

async def qn(conn, sql, *args):
    async with conn.cursor() as cur:
        await cur.execute(sql, args, prepare=False)
        return await cur.fetchall()

async def ensure_user(conn, tg_id: int, name: str):
    row = await q1(conn, "SELECT id FROM tennis.users WHERE tg_id=%s", tg_id)
    if row:
        return row[0]
    row = await q1(
        conn,
        "INSERT INTO tennis.users(phone, tg_id, name) VALUES (%s,%s,%s) RETURNING id",
        f"tg:{tg_id}", tg_id, name
    )
    return row[0]

# ─── Команды ────────────────────────────────────────────────────
@dp.message(CommandStart())
async def start(m: Message):
    await m.answer("Привет! Запись на теннис.\n• /week — расписание\n• /me — мои записи\n• /rules — правила")

@dp.message(Command("rules"))
async def rules(m: Message):
    await m.answer("Отмена без списания — не позднее чем за 12 часов до начала.")

@dp.message(Command("week"))
async def week(m: Message):
    # текущая неделя: [Пн 00:00; Пн 00:00 + 7 дней), только занятия по расписанию и только будущее
    async with pool.connection() as conn:
        rows = await qn(conn, """
            WITH w AS (SELECT date_trunc('week', now()) AS ws)
            SELECT s.id,
                   s.starts_at,
                   v.free_left,
                   COALESCE(s.capacity, 8) AS cap
            FROM tennis.sessions s
            JOIN tennis.v_session_load v USING(id)
            WHERE s.starts_at >= (SELECT ws FROM w)
              AND s.starts_at <  (SELECT ws + interval '7 day' FROM w)
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

    # Текст (МСК) без #id
    lines = ["<b>Расписание недели</b>"]
    for _, st, free_left, cap in rows:
        lines.append(f"• {fmt_dt(st)}  ({free_left}/{cap})")

    # Инлайн-кнопки “Открыть слот” (без #id)
    open_buttons = [
        InlineKeyboardButton(
            text=f"{st.astimezone(TZ):%d.%m %H:%M} ({free_left}/{cap})",
            callback_data=f"open:{sid}"
        )
        for sid, st, free_left, cap in rows
    ]
    kb_rows = [open_buttons[i:i+2] for i in range(0, len(open_buttons), 2)]
    markup = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    await m.answer("\n".join(lines), reply_markup=markup)

@dp.callback_query(F.data.startswith("open:"))
async def cb_open(c: CallbackQuery):
    sid = int(c.data.split(":")[1])
    try:
        await open_session(c.from_user.id, None, c, sid)
        await c.answer()  # погасить "крутилку"
    except Exception as e:
        logging.exception("open_session failed: %s", e)
        await c.answer(f"Ошибка открытия слота: {e}", show_alert=True)

@dp.message(Command("me"))
async def me(m: Message):
    async with pool.connection() as conn:
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

@dp.message(F.text.startswith("ses_"))
async def open_by_code(m: Message):
    code = m.text[4:]
    async with pool.connection() as conn:
        row = await q1(conn, "SELECT id FROM tennis.sessions WHERE to_char(starts_at,'YYYY-MM-DD_HH24-MI')=%s", code)
    if not row:
        await m.answer("Слот не найден.")
        return
    await open_session(m.from_user.id, m, None, row[0])

async def open_session(tg_user_id: int, m: Message|None, c: CallbackQuery|None, sid: int):
    try:
        async with pool.connection() as conn:
            uid = await ensure_user(conn, tg_user_id, (m.from_user if m else c.from_user).full_name)
            row = await q1(conn, """
                SELECT s.id, s.starts_at, s.ends_at,
                       v.free_left, COALESCE(s.capacity, 8) AS cap,
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
            can_book   = bool(is_future and free_left > 0 and not is_booked)
            # «Отменить» показываем до начала (БД сама проверит дедлайн)
            can_cancel = bool(is_future and is_booked)
            text = (
                f"<b>Слот</b>\n"
                f"{fmt_dt(st)}–{en.astimezone(TZ):%H:%M}\n"
                f"Свободно: {free_left} из {cap}"
                + ("" if is_future else "\n<b>Запись закрыта: слот уже начался</b>")
            )
            markup = kb(_sid, can_book, can_cancel)

        if m:
            await m.answer(text, reply_markup=markup)
        else:
            try:
                await c.message.edit_text(text, reply_markup=markup)
            except TelegramBadRequest as e:
                # Игнорируем "message is not modified"
                if "message is not modified" not in str(e).lower():
                    await safe_alert(c, f"Ошибка показа слота: {e}", show_alert=True)
    except Exception as e:
        logging.exception("open_session error (sid=%s): %s", sid, e)
        if c:
            await safe_alert(c, f"Ошибка показа слота: {e}", show_alert=True)
        elif m:
            await m.answer(f"Ошибка показа слота: {e}")

@dp.callback_query(F.data.startswith("who:"))
async def cb_who(c: CallbackQuery):
    sid = int(c.data.split(":")[1])
    try:
        async with pool.connection() as conn:
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
        logging.exception("who failed (sid=%s): %s", sid, e)
        await c.answer(f"Ошибка получения списка: {e}", show_alert=True)

@dp.callback_query(F.data.startswith("book:"))
async def cb_book(c: CallbackQuery):
    _, sid, kind = c.data.split(":"); sid = int(sid)
    try:
        async with pool.connection() as conn:
            uid = await ensure_user(conn, c.from_user.id, c.from_user.full_name)
            try:
                async with conn.transaction():
                    msg = (await q1(conn, "SELECT tennis.book_session(%s,%s,%s)", uid, sid, kind))[0]
            except Exception as e:
                logging.exception("book_session error (sid=%s, kind=%s, uid=%s): %s", sid, kind, uid, e)
                msg = f"Ошибка записи: {e}"
        if msg == "OK":
            await c.answer("Записано ✅", show_alert=False)
        else:
            await safe_alert(c, msg, show_alert=True)
        await open_session(c.from_user.id, None, c, sid)
    except Exception as e:
        logging.exception("cb_book outer error: %s", e)
        await safe_alert(c, f"Не удалось записаться: {e}", show_alert=True)

@dp.callback_query(F.data.startswith("cancel:"))
async def cb_cancel(c: CallbackQuery):
    sid = int(c.data.split(":")[1])
    try:
        async with pool.connection() as conn:
            uid = await ensure_user(conn, c.from_user.id, c.from_user.full_name)
            row = await q1(conn,
                "SELECT id FROM tennis.bookings WHERE user_id=%s AND session_id=%s AND status='booked'",
                uid, sid)
            if not row:
                await c.answer("У вас нет активной записи.", show_alert=True)
                return
            bid = row[0]
            try:
                msg = (await q1(conn, "SELECT tennis.cancel_booking(%s)", bid))[0]
            except Exception as e:
                logging.exception("cancel_booking error (bid=%s): %s", bid, e)
                msg = f"Ошибка отмены: {e}"
        if msg == "OK":
            await c.answer("Отменено ✅", show_alert=False)
        else:
            await safe_alert(c, msg, show_alert=True)
        await open_session(c.from_user.id, None, c, sid)
    except Exception as e:
        logging.exception("cb_cancel outer error: %s", e)
        await safe_alert(c, f"Не удалось отменить: {e}", show_alert=True)

# ─── Диагностика подключения к БД ──────────────────────────────
@dp.message(Command("db"))
async def db_check(m: Message):
    try:
        async with pool.connection() as conn:
            info = conn.info
            async with conn.cursor() as cur:
                await cur.execute("select version(), now(), current_database();", prepare=False)
                ver, ts, dbname = await cur.fetchone()
        await m.answer(
            "Connecting as:\n"
            f"user={info.user}\nhost={info.host}\ndb={info.dbname}\n"
            f"DB OK ✅\n<code>{dbname}</code>\n{ver}\nnow: {ts:%Y-%m-%d %H:%M:%S %Z}",
            parse_mode=None
        )
    except OperationalError as e:
        await m.answer(f"DB ERROR ❌: OperationalError: {e}", parse_mode=None)
    except Exception as e:
        msg = f"DB ERROR ❌: {type(e).__name__}: {e}"
        print(msg, flush=True)
        await m.answer(msg, parse_mode=None)

@dp.message(Command("ping"))
async def ping(m: Message):
    await m.answer("pong")

# ─── Точка входа ────────────────────────────────────────────────
async def main():
    global pool
    print(">>> Bot container started", flush=True)

    conninfo = _pg_env_conninfo()
    masked = conninfo.replace(f"password={os.environ.get('PGPASSWORD', '')}", "password=***")
    print(f">>> Using conninfo: {masked}", flush=True)

    # Создаём пул, открываем его уже внутри запущенного event loop
    pool = AsyncConnectionPool(
        conninfo=conninfo,
        min_size=1,
        max_size=5,
        num_workers=2,
        timeout=30,
        max_lifetime=3600,
        max_idle=300,
        open=False,  # без configure: используем prepare=False в execute()
    )
    await pool.open()

    try:
        print(">>> Starting polling...", flush=True)
        await dp.start_polling(bot)
        print(">>> Bot polling started", flush=True)
    except Exception as e:
        print(">>> Unhandled exception in polling:", e, file=sys.stderr, flush=True)
        traceback.print_exc()
    finally:
        print(">>> Polling stopped, closing pool...", flush=True)
        if pool is not None:
            await pool.close()
        # держим воркер живым
        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
