import os, sys, asyncio, traceback, logging
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.client.default import DefaultBotProperties
import psycopg
from psycopg_pool import AsyncConnectionPool
from psycopg import OperationalError

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

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

# ─── Conninfo из PG* переменных ─────────────────────────────────
def _pg_env_conninfo() -> str:
    """Собираем conninfo строго из PG* переменных Railway."""
    host = os.environ["PGHOST"]
    port = os.environ.get("PGPORT", "5432")
    db   = os.environ.get("PGDATABASE", "postgres")
    user = os.environ["PGUSER"]
    pwd  = os.environ["PGPASSWORD"]
    # Флаги для пулера Supabase
    return (
        f"host={host} port={port} dbname={db} "
        f"user={user} password={pwd} "
        f"sslmode=require gssencmode=disable channel_binding=disable "
        f"target_session_attrs=any connect_timeout=10"
    )

# ─── UI ─────────────────────────────────────────────────────────
def kb(sid: int, is_booked: bool, free_left: int):
    btns = []
    if free_left > 0 and not is_booked:
        btns.append(InlineKeyboardButton(text="➕ Разово",    callback_data=f"book:{sid}:single"))
        btns.append(InlineKeyboardButton(text="✅ Абонемент", callback_data=f"book:{sid}:pass"))
    if is_booked:
        btns.append(InlineKeyboardButton(text="↩️ Отменить",  callback_data=f"cancel:{sid}"))
    return InlineKeyboardMarkup(inline_keyboard=[btns]) if btns else None

# ─── SQL helpers ────────────────────────────────────────────────
async def q1(conn, sql, *args):
    async with conn.cursor() as cur:
        await cur.execute(sql, args)
        return await cur.fetchone()

async def qn(conn, sql, *args):
    async with conn.cursor() as cur:
        await cur.execute(sql, args)
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
    # текущая неделя: [Пн 00:00; Пн 00:00 + 7 дней)
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
              -- показываем только реальные занятия: Пн/Чт 20:00, Вс 20:00/21:00 (по МСК)
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

    # Текст (МСК)
    lines = ["<b>Расписание недели</b>"]
    for sid, st, free_left, cap in rows:
        lines.append(f"• #{sid} — {fmt_dt(st)} ({free_left}/{cap})")

    # Инлайн-кнопки “Открыть слот”
    open_buttons = [
        InlineKeyboardButton(
            text=f"#{sid} {st.astimezone(TZ):%d.%m %H:%M} ({free_left}/{cap})",
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
    await open_session(c.from_user.id, None, c, sid)

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
    for bid, st, kind in rows:
        lines.append(f"• booking {bid}: {fmt_dt(st)} ({kind})")
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
    async with pool.connection() as conn:
        uid = await ensure_user(conn, tg_user_id, (m.from_user if m else c.from_user).full_name)
        row = await q1(conn, """
            SELECT s.id, s.starts_at, s.ends_at, v.free_left, COALESCE(s.capacity, 8) AS cap,
                   EXISTS(SELECT 1 FROM tennis.bookings b
                          WHERE b.session_id=s.id AND b.user_id=%s AND b.status='booked') AS is_booked
            FROM tennis.sessions s
            JOIN tennis.v_session_load v USING(id)
            WHERE s.id=%s
        """, uid, sid)
    if not row:
        text, markup = "Слот не найден.", None
    else:
        _sid, st, en, free_left, cap, is_booked = row
        text = (
            f"<b>Слот #{_sid}</b>\n"
            f"{fmt_dt(st)}–{en.astimezone(TZ):%H:%M}\n"
            f"Свободно: {free_left} из {cap}"
        )
        markup = kb(_sid, is_booked, free_left)
    if m:
        await m.answer(text, reply_markup=markup)
    else:
        await c.message.edit_text(text, reply_markup=markup)

@dp.callback_query(F.data.startswith("book:"))
async def cb_book(c: CallbackQuery):
    _, sid, kind = c.data.split(":"); sid = int(sid)
    async with pool.connection() as conn:
        uid = await ensure_user(conn, c.from_user.id, c.from_user.full_name)
        try:
            async with conn.transaction():
                msg = (await q1(conn, "SELECT tennis.book_session(%s,%s,%s)", uid, sid, kind))[0]
        except Exception as e:
            msg = f"Ошибка: {e}"
    await c.answer("Записано ✅" if msg == "OK" else msg, show_alert=(msg!="OK"))
    await open_session(c.from_user.id, None, c, sid)

@dp.callback_query(F.data.startswith("cancel:"))
async def cb_cancel(c: CallbackQuery):
    sid = int(c.data.split(":")[1])
    async with pool.connection() as conn:
        uid = await ensure_user(conn, c.from_user.id, c.from_user.full_name)
        row = await q1(conn,
            "SELECT id FROM tennis.bookings WHERE user_id=%s AND session_id=%s AND status='booked'",
            uid, sid)
        if not row:
            await c.answer("У вас нет активной записи.", show_alert=True)
            return
        bid = row[0]
        msg = (await q1(conn, "SELECT tennis.cancel_booking(%s)", bid))[0]
    await c.answer("Отменено ✅" if msg == "OK" else msg, show_alert=(msg!="OK"))
    await open_session(c.from_user.id, None, c, sid)

# ─── Диагностика подключения к БД ──────────────────────────────
@dp.message(Command("db"))
async def db_check(m: Message):
    try:
        async with pool.connection() as conn:
            info = conn.info
            async with conn.cursor() as cur:
                await cur.execute("select version(), now(), current_database();")
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
        open=False,
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
        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
