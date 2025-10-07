import os, asyncio
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.client.default import DefaultBotProperties
import psycopg
import sys, traceback


BOT_TOKEN    = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]
TIMEZONE     = os.environ.get("TIMEZONE", "Europe/Moscow")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()


def kb(sid: int, is_booked: bool, free_left: int):
    btns = []
    if free_left > 0 and not is_booked:
        btns.append(InlineKeyboardButton(text="➕ Разово",     callback_data=f"book:{sid}:single"))
        btns.append(InlineKeyboardButton(text="✅ Абонемент",  callback_data=f"book:{sid}:pass"))
    if is_booked:
        btns.append(InlineKeyboardButton(text="↩️ Отменить",   callback_data=f"cancel:{sid}"))
    return InlineKeyboardMarkup(inline_keyboard=[btns]) if btns else None


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


@dp.message(CommandStart())
async def start(m: Message):
    await m.answer("Привет! Запись на теннис.\n• /week — расписание\n• /me — мои записи\n• /rules — правила")


@dp.message(Command("rules"))
async def rules(m: Message):
    await m.answer("Отмена без списания — не позднее чем за 12 часов до начала.")


@dp.message(Command("week"))
async def week(m: Message):
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        rows = await qn(conn, """
            WITH w AS (SELECT date_trunc('week', now()) AS ws)
            SELECT s.id, s.starts_at, v.free_left
            FROM tennis.sessions s
            JOIN tennis.v_session_load v USING(id)
            WHERE s.starts_at >= (SELECT ws FROM w)
              AND s.starts_at <  (SELECT ws + interval '7 day' FROM w)
            ORDER BY s.starts_at
        """)
    if not rows:
        await m.answer("На эту неделю слоты ещё не созданы.")
        return
    lines = ["<b>Расписание недели</b>"]
    for sid, st, free_left in rows:
        lines.append(f"• #{sid} — {st:%a %d.%m %H:%M} (свободно: {free_left})")
    await m.answer("\n".join(lines))


@dp.message(Command("me"))
async def me(m: Message):
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
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
        lines.append(f"• booking {bid}: {st:%a %d.%m %H:%M} ({kind})")
    await m.answer("\n".join(lines))


@dp.message(F.text.startswith("ses_"))
async def open_by_code(m: Message):
    code = m.text[4:]
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        row = await q1(conn, "SELECT id FROM tennis.sessions WHERE to_char(starts_at,'YYYY-MM-DD_HH24-MI')=%s", code)
    if not row:
        await m.answer("Слот не найден.")
        return
    await open_session(m.from_user.id, m, None, row[0])


async def open_session(tg_user_id: int, m: Message|None, c: CallbackQuery|None, sid: int):
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        uid = await ensure_user(conn, tg_user_id, (m.from_user if m else c.from_user).full_name)
        row = await q1(conn, """
            SELECT s.id, s.starts_at, s.ends_at, v.free_left,
                   EXISTS(SELECT 1 FROM tennis.bookings b
                          WHERE b.session_id=s.id AND b.user_id=%s AND b.status='booked') AS is_booked
            FROM tennis.sessions s
            JOIN tennis.v_session_load v USING(id)
            WHERE s.id=%s
        """, uid, sid)
    if not row:
        text, markup = "Слот не найден.", None
    else:
        _sid, st, en, free_left, is_booked = row
        text = f"<b>Слот #{_sid}</b>\n{st:%a %d.%m %H:%M}–{en:%H:%M}\nСвободно: {free_left}"
        markup = kb(_sid, is_booked, free_left)
    if m:
        await m.answer(text, reply_markup=markup)
    else:
        await c.message.edit_text(text, reply_markup=markup)


@dp.callback_query(F.data.startswith("book:"))
async def cb_book(c: CallbackQuery):
    _, sid, kind = c.data.split(":"); sid = int(sid)
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        uid = await ensure_user(conn, c.from_user.id, c.from_user.full_name)
        try:
            async with conn.transaction():
                msg = (await q1(conn, "SELECT tennis.book_session(%s,%s,%s)", uid, sid, kind))[0]
        except Exception as e:
            msg = str(e)
    await c.answer(msg, show_alert=(msg!="OK"))
    await open_session(c.from_user.id, None, c, sid)


@dp.callback_query(F.data.startswith("cancel:"))
async def cb_cancel(c: CallbackQuery):
    sid = int(c.data.split(":")[1])
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        uid = await ensure_user(conn, c.from_user.id, c.from_user.full_name)
        row = await q1(conn,
            "SELECT id FROM tennis.bookings WHERE user_id=%s AND session_id=%s AND status='booked'",
            uid, sid)
        if not row:
            await c.answer("У вас нет активной записи.", show_alert=True)
            return
        bid = row[0]
        msg = (await q1(conn, "SELECT tennis.cancel_booking(%s)", bid))[0]
    await c.answer(msg, show_alert=(msg!="OK"))
    await open_session(c.from_user.id, None, c, sid)


# ─── НАДЁЖНЫЙ КОНЕЦ С KEEP-ALIVE ──────────────────────────────

@dp.message(Command("ping"))
async def ping(m: Message):
    await m.answer("pong")


async def main():
    print(">>> Bot container started", flush=True)
    try:
        print(">>> Starting polling...", flush=True)
        await dp.start_polling(bot)
    except Exception as e:
        print(">>> Unhandled exception in polling:", e, file=sys.stderr, flush=True)
        traceback.print_exc()
    finally:
        # чтобы Railway не завершал контейнер, если polling остановился
        print(">>> Polling stopped, keeping container alive...", flush=True)
        while True:
            await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
