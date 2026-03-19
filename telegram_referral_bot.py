import asyncio
import logging
import os
import sqlite3
from contextlib import closing
from datetime import datetime
from typing import Optional

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message


# =========================================================
# CONFIG
# =========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8702728385:AAESnfWAqChZJ1dGAx0iPbN3we17jatVRHU")
ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "8034491282").split(",")
    if x.strip()
}
WITHDRAWALS_CHAT_ID = int(os.getenv("WITHDRAWALS_CHAT_ID", "-1003869807196"))
BOT_USERNAME = os.getenv("BOT_USERNAME", "your_bot_username")
REFERRAL_REWARD = float(os.getenv("REFERRAL_REWARD", "0.07"))
MIN_WITHDRAW = float(os.getenv("MIN_WITHDRAW", "2.0"))
DB_PATH = os.getenv("DB_PATH", "referral_bot.db")

if BOT_TOKEN == "PASTE_BOT_TOKEN_HERE":
    raise RuntimeError("Укажи BOT_TOKEN в переменных окружения")

logging.basicConfig(level=logging.INFO)


# =========================================================
# DATABASE
# =========================================================
class Database:
    def __init__(self, path: str):
        self.path = path
        self._init_db()

    def _connect(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with closing(self._connect()) as conn, conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    full_name TEXT,
                    balance REAL NOT NULL DEFAULT 0,
                    hold_balance REAL NOT NULL DEFAULT 0,
                    referred_by INTEGER,
                    referrals_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    is_blocked INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS referrals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    inviter_id INTEGER NOT NULL,
                    invited_id INTEGER NOT NULL UNIQUE,
                    reward REAL NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sponsor_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT,
                    join_url TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    reward REAL NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_task_completions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    task_id INTEGER NOT NULL,
                    reward REAL NOT NULL,
                    completed_at TEXT NOT NULL,
                    UNIQUE(user_id, task_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    method TEXT NOT NULL,
                    requisites TEXT NOT NULL,
                    status TEXT NOT NULL,
                    admin_id INTEGER,
                    admin_note TEXT,
                    created_at TEXT NOT NULL,
                    processed_at TEXT,
                    channel_message_id INTEGER
                )
                """
            )

    def add_or_get_user(self, user_id: int, username: Optional[str], full_name: str):
        now = datetime.utcnow().isoformat()
        with closing(self._connect()) as conn, conn:
            row = conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            if row:
                conn.execute(
                    "UPDATE users SET username = ?, full_name = ? WHERE user_id = ?",
                    (username, full_name, user_id),
                )
                return dict(row), False

            conn.execute(
                """
                INSERT INTO users (user_id, username, full_name, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (user_id, username, full_name, now),
            )
            row = conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            return dict(row), True

    def get_user(self, user_id: int):
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            return dict(row) if row else None

    def bind_referral(self, invited_id: int, inviter_id: int) -> bool:
        if invited_id == inviter_id:
            return False

        now = datetime.utcnow().isoformat()
        with closing(self._connect()) as conn, conn:
            invited = conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (invited_id,)
            ).fetchone()
            inviter = conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (inviter_id,)
            ).fetchone()

            if not invited or not inviter:
                return False
            if invited["referred_by"] is not None:
                return False

            existing = conn.execute(
                "SELECT 1 FROM referrals WHERE invited_id = ?", (invited_id,)
            ).fetchone()
            if existing:
                return False

            conn.execute(
                "UPDATE users SET referred_by = ? WHERE user_id = ?",
                (inviter_id, invited_id),
            )
            conn.execute(
                "UPDATE users SET balance = balance + ?, referrals_count = referrals_count + 1 WHERE user_id = ?",
                (REFERRAL_REWARD, inviter_id),
            )
            conn.execute(
                """
                INSERT INTO referrals (inviter_id, invited_id, reward, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (inviter_id, invited_id, REFERRAL_REWARD, now),
            )
            return True

    def get_stats(self, user_id: int):
        with closing(self._connect()) as conn:
            user = conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            if not user:
                return None
            completed_tasks = conn.execute(
                "SELECT COUNT(*) AS cnt FROM user_task_completions WHERE user_id = ?",
                (user_id,),
            ).fetchone()["cnt"]
            approved_withdrawals = conn.execute(
                "SELECT COALESCE(SUM(amount), 0) AS total FROM withdrawals WHERE user_id = ? AND status = 'approved'",
                (user_id,),
            ).fetchone()["total"]
            return {
                "balance": user["balance"],
                "hold_balance": user["hold_balance"],
                "referrals_count": user["referrals_count"],
                "completed_tasks": completed_tasks,
                "approved_withdrawals": approved_withdrawals,
            }

    def get_active_tasks(self):
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM sponsor_tasks WHERE is_active = 1 ORDER BY id DESC"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_task(self, task_id: int):
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM sponsor_tasks WHERE id = ?", (task_id,)
            ).fetchone()
            return dict(row) if row else None

    def create_task(self, title: str, description: str, join_url: str, channel_id: str, reward: float):
        now = datetime.utcnow().isoformat()
        with closing(self._connect()) as conn, conn:
            cur = conn.execute(
                """
                INSERT INTO sponsor_tasks (title, description, join_url, channel_id, reward, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (title, description, join_url, channel_id, reward, now),
            )
            return cur.lastrowid

    def deactivate_task(self, task_id: int) -> bool:
        with closing(self._connect()) as conn, conn:
            cur = conn.execute(
                "UPDATE sponsor_tasks SET is_active = 0 WHERE id = ? AND is_active = 1",
                (task_id,),
            )
            return cur.rowcount > 0

    def has_completed_task(self, user_id: int, task_id: int) -> bool:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT 1 FROM user_task_completions WHERE user_id = ? AND task_id = ?",
                (user_id, task_id),
            ).fetchone()
            return bool(row)

    def complete_task(self, user_id: int, task_id: int, reward: float) -> bool:
        now = datetime.utcnow().isoformat()
        with closing(self._connect()) as conn, conn:
            exists = conn.execute(
                "SELECT 1 FROM user_task_completions WHERE user_id = ? AND task_id = ?",
                (user_id, task_id),
            ).fetchone()
            if exists:
                return False

            conn.execute(
                """
                INSERT INTO user_task_completions (user_id, task_id, reward, completed_at)
                VALUES (?, ?, ?, ?)
                """,
                (user_id, task_id, reward, now),
            )
            conn.execute(
                "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                (reward, user_id),
            )
            return True

    def create_withdrawal(self, user_id: int, amount: float, method: str, requisites: str) -> int:
        now = datetime.utcnow().isoformat()
        with closing(self._connect()) as conn, conn:
            user = conn.execute(
                "SELECT balance FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            if not user or user["balance"] < amount:
                raise ValueError("Недостаточно средств")
            if amount < MIN_WITHDRAW:
                raise ValueError("Сумма меньше минимального вывода")

            conn.execute(
                "UPDATE users SET balance = balance - ?, hold_balance = hold_balance + ? WHERE user_id = ?",
                (amount, amount, user_id),
            )
            cur = conn.execute(
                """
                INSERT INTO withdrawals (user_id, amount, method, requisites, status, created_at)
                VALUES (?, ?, ?, ?, 'pending', ?)
                """,
                (user_id, amount, method, requisites, now),
            )
            return cur.lastrowid

    def set_withdrawal_channel_message(self, withdrawal_id: int, message_id: int):
        with closing(self._connect()) as conn, conn:
            conn.execute(
                "UPDATE withdrawals SET channel_message_id = ? WHERE id = ?",
                (message_id, withdrawal_id),
            )

    def get_withdrawal(self, withdrawal_id: int):
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM withdrawals WHERE id = ?", (withdrawal_id,)
            ).fetchone()
            return dict(row) if row else None

    def process_withdrawal(self, withdrawal_id: int, admin_id: int, approve: bool, admin_note: str = ""):
        now = datetime.utcnow().isoformat()
        with closing(self._connect()) as conn, conn:
            wd = conn.execute(
                "SELECT * FROM withdrawals WHERE id = ?", (withdrawal_id,)
            ).fetchone()
            if not wd:
                raise ValueError("Заявка не найдена")
            if wd["status"] != "pending":
                raise ValueError("Заявка уже обработана")

            if approve:
                conn.execute(
                    "UPDATE users SET hold_balance = hold_balance - ? WHERE user_id = ?",
                    (wd["amount"], wd["user_id"]),
                )
                conn.execute(
                    """
                    UPDATE withdrawals
                    SET status = 'approved', admin_id = ?, admin_note = ?, processed_at = ?
                    WHERE id = ?
                    """,
                    (admin_id, admin_note, now, withdrawal_id),
                )
            else:
                conn.execute(
                    "UPDATE users SET hold_balance = hold_balance - ?, balance = balance + ? WHERE user_id = ?",
                    (wd["amount"], wd["amount"], wd["user_id"]),
                )
                conn.execute(
                    """
                    UPDATE withdrawals
                    SET status = 'rejected', admin_id = ?, admin_note = ?, processed_at = ?
                    WHERE id = ?
                    """,
                    (admin_id, admin_note, now, withdrawal_id),
                )

    def get_top_users(self, limit: int = 10):
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT user_id, username, full_name, referrals_count, balance FROM users ORDER BY referrals_count DESC, balance DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]


db = Database(DB_PATH)


# =========================================================
# KEYBOARDS
# =========================================================
def main_menu():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👥 Рефералы", callback_data="menu_ref")],
            [InlineKeyboardButton(text="💼 Баланс", callback_data="menu_balance")],
            [InlineKeyboardButton(text="🎯 Задания", callback_data="menu_tasks")],
            [InlineKeyboardButton(text="💸 Вывод", callback_data="menu_withdraw")],
            [InlineKeyboardButton(text="🏆 Топ", callback_data="menu_top")],
        ]
    )


def back_menu():
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ В меню", callback_data="back_main")]]
    )


def withdrawal_moderation_kb(withdrawal_id: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Одобрить", callback_data=f"wd_approve:{withdrawal_id}"
                ),
                InlineKeyboardButton(
                    text="❌ Отклонить", callback_data=f"wd_reject:{withdrawal_id}"
                ),
            ]
        ]
    )


def task_card_kb(task_id: int, join_url: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Подписаться", url=join_url)],
            [InlineKeyboardButton(text="✅ Проверить", callback_data=f"check_task:{task_id}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_tasks")],
        ]
    )


# =========================================================
# STATES
# =========================================================
class WithdrawStates(StatesGroup):
    waiting_method = State()
    waiting_requisites = State()
    waiting_amount = State()


class CreateTaskStates(StatesGroup):
    waiting_payload = State()


# =========================================================
# HELPERS
# =========================================================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def money(value: float) -> str:
    return f"{value:.2f}$"


def user_link(user_id: int, full_name: str) -> str:
    safe = full_name.replace("<", "").replace(">", "")
    return f'<a href="tg://user?id={user_id}">{safe}</a>'


async def safe_edit(target, text: str, reply_markup=None):
    try:
        await target.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest:
        await target.answer(text, reply_markup=reply_markup)


async def check_subscription(bot: Bot, user_id: int, channel_id: str) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
        return member.status in {"member", "administrator", "creator"}
    except Exception:
        return False


async def render_main(message: Message | CallbackQuery):
    text = (
        "<b>Главное меню</b>\n\n"
        "Добро пожаловать в реферального бота.\n"
        f"• Награда за реферала: <b>{money(REFERRAL_REWARD)}</b>\n"
        f"• Минимальный вывод: <b>{money(MIN_WITHDRAW)}</b>\n\n"
        "Выбери раздел ниже."
    )
    if isinstance(message, CallbackQuery):
        await safe_edit(message.message, text, main_menu())
        await message.answer()
    else:
        await message.answer(text, reply_markup=main_menu())


# =========================================================
# ROUTER
# =========================================================
router = Router()


@router.message(CommandStart())
async def start_handler(message: Message):
    args = message.text.split(maxsplit=1)
    ref_arg = None
    if len(args) > 1:
        ref_arg = args[1].strip()

    _, created = db.add_or_get_user(
        user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
    )

    referral_text = ""
    if ref_arg and ref_arg.startswith("ref_"):
        try:
            inviter_id = int(ref_arg.replace("ref_", ""))
            bound = db.bind_referral(message.from_user.id, inviter_id)
            if bound:
                referral_text = (
                    f"\n🎉 Ты зарегистрировался по реферальной ссылке. "
                    f"Пригласивший получил {money(REFERRAL_REWARD)}."
                )
                try:
                    await message.bot.send_message(
                        inviter_id,
                        (
                            f"🎉 У тебя новый реферал: {user_link(message.from_user.id, message.from_user.full_name)}\n"
                            f"Начислено: <b>{money(REFERRAL_REWARD)}</b>"
                        ),
                    )
                except Exception:
                    pass
        except ValueError:
            pass

    welcome = (
        "<b>Бот запущен</b>\n\n"
        f"За каждого приглашённого пользователя начисляется <b>{money(REFERRAL_REWARD)}</b>."
        f"\nМинимальный вывод: <b>{money(MIN_WITHDRAW)}</b>."
        f"{referral_text}"
    )
    if created:
        welcome += "\n\nТы зарегистрирован в системе."

    await message.answer(welcome, reply_markup=main_menu())


@router.message(Command("menu"))
async def menu_cmd(message: Message):
    db.add_or_get_user(message.from_user.id, message.from_user.username, message.from_user.full_name)
    await render_main(message)


@router.callback_query(F.data == "back_main")
async def back_main(call: CallbackQuery):
    await render_main(call)


@router.callback_query(F.data == "menu_balance")
async def menu_balance(call: CallbackQuery):
    stats = db.get_stats(call.from_user.id)
    text = (
        "<b>Твой баланс</b>\n\n"
        f"Доступно: <b>{money(stats['balance'])}</b>\n"
        f"На проверке: <b>{money(stats['hold_balance'])}</b>\n"
        f"Всего выведено: <b>{money(stats['approved_withdrawals'])}</b>\n"
        f"Рефералов: <b>{stats['referrals_count']}</b>\n"
        f"Выполнено заданий: <b>{stats['completed_tasks']}</b>"
    )
    await safe_edit(call.message, text, back_menu())
    await call.answer()


@router.callback_query(F.data == "menu_ref")
async def menu_ref(call: CallbackQuery):
    stats = db.get_stats(call.from_user.id)
    link = f"https://t.me/{BOT_USERNAME}?start=ref_{call.from_user.id}"
    text = (
        "<b>Реферальная система</b>\n\n"
        f"Твоя ссылка:\n<code>{link}</code>\n\n"
        f"За 1 реферала: <b>{money(REFERRAL_REWARD)}</b>\n"
        f"Твои рефералы: <b>{stats['referrals_count']}</b>\n\n"
        "Отправляй ссылку друзьям. Когда новый пользователь запустит бота по ней, тебе начислится награда."
    )
    await safe_edit(call.message, text, back_menu())
    await call.answer()


@router.callback_query(F.data == "menu_top")
async def menu_top(call: CallbackQuery):
    top = db.get_top_users(10)
    lines = ["<b>Топ участников</b>"]
    if not top:
        lines.append("\nПока данных нет.")
    else:
        lines.append("")
        for i, row in enumerate(top, start=1):
            name = row["full_name"] or row["username"] or str(row["user_id"])
            lines.append(
                f"{i}. {name} — {row['referrals_count']} реф., баланс {money(row['balance'])}"
            )
    await safe_edit(call.message, "\n".join(lines), back_menu())
    await call.answer()


@router.callback_query(F.data == "menu_tasks")
async def menu_tasks(call: CallbackQuery):
    tasks = db.get_active_tasks()
    if not tasks:
        text = "<b>Задания</b>\n\nСейчас активных заданий нет."
        await safe_edit(call.message, text, back_menu())
        await call.answer()
        return

    keyboard_rows = []
    for task in tasks[:20]:
        keyboard_rows.append([
            InlineKeyboardButton(
                text=f"#{task['id']} {task['title']} • {money(task['reward'])}",
                callback_data=f"task:{task['id']}",
            )
        ])
    keyboard_rows.append([InlineKeyboardButton(text="⬅️ В меню", callback_data="back_main")])
    text = (
        "<b>Задания от спонсоров</b>\n\n"
        "Выбери задание. После подписки нажми «Проверить», и награда начислится автоматически."
    )
    await safe_edit(call.message, text, InlineKeyboardMarkup(inline_keyboard=keyboard_rows))
    await call.answer()


@router.callback_query(F.data.startswith("task:"))
async def open_task(call: CallbackQuery):
    task_id = int(call.data.split(":")[1])
    task = db.get_task(task_id)
    if not task or not task["is_active"]:
        await call.answer("Задание не найдено или отключено", show_alert=True)
        return

    done = db.has_completed_task(call.from_user.id, task_id)
    status = "✅ Уже выполнено" if done else "⌛ Не выполнено"
    text = (
        f"<b>{task['title']}</b>\n\n"
        f"{task['description'] or 'Подпишись на канал и получи награду.'}\n\n"
        f"Награда: <b>{money(task['reward'])}</b>\n"
        f"Статус: <b>{status}</b>"
    )
    await safe_edit(call.message, text, task_card_kb(task_id, task["join_url"]))
    await call.answer()


@router.callback_query(F.data.startswith("check_task:"))
async def check_task_handler(call: CallbackQuery):
    task_id = int(call.data.split(":")[1])
    task = db.get_task(task_id)
    if not task or not task["is_active"]:
        await call.answer("Задание недоступно", show_alert=True)
        return

    if db.has_completed_task(call.from_user.id, task_id):
        await call.answer("Ты уже получил награду по этому заданию", show_alert=True)
        return

    subscribed = await check_subscription(call.bot, call.from_user.id, task["channel_id"])
    if not subscribed:
        await call.answer("Сначала подпишись на канал, потом нажми проверить", show_alert=True)
        return

    completed = db.complete_task(call.from_user.id, task_id, task["reward"])
    if completed:
        await call.answer(f"Готово! Начислено {money(task['reward'])}", show_alert=True)
        await open_task(call)
    else:
        await call.answer("Уже выполнено", show_alert=True)


@router.callback_query(F.data == "menu_withdraw")
async def menu_withdraw(call: CallbackQuery, state: FSMContext):
    stats = db.get_stats(call.from_user.id)
    text = (
        "<b>Вывод средств</b>\n\n"
        f"Доступно: <b>{money(stats['balance'])}</b>\n"
        f"Минимальная сумма вывода: <b>{money(MIN_WITHDRAW)}</b>\n\n"
        "Нажми на кнопку ниже, чтобы создать заявку."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📝 Создать заявку", callback_data="withdraw_create")],
            [InlineKeyboardButton(text="⬅️ В меню", callback_data="back_main")],
        ]
    )
    await state.clear()
    await safe_edit(call.message, text, kb)
    await call.answer()


@router.callback_query(F.data == "withdraw_create")
async def withdraw_create(call: CallbackQuery, state: FSMContext):
    stats = db.get_stats(call.from_user.id)
    if stats["balance"] < MIN_WITHDRAW:
        await call.answer(
            f"Минимум для вывода {money(MIN_WITHDRAW)}. У тебя {money(stats['balance'])}",
            show_alert=True,
        )
        return

    await state.set_state(WithdrawStates.waiting_method)
    await call.message.answer(
        "Введите способ вывода.\nПримеры: <code>USDT TRC20</code>, <code>Карта</code>, <code>ЮMoney</code>"
    )
    await call.answer()


@router.message(WithdrawStates.waiting_method)
async def withdraw_method(message: Message, state: FSMContext):
    await state.update_data(method=message.text.strip())
    await state.set_state(WithdrawStates.waiting_requisites)
    await message.answer("Теперь отправь реквизиты для выплаты.")


@router.message(WithdrawStates.waiting_requisites)
async def withdraw_requisites(message: Message, state: FSMContext):
    await state.update_data(requisites=message.text.strip())
    await state.set_state(WithdrawStates.waiting_amount)
    stats = db.get_stats(message.from_user.id)
    await message.answer(
        f"Введи сумму вывода числом. Доступно: {money(stats['balance'])}. Минимум: {money(MIN_WITHDRAW)}"
    )


@router.message(WithdrawStates.waiting_amount)
async def withdraw_amount(message: Message, state: FSMContext):
    try:
        amount = float(message.text.replace(",", ".").strip())
    except ValueError:
        await message.answer("Введи сумму числом. Например: 2 или 2.5")
        return

    data = await state.get_data()
    try:
        withdrawal_id = db.create_withdrawal(
            user_id=message.from_user.id,
            amount=amount,
            method=data["method"],
            requisites=data["requisites"],
        )
    except ValueError as e:
        await message.answer(str(e))
        return

    wd = db.get_withdrawal(withdrawal_id)
    moderation_text = (
        "<b>Новая заявка на вывод</b>\n\n"
        f"ID заявки: <code>{withdrawal_id}</code>\n"
        f"Пользователь: {user_link(message.from_user.id, message.from_user.full_name)}\n"
        f"User ID: <code>{message.from_user.id}</code>\n"
        f"Сумма: <b>{money(wd['amount'])}</b>\n"
        f"Способ: <b>{wd['method']}</b>\n"
        f"Реквизиты: <code>{wd['requisites']}</code>\n"
        f"Создано: <code>{wd['created_at']}</code>"
    )
    sent = await message.bot.send_message(
        WITHDRAWALS_CHAT_ID,
        moderation_text,
        reply_markup=withdrawal_moderation_kb(withdrawal_id),
    )
    db.set_withdrawal_channel_message(withdrawal_id, sent.message_id)

    await message.answer(
        "✅ Заявка создана и отправлена на модерацию.\n"
        "После проверки администратор одобрит или отклонит её."
    )
    await state.clear()


@router.callback_query(F.data.startswith("wd_approve:"))
async def approve_withdrawal(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    withdrawal_id = int(call.data.split(":")[1])
    try:
        db.process_withdrawal(withdrawal_id, call.from_user.id, approve=True, admin_note="Одобрено")
        wd = db.get_withdrawal(withdrawal_id)
        await safe_edit(
            call.message,
            call.message.html_text + f"\n\n✅ <b>Статус:</b> Одобрено админом {call.from_user.id}",
            None,
        )
        try:
            await call.bot.send_message(
                wd["user_id"],
                f"✅ Твоя заявка #{withdrawal_id} на сумму {money(wd['amount'])} одобрена."
            )
        except Exception:
            pass
        await call.answer("Заявка одобрена", show_alert=True)
    except ValueError as e:
        await call.answer(str(e), show_alert=True)


@router.callback_query(F.data.startswith("wd_reject:"))
async def reject_withdrawal(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    withdrawal_id = int(call.data.split(":")[1])
    try:
        db.process_withdrawal(withdrawal_id, call.from_user.id, approve=False, admin_note="Отклонено")
        wd = db.get_withdrawal(withdrawal_id)
        await safe_edit(
            call.message,
            call.message.html_text + f"\n\n❌ <b>Статус:</b> Отклонено админом {call.from_user.id}",
            None,
        )
        try:
            await call.bot.send_message(
                wd["user_id"],
                f"❌ Твоя заявка #{withdrawal_id} на сумму {money(wd['amount'])} отклонена. Деньги возвращены на баланс."
            )
        except Exception:
            pass
        await call.answer("Заявка отклонена", show_alert=True)
    except ValueError as e:
        await call.answer(str(e), show_alert=True)


# =========================================================
# ADMIN COMMANDS
# =========================================================
@router.message(Command("admin"))
async def admin_help(message: Message):
    if not is_admin(message.from_user.id):
        return
    text = (
        "<b>Админ-команды</b>\n\n"
        "<code>/addtask</code> — начать создание задания\n"
        "<code>/deltask ID</code> — отключить задание\n"
        "<code>/tasks</code> — список активных заданий\n"
        "<code>/stats</code> — краткая статистика"
    )
    await message.answer(text)


@router.message(Command("addtask"))
async def add_task_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(CreateTaskStates.waiting_payload)
    await message.answer(
        "Отправь данные задания одной строкой в формате:\n\n"
        "<code>Название | Описание | Ссылка | channel_id или @username | Награда</code>\n\n"
        "Пример:\n"
        "<code>Подписка на канал | Подпишись на наш канал | https://t.me/testchannel | @testchannel | 0.15</code>"
    )


@router.message(CreateTaskStates.waiting_payload)
async def add_task_finish(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    parts = [x.strip() for x in message.text.split("|")]
    if len(parts) != 5:
        await message.answer("Неверный формат. Нужно 5 частей через |")
        return

    title, description, join_url, channel_id, reward_text = parts
    try:
        reward = float(reward_text.replace(",", "."))
    except ValueError:
        await message.answer("Награда должна быть числом")
        return

    task_id = db.create_task(title, description, join_url, channel_id, reward)
    await state.clear()
    await message.answer(
        f"✅ Задание создано. ID: <code>{task_id}</code>\n"
        f"Название: <b>{title}</b>\n"
        f"Награда: <b>{money(reward)}</b>"
    )


@router.message(Command("deltask"))
async def delete_task(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: <code>/deltask ID</code>")
        return
    task_id = int(parts[1])
    ok = db.deactivate_task(task_id)
    await message.answer("✅ Задание отключено" if ok else "Задание не найдено")


@router.message(Command("tasks"))
async def tasks_list(message: Message):
    if not is_admin(message.from_user.id):
        return
    tasks = db.get_active_tasks()
    if not tasks:
        await message.answer("Активных заданий нет")
        return
    text = ["<b>Активные задания</b>"]
    for t in tasks:
        text.append(
            f"\n#{t['id']} {t['title']}\nКанал: <code>{t['channel_id']}</code>\nНаграда: {money(t['reward'])}"
        )
    await message.answer("\n".join(text))


@router.message(Command("stats"))
async def admin_stats(message: Message):
    if not is_admin(message.from_user.id):
        return
    with closing(db._connect()) as conn:
        users = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
        referrals = conn.execute("SELECT COUNT(*) AS c FROM referrals").fetchone()["c"]
        pending = conn.execute("SELECT COUNT(*) AS c FROM withdrawals WHERE status='pending'").fetchone()["c"]
        approved_sum = conn.execute(
            "SELECT COALESCE(SUM(amount), 0) AS s FROM withdrawals WHERE status='approved'"
        ).fetchone()["s"]
        tasks_done = conn.execute("SELECT COUNT(*) AS c FROM user_task_completions").fetchone()["c"]
    text = (
        "<b>Статистика</b>\n\n"
        f"Пользователей: <b>{users}</b>\n"
        f"Рефералов: <b>{referrals}</b>\n"
        f"Выполнено заданий: <b>{tasks_done}</b>\n"
        f"Заявок в ожидании: <b>{pending}</b>\n"
        f"Всего одобрено выплат: <b>{money(approved_sum)}</b>"
    )
    await message.answer(text)


# =========================================================
# FALLBACK
# =========================================================
@router.message()
async def fallback(message: Message):
    db.add_or_get_user(message.from_user.id, message.from_user.username, message.from_user.full_name)
    await message.answer(
        "Не понял команду. Используй /start или /menu",
        reply_markup=main_menu(),
    )


# =========================================================
# RUN
# =========================================================
async def main():
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
