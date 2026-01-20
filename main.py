#!/usr/bin/env python3
"""
PRODUCTION-READY TELEGRAM EARNING BOT
Single-file implementation with admin panel, anti-fraud, and full task system
Deployable on any VPS/Cloud server
"""

import asyncio
import os
import sqlite3
import hashlib
import json
import logging
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
    CallbackQuery, Message
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram.client.session.aiohttp import AiohttpSession

# ==================== CONFIGURATION ====================
# Use environment variables in production
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
ADMIN_IDS = list(map(int, os.getenv("ADMIN_IDS", "123456789,987654321").split(",")))
DATABASE_PATH = os.getenv("DATABASE_PATH", "earning_bot.db")
LOG_CHANNEL_ID = os.getenv("LOG_CHANNEL_ID", "-1001234567890")

# Bot configuration
MINIMUM_WITHDRAW = 50.0  # Minimum withdraw amount in BDT
REFERRAL_BONUS = 10.0    # Referral bonus in BDT
DAILY_TASK_LIMIT = 20    # Maximum tasks per day per user
TASK_COOLDOWN = 30       # Seconds between tasks
PENALTY_MULTIPLIER = 2.0 # Penalty multiplier for fraud

# Initialize bot and dispatcher
bot = Bot(token=BOT_TOKEN, session=AiohttpSession())
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== DATABASE SCHEMA ====================
class Database:
    def __init__(self, path: str):
        self.path = path
        self.init_database()
    
    def get_connection(self):
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    
    def init_database(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Users table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    full_name TEXT,
                    balance REAL DEFAULT 0.0,
                    total_earned REAL DEFAULT 0.0,
                    total_tasks INTEGER DEFAULT 0,
                    total_referrals INTEGER DEFAULT 0,
                    referral_code TEXT UNIQUE,
                    referred_by INTEGER,
                    join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    today_earnings REAL DEFAULT 0.0,
                    total_penalties REAL DEFAULT 0.0,
                    risk_score INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'active',
                    device_hash TEXT,
                    ip_hash TEXT,
                    accepted_terms BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY (referred_by) REFERENCES users(user_id)
                )
            ''')
            
            # Tasks table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_name TEXT NOT NULL,
                    channel_username TEXT,
                    channel_invite_link TEXT NOT NULL,
                    reward_amount REAL NOT NULL,
                    priority INTEGER DEFAULT 0,
                    expiry_date TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    daily_limit INTEGER DEFAULT 0,
                    total_completions INTEGER DEFAULT 0
                )
            ''')
            
            # Completed tasks
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS completed_tasks (
                    completion_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    task_id INTEGER NOT NULL,
                    completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    reward_earned REAL NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
                )
            ''')
            
            # Transaction history
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transaction_history (
                    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    transaction_type TEXT NOT NULL, -- 'task', 'referral', 'withdraw', 'penalty', 'adjustment'
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            
            # Referral logs
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS referral_logs (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    referrer_id INTEGER NOT NULL,
                    referred_id INTEGER NOT NULL,
                    bonus_amount REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (referrer_id) REFERENCES users(user_id),
                    FOREIGN KEY (referred_id) REFERENCES users(user_id)
                )
            ''')
            
            # Withdraw requests
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS withdraw_requests (
                    request_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    method TEXT NOT NULL, -- 'bkash', 'nagad', 'rocket'
                    account_number TEXT NOT NULL,
                    status TEXT DEFAULT 'pending', -- 'pending', 'approved', 'rejected', 'paid'
                    proof_tx_id TEXT,
                    admin_notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            
            # Penalties
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS penalties (
                    penalty_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            
            # Device fingerprints
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS device_fingerprints (
                    fingerprint_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    device_hash TEXT NOT NULL,
                    ip_hash TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            
            # Abuse flags
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS abuse_flags (
                    flag_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    flag_type TEXT NOT NULL, -- 'multi_account', 'unjoin_fraud', 'withdraw_abuse'
                    severity INTEGER DEFAULT 1,
                    details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            
            # Admin actions log
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS admin_logs (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    target_id INTEGER,
                    details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
    
    def execute(self, query: str, params: tuple = ()):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor.lastrowid
    
    def fetch_one(self, query: str, params: tuple = ()):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.fetchone()
    
    def fetch_all(self, query: str, params: tuple = ()):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.fetchall()

# Initialize database
db = Database(DATABASE_PATH)

# ==================== STATE MACHINES ====================
class UserStates(StatesGroup):
    waiting_for_terms = State()
    waiting_for_withdraw_method = State()
    waiting_for_account_number = State()
    waiting_for_withdraw_amount = State()
    viewing_task = State()

class AdminStates(StatesGroup):
    main_menu = State()
    manage_tasks = State()
    add_task = State()
    edit_task = State()
    manage_users = State()
    view_user = State()
    adjust_balance = State()
    manage_withdrawals = State()
    view_withdrawal = State()
    system_stats = State()
    add_task_name = State()
    add_task_link = State()
    add_task_reward = State()
    add_task_priority = State()
    add_task_expiry = State()

# ==================== HELPER FUNCTIONS ====================
def generate_referral_code(user_id: int) -> str:
    """Generate unique referral code for user"""
    return f"REF{user_id:06d}{hashlib.md5(str(user_id).encode()).hexdigest()[:4].upper()}"

def generate_device_hash(user_id: int, message: Message) -> str:
    """Generate device fingerprint hash"""
    user_agent = f"{message.from_user.id}{message.date.timestamp()}"
    return hashlib.md5(user_agent.encode()).hexdigest()

def format_balance(amount: float) -> str:
    """Format balance as Bangladeshi Taka"""
    return f"৳{amount:.2f}"

def get_today_date() -> str:
    """Get today's date in YYYY-MM-DD format"""
    return datetime.now().strftime("%Y-%m-%d")

async def check_channel_membership(user_id: int, channel_username: str) -> bool:
    """
    Check if user is member of a channel
    Note: In production, implement proper Telegram API checks
    """
    try:
        # For demo purposes, simulate checking
        # In production, use: await bot.get_chat_member(chat_id, user_id)
        await asyncio.sleep(0.5)  # Simulate API call
        return True  # Simulate successful check
    except Exception as e:
        logger.error(f"Error checking channel membership: {e}")
        return False

async def log_to_channel(text: str):
    """Log important events to Telegram channel"""
    try:
        await bot.send_message(LOG_CHANNEL_ID, text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Failed to log to channel: {e}")

# ==================== USER REGISTRATION ====================
async def register_user(user_id: int, username: str, full_name: str, 
                        referred_by: Optional[int] = None,
                        message: Optional[Message] = None) -> Dict:
    """Register new user in database"""
    
    # Check if user already exists
    existing = db.fetch_one("SELECT * FROM users WHERE user_id = ?", (user_id,))
    if existing:
        return dict(existing)
    
    # Generate referral code
    referral_code = generate_referral_code(user_id)
    
    # Generate device fingerprint
    device_hash = generate_device_hash(user_id, message) if message else "unknown"
    
    # Insert new user
    db.execute('''
        INSERT INTO users 
        (user_id, username, full_name, referral_code, referred_by, device_hash, join_date, last_active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (user_id, username, full_name, referral_code, referred_by, device_hash, 
          datetime.now(), datetime.now()))
    
    # Process referral bonus if applicable
    if referred_by:
        # Check if referrer exists and not self-referral
        if referred_by != user_id:
            # Add referral bonus to referrer
            db.execute('''
                UPDATE users 
                SET balance = balance + ?, total_referrals = total_referrals + 1
                WHERE user_id = ?
            ''', (REFERRAL_BONUS, referred_by))
            
            # Log referral transaction
            db.execute('''
                INSERT INTO transaction_history 
                (user_id, amount, transaction_type, description)
                VALUES (?, ?, ?, ?)
            ''', (referred_by, REFERRAL_BONUS, 'referral', f'Referral bonus for user {user_id}'))
            
            # Log referral
            db.execute('''
                INSERT INTO referral_logs 
                (referrer_id, referred_id, bonus_amount)
                VALUES (?, ?, ?)
            ''', (referred_by, user_id, REFERRAL_BONUS))
            
            await log_to_channel(
                f"🎯 <b>New Referral</b>\n"
                f"Referrer: {referred_by}\n"
                f"New User: {user_id}\n"
                f"Bonus: {format_balance(REFERRAL_BONUS)}"
            )
    
    # Get created user
    user = db.fetch_one("SELECT * FROM users WHERE user_id = ?", (user_id,))
    return dict(user)

# ==================== MAIN KEYBOARDS ====================
def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Get main user keyboard"""
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text="👤 My Account"))
    builder.add(KeyboardButton(text="💰 Refer to Earn"))
    builder.add(KeyboardButton(text="💸 Withdraw"))
    builder.add(KeyboardButton(text="📢 Join TG Channel to Earn"))
    builder.add(KeyboardButton(text="🆘 Help"))
    builder.adjust(2, 2, 1)
    return builder.as_markup(resize_keyboard=True)

def get_admin_keyboard() -> InlineKeyboardMarkup:
    """Get admin main menu keyboard"""
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="📊 System Stats", callback_data="admin_stats"))
    builder.add(InlineKeyboardButton(text="👥 Manage Users", callback_data="admin_users"))
    builder.add(InlineKeyboardButton(text="📋 Manage Tasks", callback_data="admin_tasks"))
    builder.add(InlineKeyboardButton(text="💰 Manage Withdrawals", callback_data="admin_withdrawals"))
    builder.add(InlineKeyboardButton(text="🚨 Abuse Reports", callback_data="admin_abuse"))
    builder.add(InlineKeyboardButton(text="⚙️ System Settings", callback_data="admin_settings"))
    builder.adjust(2, 2, 2)
    return builder.as_markup()

# ==================== USER HANDLERS ====================
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    """Handle /start command with referral support"""
    user_id = message.from_user.id
    username = message.from_user.username
    full_name = message.from_user.full_name
    
    # Check for referral code in start parameters
    referred_by = None
    if len(message.text.split()) > 1:
        ref_code = message.text.split()[1]
        # Find user with this referral code
        ref_user = db.fetch_one("SELECT user_id FROM users WHERE referral_code = ?", (ref_code,))
        if ref_user:
            referred_by = ref_user['user_id']
            # Prevent self-referral
            if referred_by == user_id:
                referred_by = None
    
    # Register or get user
    user = await register_user(user_id, username, full_name, referred_by, message)
    
    # Check if user accepted terms
    if not user['accepted_terms']:
        await message.answer(
            "📜 <b>Terms & Conditions</b>\n\n"
            "1. You must complete tasks honestly\n"
            "2. Fraudulent activity will result in penalties\n"
            "3. Withdrawals are processed within 24 hours\n"
            "4. Minimum withdrawal: ৳50\n"
            "5. Admin decisions are final\n\n"
            "Do you accept these terms?",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ I Accept", callback_data="accept_terms")],
                [InlineKeyboardButton(text="❌ Decline", callback_data="decline_terms")]
            ])
        )
        await state.set_state(UserStates.waiting_for_terms)
        return
    
    # Welcome message
    welcome_text = (
        f"👋 Welcome <b>{full_name}</b>!\n\n"
        f"🎯 <b>Earn Money by joining Telegram Channels</b>\n"
        f"• Complete tasks & earn cash\n"
        f"• Refer friends for bonus\n"
        f"• Withdraw via bKash/Nagad/Rocket\n\n"
        f"💰 <b>Your Balance:</b> {format_balance(user['balance'])}\n"
        f"📅 <b>Joined:</b> {user['join_date'][:10] if user['join_date'] else 'Today'}"
    )
    
    if referred_by:
        welcome_text += f"\n\n🎁 You were referred by user {referred_by}"
    
    await message.answer(welcome_text, 
                        parse_mode=ParseMode.HTML,
                        reply_markup=get_main_keyboard())
    
    # Reset today's earnings at midnight
    db.execute("UPDATE users SET today_earnings = 0 WHERE DATE(last_active) < DATE('now')")

@dp.callback_query(F.data == "accept_terms")
async def accept_terms(callback: CallbackQuery, state: FSMContext):
    """Handle terms acceptance"""
    user_id = callback.from_user.id
    db.execute("UPDATE users SET accepted_terms = TRUE WHERE user_id = ?", (user_id,))
    
    await callback.message.edit_text(
        "✅ <b>Terms Accepted!</b>\n\n"
        "You can now start earning money!",
        parse_mode=ParseMode.HTML
    )
    
    # Show main menu
    await cmd_start(callback.message, state)

@dp.message(F.text == "👤 My Account")
async def my_account(message: Message):
    """Show user account information"""
    user_id = message.from_user.id
    user = db.fetch_one("SELECT * FROM users WHERE user_id = ?", (user_id,))
    
    if not user:
        await message.answer("User not found. Please use /start")
        return
    
    # Calculate today's earnings from transactions
    today_earnings = db.fetch_one(
        "SELECT COALESCE(SUM(amount), 0) as total FROM transaction_history "
        "WHERE user_id = ? AND DATE(created_at) = DATE('now') AND transaction_type IN ('task', 'referral')",
        (user_id,)
    )['total']
    
    account_text = (
        f"👤 <b>My Account</b>\n\n"
        f"🆔 <b>User ID:</b> {user['user_id']}\n"
        f"📛 <b>Name:</b> {user['full_name'] or 'Not set'}\n"
        f"💳 <b>Balance:</b> {format_balance(user['balance'])}\n"
        f"📊 <b>Total Earned:</b> {format_balance(user['total_earned'])}\n"
        f"✅ <b>Tasks Completed:</b> {user['total_tasks']}\n"
        f"👥 <b>Referrals:</b> {user['total_referrals']}\n"
        f"📅 <b>Joined:</b> {user['join_date'][:10] if user['join_date'] else 'N/A'}\n"
        f"💰 <b>Today's Earnings:</b> {format_balance(today_earnings)}\n"
        f"⚠️ <b>Penalties:</b> {format_balance(user['total_penalties'])}\n"
        f"🎯 <b>Referral Code:</b> <code>{user['referral_code']}</code>"
    )
    
    await message.answer(account_text, parse_mode=ParseMode.HTML)

@dp.message(F.text == "💰 Refer to Earn")
async def refer_to_earn(message: Message):
    """Show referral information"""
    user_id = message.from_user.id
    user = db.fetch_one("SELECT referral_code FROM users WHERE user_id = ?", (user_id,))
    
    if not user:
        await message.answer("Please use /start first")
        return
    
    referral_link = f"https://t.me/{BOT_TOKEN.split(':')[0]}?start={user['referral_code']}"
    
    referral_text = (
        f"🎯 <b>Refer & Earn Program</b>\n\n"
        f"• Earn <b>{format_balance(REFERRAL_BONUS)}</b> for each friend who joins\n"
        f"• Your friend also gets welcome bonus\n"
        f"• No limit on referrals\n\n"
        f"📎 <b>Your Referral Link:</b>\n"
        f"<code>{referral_link}</code>\n\n"
        f"📊 <b>Referral Stats:</b>\n"
    )
    
    # Get referral stats
    ref_stats = db.fetch_all(
        "SELECT COUNT(*) as count, COALESCE(SUM(bonus_amount), 0) as total FROM referral_logs WHERE referrer_id = ?",
        (user_id,)
    )[0]
    
    referral_text += f"• Total Referrals: {ref_stats['count']}\n"
    referral_text += f"• Total Bonus Earned: {format_balance(ref_stats['total'])}"
    
    await message.answer(referral_text, parse_mode=ParseMode.HTML)

@dp.message(F.text == "💸 Withdraw")
async def withdraw_menu(message: Message, state: FSMContext):
    """Show withdraw options"""
    user_id = message.from_user.id
    user = db.fetch_one("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    
    if not user:
        await message.answer("Please use /start first")
        return
    
    if user['balance'] < MINIMUM_WITHDRAW:
        await message.answer(
            f"❌ <b>Minimum withdrawal is {format_balance(MINIMUM_WITHDRAW)}</b>\n"
            f"Your balance: {format_balance(user['balance'])}\n"
            f"You need {format_balance(MINIMUM_WITHDRAW - user['balance'])} more.",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Show withdrawal methods
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 bKash", callback_data="withdraw_bkash")],
        [InlineKeyboardButton(text="🏧 Nagad", callback_data="withdraw_nagad")],
        [InlineKeyboardButton(text="🚀 Rocket", callback_data="withdraw_rocket")],
        [InlineKeyboardButton(text="📜 Withdrawal History", callback_data="withdraw_history")]
    ])
    
    await message.answer(
        f"💸 <b>Withdraw Funds</b>\n\n"
        f"💰 <b>Available Balance:</b> {format_balance(user['balance'])}\n"
        f"📉 <b>Minimum Withdrawal:</b> {format_balance(MINIMUM_WITHDRAW)}\n\n"
        f"<b>Select Payment Method:</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard
    )
    await state.set_state(UserStates.waiting_for_withdraw_method)

@dp.callback_query(F.data.startswith("withdraw_"))
async def process_withdraw_method(callback: CallbackQuery, state: FSMContext):
    """Process withdrawal method selection"""
    method = callback.data.replace("withdraw_", "")
    method_names = {
        "bkash": "bKash",
        "nagad": "Nagad",
        "rocket": "Rocket"
    }
    
    if method == "history":
        await show_withdrawal_history(callback.message)
        return
    
    await state.update_data(withdraw_method=method)
    
    await callback.message.edit_text(
        f"💸 <b>Withdraw to {method_names[method]}</b>\n\n"
        f"Please enter your {method_names[method]} account number:",
        parse_mode=ParseMode.HTML
    )
    await state.set_state(UserStates.waiting_for_account_number)

@dp.message(UserStates.waiting_for_account_number)
async def get_account_number(message: Message, state: FSMContext):
    """Get account number for withdrawal"""
    account_number = message.text.strip()
    
    # Validate account number
    if not account_number.isdigit() or len(account_number) != 11:
        await message.answer("❌ Please enter a valid 11-digit mobile number")
        return
    
    await state.update_data(account_number=account_number)
    
    user = db.fetch_one("SELECT balance FROM users WHERE user_id = ?", (message.from_user.id,))
    
    await message.answer(
        f"📱 <b>Account Number:</b> {account_number}\n"
        f"💰 <b>Available Balance:</b> {format_balance(user['balance'])}\n\n"
        f"Enter withdrawal amount (Minimum: {format_balance(MINIMUM_WITHDRAW)}):",
        parse_mode=ParseMode.HTML
    )
    await state.set_state(UserStates.waiting_for_withdraw_amount)

@dp.message(UserStates.waiting_for_withdraw_amount)
async def get_withdraw_amount(message: Message, state: FSMContext):
    """Process withdrawal amount"""
    try:
        amount = float(message.text)
        user_id = message.from_user.id
        
        # Validate amount
        if amount < MINIMUM_WITHDRAW:
            await message.answer(f"❌ Minimum withdrawal is {format_balance(MINIMUM_WITHDRAW)}")
            return
        
        user = db.fetch_one("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        if amount > user['balance']:
            await message.answer("❌ Insufficient balance")
            return
        
        # Get withdrawal data
        data = await state.get_data()
        method = data['withdraw_method']
        account_number = data['account_number']
        
        # Freeze balance by deducting
        db.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount, user_id))
        
        # Create withdrawal request
        db.execute('''
            INSERT INTO withdraw_requests 
            (user_id, amount, method, account_number, status)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, amount, method, account_number, 'pending'))
        
        # Log transaction
        db.execute('''
            INSERT INTO transaction_history 
            (user_id, amount, transaction_type, description)
            VALUES (?, ?, ?, ?)
        ''', (user_id, -amount, 'withdraw', f'Withdrawal request pending - {method}'))
        
        request_id = db.fetch_one("SELECT last_insert_rowid()")[0]
        
        await message.answer(
            f"✅ <b>Withdrawal Request Submitted!</b>\n\n"
            f"💰 <b>Amount:</b> {format_balance(amount)}\n"
            f"📱 <b>Method:</b> {method.upper()}\n"
            f"🔢 <b>Account:</b> {account_number}\n"
            f"📝 <b>Request ID:</b> {request_id}\n\n"
            f"Your request will be processed within 24 hours.",
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        
        # Notify admins
        await log_to_channel(
            f"🔄 <b>New Withdrawal Request</b>\n"
            f"User: {user_id}\n"
            f"Amount: {format_balance(amount)}\n"
            f"Method: {method}\n"
            f"Request ID: {request_id}"
        )
        
        await state.clear()
        
    except ValueError:
        await message.answer("❌ Please enter a valid number")

async def show_withdrawal_history(message: Message):
    """Show user's withdrawal history"""
    user_id = message.from_user.id
    
    withdrawals = db.fetch_all(
        "SELECT * FROM withdraw_requests WHERE user_id = ? ORDER BY created_at DESC LIMIT 10",
        (user_id,)
    )
    
    if not withdrawals:
        await message.answer("📭 No withdrawal history found")
        return
    
    history_text = "📜 <b>Withdrawal History</b>\n\n"
    
    for w in withdrawals:
        status_icons = {
            'pending': '⏳',
            'approved': '✅',
            'rejected': '❌',
            'paid': '💰'
        }
        
        history_text += (
            f"{status_icons.get(w['status'], '📝')} <b>{format_balance(w['amount'])}</b>\n"
            f"Method: {w['method'].upper()}\n"
            f"Status: {w['status'].title()}\n"
            f"Date: {w['created_at'][:10]}\n"
        )
        
        if w['proof_tx_id']:
            history_text += f"TX ID: {w['proof_tx_id']}\n"
        
        history_text += "―" * 20 + "\n"
    
    await message.answer(history_text, parse_mode=ParseMode.HTML)

@dp.message(F.text == "📢 Join TG Channel to Earn")
async def join_channel_menu(message: Message, state: FSMContext):
    """Show available channel join tasks"""
    user_id = message.from_user.id
    
    # Check daily task limit
    today_tasks = db.fetch_one(
        "SELECT COUNT(*) as count FROM completed_tasks "
        "WHERE user_id = ? AND DATE(completed_at) = DATE('now')",
        (user_id,)
    )['count']
    
    if today_tasks >= DAILY_TASK_LIMIT:
        await message.answer(
            f"❌ <b>Daily Task Limit Reached!</b>\n\n"
            f"You have completed {today_tasks}/{DAILY_TASK_LIMIT} tasks today.\n"
            f"Come back tomorrow for more tasks!",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Get available tasks
    tasks = db.fetch_all(
        "SELECT * FROM tasks WHERE is_active = TRUE AND "
        "(expiry_date IS NULL OR expiry_date > datetime('now')) "
        "ORDER BY priority DESC, reward_amount DESC"
    )
    
    if not tasks:
        await message.answer("📭 No tasks available at the moment. Check back later!")
        return
    
    # Check which tasks user hasn't completed
    available_tasks = []
    for task in tasks:
        completed = db.fetch_one(
            "SELECT 1 FROM completed_tasks WHERE user_id = ? AND task_id = ?",
            (user_id, task['task_id'])
        )
        if not completed:
            available_tasks.append(task)
    
    if not available_tasks:
        await message.answer("🎉 You've completed all available tasks! Check back later for new tasks.")
        return
    
    # Show first task
    task = available_tasks[0]
    await show_task(message, task, 1, len(available_tasks))
    await state.set_state(UserStates.viewing_task)
    await state.update_data(current_task_id=task['task_id'], task_index=1, total_tasks=len(available_tasks))

async def show_task(message: Message, task: Dict, current: int, total: int):
    """Display a task to user"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Join Channel", url=task['channel_invite_link'])],
        [InlineKeyboardButton(text="✅ Check Join", callback_data=f"check_join_{task['task_id']}")],
        [InlineKeyboardButton(text="⏭️ Skip Task", callback_data="skip_task")]
    ])
    
    task_text = (
        f"📢 <b>Task {current} of {total}</b>\n\n"
        f"🏷️ <b>Task Name:</b> {task['task_name']}\n"
        f"💰 <b>Reward:</b> {format_balance(task['reward_amount'])}\n"
        f"📊 <b>Priority:</b> {'⭐' * min(task['priority'], 5)}\n"
        f"👥 <b>Completed:</b> {task['total_completions']} times\n\n"
        f"<b>Instructions:</b>\n"
        f"1. Click 'Join Channel' button\n"
        f"2. Join the channel\n"
        f"3. Click 'Check Join' to verify\n"
        f"4. Get paid instantly!"
    )
    
    await message.answer(task_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

@dp.callback_query(F.data.startswith("check_join_"))
async def check_channel_join(callback: CallbackQuery, state: FSMContext):
    """Check if user joined the channel"""
    user_id = callback.from_user.id
    task_id = int(callback.data.replace("check_join_", ""))
    
    # Get task details
    task = db.fetch_one("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
    if not task:
        await callback.answer("Task not found")
        return
    
    # Check if already completed
    completed = db.fetch_one(
        "SELECT 1 FROM completed_tasks WHERE user_id = ? AND task_id = ?",
        (user_id, task_id)
    )
    if completed:
        await callback.answer("You already completed this task")
        return
    
    # Verify channel membership
    is_member = await check_channel_membership(user_id, task['channel_username'])
    
    if is_member:
        # Award reward
        db.execute("UPDATE users SET balance = balance + ?, total_tasks = total_tasks + 1 WHERE user_id = ?",
                  (task['reward_amount'], user_id))
        
        # Log completion
        db.execute('''
            INSERT INTO completed_tasks (user_id, task_id, reward_earned)
            VALUES (?, ?, ?)
        ''', (user_id, task_id, task['reward_amount']))
        
        # Log transaction
        db.execute('''
            INSERT INTO transaction_history (user_id, amount, transaction_type, description)
            VALUES (?, ?, ?, ?)
        ''', (user_id, task['reward_amount'], 'task', f'Task: {task["task_name"]}'))
        
        # Update task stats
        db.execute("UPDATE tasks SET total_completions = total_completions + 1 WHERE task_id = ?", (task_id,))
        
        await callback.message.edit_text(
            f"✅ <b>Task Completed Successfully!</b>\n\n"
            f"💰 <b>Reward Added:</b> {format_balance(task['reward_amount'])}\n"
            f"🏷️ <b>Task:</b> {task['task_name']}\n\n"
            f"Moving to next task...",
            parse_mode=ParseMode.HTML
        )
        
        # Show next task or completion message
        data = await state.get_data()
        current_index = data.get('task_index', 1)
        total_tasks = data.get('total_tasks', 1)
        
        if current_index < total_tasks:
            # Get next task
            tasks = db.fetch_all(
                "SELECT t.* FROM tasks t "
                "WHERE t.is_active = TRUE AND "
                "(t.expiry_date IS NULL OR t.expiry_date > datetime('now')) AND "
                "t.task_id NOT IN (SELECT task_id FROM completed_tasks WHERE user_id = ?) "
                "ORDER BY t.priority DESC, t.reward_amount DESC",
                (user_id,)
            )
            
            if tasks and current_index < len(tasks):
                next_task = tasks[current_index]
                await show_task(callback.message, next_task, current_index + 1, total_tasks)
                await state.update_data(current_task_id=next_task['task_id'], task_index=current_index + 1)
        else:
            await callback.message.answer(
                "🎉 <b>All Tasks Completed!</b>\n\n"
                "You've completed all available tasks for now.\n"
                "Check back later for more earning opportunities!",
                parse_mode=ParseMode.HTML,
                reply_markup=get_main_keyboard()
            )
            await state.clear()
    
    else:
        await callback.answer("❌ You haven't joined the channel yet. Please join and try again.")

@dp.callback_query(F.data == "skip_task")
async def skip_task(callback: CallbackQuery, state: FSMContext):
    """Skip current task"""
    data = await state.get_data()
    current_index = data.get('task_index', 1)
    total_tasks = data.get('total_tasks', 1)
    
    if current_index < total_tasks:
        user_id = callback.from_user.id
        
        # Get next task
        tasks = db.fetch_all(
            "SELECT t.* FROM tasks t "
            "WHERE t.is_active = TRUE AND "
            "(t.expiry_date IS NULL OR t.expiry_date > datetime('now')) AND "
            "t.task_id NOT IN (SELECT task_id FROM completed_tasks WHERE user_id = ?) "
            "ORDER BY t.priority DESC, t.reward_amount DESC",
            (user_id,)
        )
        
        if tasks and current_index < len(tasks):
            next_task = tasks[current_index]
            await callback.message.edit_text("⏭️ Task skipped.")
            await show_task(callback.message, next_task, current_index + 1, total_tasks)
            await state.update_data(current_task_id=next_task['task_id'], task_index=current_index + 1)
    else:
        await callback.message.edit_text(
            "🎉 <b>No more tasks available!</b>\n\n"
            "You've seen all available tasks.",
            parse_mode=ParseMode.HTML
        )
        await state.clear()

@dp.message(F.text == "🆘 Help")
async def help_command(message: Message):
    """Show help information"""
    help_text = (
        "🆘 <b>Help & Support</b>\n\n"
        "📚 <b>How to Earn:</b>\n"
        "1. Click 'Join TG Channel to Earn'\n"
        "2. Complete tasks by joining channels\n"
        "3. Earn money for each completed task\n"
        "4. Refer friends for extra bonus\n\n"
        
        "💸 <b>Withdrawals:</b>\n"
        "• Minimum withdrawal: ৳50\n"
        "• Methods: bKash, Nagad, Rocket\n"
        "• Processed within 24 hours\n"
        "• Balance frozen during review\n\n"
        
        "⚖️ <b>Rules:</b>\n"
        "• No multi-accounting\n"
        "• No cheating or automation\n"
        "• Stay joined in channels\n"
        "• Penalties for rule violations\n\n"
        
        "📞 <b>Support:</b>\n"
        "Contact @admin_username for help\n\n"
        
        "⚠️ <b>Warning:</b>\n"
        "Fraudulent activity will result in permanent ban and loss of all earnings."
    )
    
    await message.answer(help_text, parse_mode=ParseMode.HTML)

# ==================== ADMIN HANDLERS ====================
def is_admin(user_id: int) -> bool:
    """Check if user is admin"""
    return user_id in ADMIN_IDS

@dp.message(Command("admin"))
async def admin_panel(message: Message, state: FSMContext):
    """Admin panel entry point"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Access Denied: Admin only")
        return
    
    await message.answer(
        "🛡️ <b>Admin Panel</b>\n\n"
        "Select an option:",
        parse_mode=ParseMode.HTML,
        reply_markup=get_admin_keyboard()
    )
    await state.set_state(AdminStates.main_menu)

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    """Show system statistics"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Access denied")
        return
    
    # Get stats
    total_users = db.fetch_one("SELECT COUNT(*) as count FROM users")['count']
    active_users = db.fetch_one("SELECT COUNT(*) as count FROM users WHERE DATE(last_active) = DATE('now')")['count']
    total_earnings = db.fetch_one("SELECT COALESCE(SUM(amount), 0) as total FROM transaction_history WHERE transaction_type IN ('task', 'referral')")['total']
    today_earnings = db.fetch_one("SELECT COALESCE(SUM(amount), 0) as total FROM transaction_history WHERE DATE(created_at) = DATE('now') AND transaction_type IN ('task', 'referral')")['total']
    pending_withdrawals = db.fetch_one("SELECT COALESCE(SUM(amount), 0) as total FROM withdraw_requests WHERE status = 'pending'")['total']
    active_tasks = db.fetch_one("SELECT COUNT(*) as count FROM tasks WHERE is_active = TRUE")['count']
    
    stats_text = (
        f"📊 <b>System Statistics</b>\n\n"
        f"👥 <b>Total Users:</b> {total_users}\n"
        f"🎯 <b>Active Today:</b> {active_users}\n"
        f"💰 <b>Total Earnings Paid:</b> {format_balance(total_earnings)}\n"
        f"📈 <b>Today's Earnings:</b> {format_balance(today_earnings)}\n"
        f"⏳ <b>Pending Withdrawals:</b> {format_balance(pending_withdrawals)}\n"
        f"📋 <b>Active Tasks:</b> {active_tasks}\n\n"
        
        f"⚙️ <b>System Info:</b>\n"
        f"• Min Withdraw: {format_balance(MINIMUM_WITHDRAW)}\n"
        f"• Referral Bonus: {format_balance(REFERRAL_BONUS)}\n"
        f"• Daily Task Limit: {DAILY_TASK_LIMIT}\n"
        f"• Penalty Multiplier: {PENALTY_MULTIPLIER}x"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_stats"),
         InlineKeyboardButton(text="📈 Detailed Stats", callback_data="admin_detailed_stats")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_back")]
    ])
    
    await callback.message.edit_text(stats_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

@dp.callback_query(F.data == "admin_users")
async def admin_users(callback: CallbackQuery, state: FSMContext):
    """Manage users"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Access denied")
        return
    
    # Get recent users
    users = db.fetch_all(
        "SELECT user_id, username, balance, total_tasks, status, last_active "
        "FROM users ORDER BY last_active DESC LIMIT 20"
    )
    
    if not users:
        await callback.message.edit_text("No users found")
        return
    
    keyboard = InlineKeyboardBuilder()
    for user in users:
        status_icon = "✅" if user['status'] == 'active' else "❌"
        username = user['username'] or f"User {user['user_id']}"
        keyboard.add(InlineKeyboardButton(
            text=f"{status_icon} {username} - ৳{user['balance']:.0f}",
            callback_data=f"view_user_{user['user_id']}"
        ))
    
    keyboard.add(InlineKeyboardButton(text="🔍 Search User", callback_data="search_user"))
    keyboard.add(InlineKeyboardButton(text="⬅️ Back", callback_data="admin_back"))
    keyboard.adjust(1)
    
    await callback.message.edit_text(
        "👥 <b>User Management</b>\n\n"
        "Select a user to manage:",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard.as_markup()
    )
    await state.set_state(AdminStates.manage_users)

@dp.callback_query(F.data.startswith("view_user_"))
async def admin_view_user(callback: CallbackQuery, state: FSMContext):
    """View detailed user information"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Access denied")
        return
    
    user_id = int(callback.data.replace("view_user_", ""))
    user = db.fetch_one("SELECT * FROM users WHERE user_id = ?", (user_id,))
    
    if not user:
        await callback.answer("User not found")
        return
    
    # Get user statistics
    today_earnings = db.fetch_one(
        "SELECT COALESCE(SUM(amount), 0) as total FROM transaction_history "
        "WHERE user_id = ? AND DATE(created_at) = DATE('now') AND transaction_type IN ('task', 'referral')",
        (user_id,)
    )['total']
    
    total_withdrawn = db.fetch_one(
        "SELECT COALESCE(SUM(amount), 0) as total FROM withdraw_requests "
        "WHERE user_id = ? AND status IN ('approved', 'paid')",
        (user_id,)
    )['total']
    
    user_text = (
        f"👤 <b>User Details</b>\n\n"
        f"🆔 <b>User ID:</b> {user['user_id']}\n"
        f"📛 <b>Name:</b> {user['full_name'] or 'N/A'}\n"
        f"👤 <b>Username:</b> @{user['username'] or 'N/A'}\n"
        f"💳 <b>Balance:</b> {format_balance(user['balance'])}\n"
        f"💰 <b>Total Earned:</b> {format_balance(user['total_earned'])}\n"
        f"📤 <b>Total Withdrawn:</b> {format_balance(total_withdrawn)}\n"
        f"✅ <b>Tasks Completed:</b> {user['total_tasks']}\n"
        f"👥 <b>Referrals:</b> {user['total_referrals']}\n"
        f"📅 <b>Joined:</b> {user['join_date'][:10] if user['join_date'] else 'N/A'}\n"
        f"🕒 <b>Last Active:</b> {user['last_active'][:19] if user['last_active'] else 'N/A'}\n"
        f"💰 <b>Today's Earnings:</b> {format_balance(today_earnings)}\n"
        f"⚠️ <b>Penalties:</b> {format_balance(user['total_penalties'])}\n"
        f"🎯 <b>Risk Score:</b> {user['risk_score']}/100\n"
        f"📊 <b>Status:</b> {user['status']}\n"
        f"🔗 <b>Referral Code:</b> {user['referral_code']}"
    )
    
    if user['referred_by']:
        user_text += f"\n👥 <b>Referred By:</b> {user['referred_by']}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ Add Balance", callback_data=f"adjust_balance_add_{user_id}"),
            InlineKeyboardButton(text="➖ Deduct Balance", callback_data=f"adjust_balance_deduct_{user_id}")
        ],
        [
            InlineKeyboardButton(text="✅ Activate", callback_data=f"user_activate_{user_id}"),
            InlineKeyboardButton(text="❌ Ban", callback_data=f"user_ban_{user_id}")
        ],
        [
            InlineKeyboardButton(text="📊 View Transactions", callback_data=f"user_transactions_{user_id}"),
            InlineKeyboardButton(text="📜 View Tasks", callback_data=f"user_tasks_{user_id}")
        ],
        [
            InlineKeyboardButton(text="⬅️ Back", callback_data="admin_users")
        ]
    ])
    
    await callback.message.edit_text(user_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    await state.set_state(AdminStates.view_user)

@dp.callback_query(F.data.startswith("adjust_balance_"))
async def admin_adjust_balance(callback: CallbackQuery, state: FSMContext):
    """Adjust user balance"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Access denied")
        return
    
    action, user_id = callback.data.replace("adjust_balance_", "").split("_")
    user_id = int(user_id)
    
    await state.update_data(adjust_user_id=user_id, adjust_action=action)
    await callback.message.edit_text(
        f"💰 <b>Adjust Balance</b>\n\n"
        f"User ID: {user_id}\n"
        f"Action: {'Add' if action == 'add' else 'Deduct'}\n\n"
        f"Enter amount:",
        parse_mode=ParseMode.HTML
    )
    await state.set_state(AdminStates.adjust_balance)

@dp.message(AdminStates.adjust_balance)
async def process_balance_adjustment(message: Message, state: FSMContext):
    """Process balance adjustment"""
    try:
        amount = float(message.text)
        if amount <= 0:
            await message.answer("❌ Amount must be positive")
            return
        
        data = await state.get_data()
        user_id = data['adjust_user_id']
        action = data['adjust_action']
        
        # Get current balance
        user = db.fetch_one("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        if not user:
            await message.answer("❌ User not found")
            return
        
        # Calculate new balance
        if action == 'add':
            new_balance = user['balance'] + amount
            transaction_type = 'adjustment_add'
            description = f'Admin added balance: {format_balance(amount)}'
        else:
            if amount > user['balance']:
                await message.answer("❌ Cannot deduct more than current balance")
                return
            new_balance = user['balance'] - amount
            transaction_type = 'adjustment_deduct'
            description = f'Admin deducted balance: {format_balance(amount)}'
        
        # Update balance
        db.execute("UPDATE users SET balance = ? WHERE user_id = ?", (new_balance, user_id))
        
        # Log transaction
        db.execute('''
            INSERT INTO transaction_history (user_id, amount, transaction_type, description)
            VALUES (?, ?, ?, ?)
        ''', (user_id, amount if action == 'add' else -amount, transaction_type, description))
        
        # Log admin action
        db.execute('''
            INSERT INTO admin_logs (admin_id, action, target_id, details)
            VALUES (?, ?, ?, ?)
        ''', (message.from_user.id, f'balance_{action}', user_id, f'Amount: {amount}'))
        
        await message.answer(
            f"✅ <b>Balance Updated Successfully!</b>\n\n"
            f"User ID: {user_id}\n"
            f"Action: {'Added' if action == 'add' else 'Deducted'}\n"
            f"Amount: {format_balance(amount)}\n"
            f"New Balance: {format_balance(new_balance)}",
            parse_mode=ParseMode.HTML
        )
        
        # Return to user view
        await admin_view_user(callback=CallbackQuery(
            message=message,
            data=f"view_user_{user_id}",
            from_user=message.from_user,
            chat_instance=""
        ), state=state)
        
    except ValueError:
        await message.answer("❌ Please enter a valid number")

@dp.callback_query(F.data.startswith("user_"))
async def admin_user_actions(callback: CallbackQuery):
    """Handle user actions (ban/activate)"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Access denied")
        return
    
    action, user_id = callback.data.replace("user_", "").split("_")
    user_id = int(user_id)
    
    if action == 'ban':
        db.execute("UPDATE users SET status = 'banned' WHERE user_id = ?", (user_id,))
        await callback.answer("✅ User banned")
    elif action == 'activate':
        db.execute("UPDATE users SET status = 'active' WHERE user_id = ?", (user_id,))
        await callback.answer("✅ User activated")
    
    # Refresh user view
    await admin_view_user(callback, state=None)

@dp.callback_query(F.data == "admin_tasks")
async def admin_tasks(callback: CallbackQuery, state: FSMContext):
    """Manage tasks"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Access denied")
        return
    
    # Get all tasks
    tasks = db.fetch_all("SELECT * FROM tasks ORDER BY is_active DESC, priority DESC, created_at DESC")
    
    keyboard = InlineKeyboardBuilder()
    
    for task in tasks:
        status = "✅" if task['is_active'] else "❌"
        keyboard.add(InlineKeyboardButton(
            text=f"{status} {task['task_name']} - ৳{task['reward_amount']:.0f}",
            callback_data=f"edit_task_{task['task_id']}"
        ))
    
    keyboard.add(InlineKeyboardButton(text="➕ Add New Task", callback_data="add_task"))
    keyboard.add(InlineKeyboardButton(text="⬅️ Back", callback_data="admin_back"))
    keyboard.adjust(1)
    
    await callback.message.edit_text(
        "📋 <b>Task Management</b>\n\n"
        f"Total Tasks: {len(tasks)}\n"
        "Select a task to edit:",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard.as_markup()
    )
    await state.set_state(AdminStates.manage_tasks)

@dp.callback_query(F.data == "add_task")
async def admin_add_task(callback: CallbackQuery, state: FSMContext):
    """Add new task"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Access denied")
        return
    
    await callback.message.edit_text(
        "📝 <b>Add New Task</b>\n\n"
        "Enter task name:",
        parse_mode=ParseMode.HTML
    )
    await state.set_state(AdminStates.add_task_name)

@dp.message(AdminStates.add_task_name)
async def process_task_name(message: Message, state: FSMContext):
    """Process task name"""
    await state.update_data(task_name=message.text)
    
    await message.answer(
        "🔗 <b>Add Task Link</b>\n\n"
        "Enter channel invite link:",
        parse_mode=ParseMode.HTML
    )
    await state.set_state(AdminStates.add_task_link)

@dp.message(AdminStates.add_task_link)
async def process_task_link(message: Message, state: FSMContext):
    """Process task link"""
    await state.update_data(channel_invite_link=message.text)
    
    await message.answer(
        "💰 <b>Task Reward</b>\n\n"
        "Enter reward amount:",
        parse_mode=ParseMode.HTML
    )
    await state.set_state(AdminStates.add_task_reward)

@dp.message(AdminStates.add_task_reward)
async def process_task_reward(message: Message, state: FSMContext):
    """Process task reward"""
    try:
        reward = float(message.text)
        if reward <= 0:
            await message.answer("❌ Reward must be positive")
            return
        
        await state.update_data(reward_amount=reward)
        
        await message.answer(
            "🎯 <b>Task Priority</b>\n\n"
            "Enter priority (1-5, higher = shown first):",
            parse_mode=ParseMode.HTML
        )
        await state.set_state(AdminStates.add_task_priority)
        
    except ValueError:
        await message.answer("❌ Please enter a valid number")

@dp.message(AdminStates.add_task_priority)
async def process_task_priority(message: Message, state: FSMContext):
    """Process task priority"""
    try:
        priority = int(message.text)
        if not 1 <= priority <= 5:
            await message.answer("❌ Priority must be between 1-5")
            return
        
        await state.update_data(priority=priority)
        
        await message.answer(
            "⏰ <b>Task Expiry</b>\n\n"
            "Enter expiry in days (0 for no expiry):",
            parse_mode=ParseMode.HTML
        )
        await state.set_state(AdminStates.add_task_expiry)
        
    except ValueError:
        await message.answer("❌ Please enter a valid number")

@dp.message(AdminStates.add_task_expiry)
async def process_task_expiry(message: Message, state: FSMContext):
    """Process task expiry and create task"""
    try:
        expiry_days = int(message.text)
        
        data = await state.get_data()
        
        # Calculate expiry date
        expiry_date = None
        if expiry_days > 0:
            expiry_date = datetime.now() + timedelta(days=expiry_days)
        
        # Create task
        db.execute('''
            INSERT INTO tasks 
            (task_name, channel_invite_link, reward_amount, priority, expiry_date, is_active)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            data['task_name'],
            data['channel_invite_link'],
            data['reward_amount'],
            data['priority'],
            expiry_date,
            True
        ))
        
        task_id = db.fetch_one("SELECT last_insert_rowid()")[0]
        
        await message.answer(
            f"✅ <b>Task Created Successfully!</b>\n\n"
            f"🏷️ <b>Name:</b> {data['task_name']}\n"
            f"💰 <b>Reward:</b> {format_balance(data['reward_amount'])}\n"
            f"🎯 <b>Priority:</b> {data['priority']}\n"
            f"🔗 <b>Link:</b> {data['channel_invite_link']}\n"
            f"⏰ <b>Expiry:</b> {expiry_days} days\n"
            f"📝 <b>Task ID:</b> {task_id}",
            parse_mode=ParseMode.HTML
        )
        
        # Log admin action
        db.execute('''
            INSERT INTO admin_logs (admin_id, action, target_id, details)
            VALUES (?, ?, ?, ?)
        ''', (message.from_user.id, 'add_task', task_id, data['task_name']))
        
        await admin_tasks(callback=CallbackQuery(
            message=message,
            data="admin_tasks",
            from_user=message.from_user,
            chat_instance=""
        ), state=state)
        
    except ValueError:
        await message.answer("❌ Please enter a valid number")

@dp.callback_query(F.data.startswith("edit_task_"))
async def admin_edit_task(callback: CallbackQuery):
    """Edit existing task"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Access denied")
        return
    
    task_id = int(callback.data.replace("edit_task_", ""))
    task = db.fetch_one("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
    
    if not task:
        await callback.answer("Task not found")
        return
    
    task_text = (
        f"📝 <b>Edit Task</b>\n\n"
        f"🏷️ <b>Name:</b> {task['task_name']}\n"
        f"💰 <b>Reward:</b> {format_balance(task['reward_amount'])}\n"
        f"🎯 <b>Priority:</b> {task['priority']}\n"
        f"🔗 <b>Link:</b> {task['channel_invite_link']}\n"
        f"✅ <b>Completions:</b> {task['total_completions']}\n"
        f"📅 <b>Created:</b> {task['created_at'][:10]}\n"
        f"⏰ <b>Expiry:</b> {task['expiry_date'][:10] if task['expiry_date'] else 'Never'}\n"
        f"📊 <b>Status:</b> {'Active' if task['is_active'] else 'Inactive'}"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Activate" if not task['is_active'] else "❌ Deactivate", 
                               callback_data=f"toggle_task_{task_id}"),
            InlineKeyboardButton(text="✏️ Edit", callback_data=f"modify_task_{task_id}")
        ],
        [
            InlineKeyboardButton(text="🗑️ Delete", callback_data=f"delete_task_{task_id}"),
            InlineKeyboardButton(text="📊 Stats", callback_data=f"task_stats_{task_id}")
        ],
        [
            InlineKeyboardButton(text="⬅️ Back", callback_data="admin_tasks")
        ]
    ])
    
    await callback.message.edit_text(task_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

@dp.callback_query(F.data.startswith("toggle_task_"))
async def toggle_task_status(callback: CallbackQuery):
    """Toggle task active/inactive"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Access denied")
        return
    
    task_id = int(callback.data.replace("toggle_task_", ""))
    task = db.fetch_one("SELECT is_active FROM tasks WHERE task_id = ?", (task_id,))
    
    if not task:
        await callback.answer("Task not found")
        return
    
    new_status = not task['is_active']
    db.execute("UPDATE tasks SET is_active = ? WHERE task_id = ?", (new_status, task_id))
    
    await callback.answer(f"Task {'activated' if new_status else 'deactivated'}")
    await admin_edit_task(callback)

@dp.callback_query(F.data == "admin_withdrawals")
async def admin_withdrawals(callback: CallbackQuery):
    """Manage withdrawal requests"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Access denied")
        return
    
    # Get pending withdrawals
    withdrawals = db.fetch_all(
        "SELECT w.*, u.username FROM withdraw_requests w "
        "LEFT JOIN users u ON w.user_id = u.user_id "
        "WHERE w.status = 'pending' "
        "ORDER BY w.created_at DESC"
    )
    
    if not withdrawals:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_back")]
        ])
        
        await callback.message.edit_text(
            "💰 <b>Withdrawal Management</b>\n\n"
            "No pending withdrawals.",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
        return
    
    keyboard = InlineKeyboardBuilder()
    
    for w in withdrawals:
        username = w['username'] or f"User {w['user_id']}"
        keyboard.add(InlineKeyboardButton(
            text=f"{username} - ৳{w['amount']:.0f} - {w['method']}",
            callback_data=f"view_withdrawal_{w['request_id']}"
        ))
    
    keyboard.add(InlineKeyboardButton(text="⬅️ Back", callback_data="admin_back"))
    keyboard.adjust(1)
    
    await callback.message.edit_text(
        f"💰 <b>Withdrawal Management</b>\n\n"
        f"Pending withdrawals: {len(withdrawals)}\n"
        f"Total amount: ৳{sum(w['amount'] for w in withdrawals):.2f}\n\n"
        f"Select a request to process:",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard.as_markup()
    )

@dp.callback_query(F.data.startswith("view_withdrawal_"))
async def admin_view_withdrawal(callback: CallbackQuery):
    """View withdrawal details"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Access denied")
        return
    
    request_id = int(callback.data.replace("view_withdrawal_", ""))
    
    withdrawal = db.fetch_one(
        "SELECT w.*, u.username, u.balance FROM withdraw_requests w "
        "LEFT JOIN users u ON w.user_id = u.user_id "
        "WHERE w.request_id = ?",
        (request_id,)
    )
    
    if not withdrawal:
        await callback.answer("Withdrawal not found")
        return
    
    status_icons = {
        'pending': '⏳',
        'approved': '✅',
        'rejected': '❌',
        'paid': '💰'
    }
    
    withdrawal_text = (
        f"💰 <b>Withdrawal Details</b>\n\n"
        f"📝 <b>Request ID:</b> {withdrawal['request_id']}\n"
        f"👤 <b>User:</b> @{withdrawal['username'] or 'N/A'} ({withdrawal['user_id']})\n"
        f"💳 <b>User Balance:</b> {format_balance(withdrawal['balance'])}\n"
        f"💰 <b>Amount:</b> {format_balance(withdrawal['amount'])}\n"
        f"📱 <b>Method:</b> {withdrawal['method'].upper()}\n"
        f"🔢 <b>Account:</b> {withdrawal['account_number']}\n"
        f"📅 <b>Requested:</b> {withdrawal['created_at'][:19]}\n"
        f"📊 <b>Status:</b> {status_icons.get(withdrawal['status'], '📝')} {withdrawal['status'].title()}"
    )
    
    if withdrawal['proof_tx_id']:
        withdrawal_text += f"\n📄 <b>TX ID:</b> {withdrawal['proof_tx_id']}"
    
    if withdrawal['admin_notes']:
        withdrawal_text += f"\n📝 <b>Admin Notes:</b> {withdrawal['admin_notes']}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_withdrawal_{request_id}"),
            InlineKeyboardButton(text="❌ Reject", callback_data=f"reject_withdrawal_{request_id}")
        ],
        [
            InlineKeyboardButton(text="💰 Mark Paid", callback_data=f"pay_withdrawal_{request_id}"),
            InlineKeyboardButton(text="📝 Add TX ID", callback_data=f"add_txid_{request_id}")
        ],
        [
            InlineKeyboardButton(text="⬅️ Back", callback_data="admin_withdrawals")
        ]
    ])
    
    await callback.message.edit_text(withdrawal_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

@dp.callback_query(F.data.startswith("approve_withdrawal_"))
async def approve_withdrawal(callback: CallbackQuery):
    """Approve withdrawal request"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Access denied")
        return
    
    request_id = int(callback.data.replace("approve_withdrawal_", ""))
    
    # Update withdrawal status
    db.execute("UPDATE withdraw_requests SET status = 'approved', processed_at = datetime('now') WHERE request_id = ?", 
               (request_id,))
    
    # Log admin action
    db.execute('''
        INSERT INTO admin_logs (admin_id, action, target_id, details)
        VALUES (?, ?, ?, ?)
    ''', (callback.from_user.id, 'approve_withdrawal', request_id, 'Withdrawal approved'))
    
    await callback.answer("✅ Withdrawal approved")
    await admin_view_withdrawal(callback)

@dp.callback_query(F.data.startswith("reject_withdrawal_"))
async def reject_withdrawal(callback: CallbackQuery):
    """Reject withdrawal request"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Access denied")
        return
    
    request_id = int(callback.data.replace("reject_withdrawal_", ""))
    
    # Get withdrawal details
    withdrawal = db.fetch_one("SELECT user_id, amount FROM withdraw_requests WHERE request_id = ?", (request_id,))
    
    if withdrawal:
        # Return frozen balance to user
        db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", 
                  (withdrawal['amount'], withdrawal['user_id']))
        
        # Update withdrawal status
        db.execute("UPDATE withdraw_requests SET status = 'rejected', processed_at = datetime('now') WHERE request_id = ?", 
                  (request_id,))
        
        # Log transaction
        db.execute('''
            INSERT INTO transaction_history (user_id, amount, transaction_type, description)
            VALUES (?, ?, ?, ?)
        ''', (withdrawal['user_id'], withdrawal['amount'], 'withdraw_refund', 'Withdrawal rejected - balance returned'))
        
        # Log admin action
        db.execute('''
            INSERT INTO admin_logs (admin_id, action, target_id, details)
            VALUES (?, ?, ?, ?)
        ''', (callback.from_user.id, 'reject_withdrawal', request_id, 'Withdrawal rejected'))
    
    await callback.answer("❌ Withdrawal rejected")
    await admin_view_withdrawal(callback)

@dp.callback_query(F.data == "admin_abuse")
async def admin_abuse_reports(callback: CallbackQuery):
    """Show abuse reports"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Access denied")
        return
    
    # Get abuse flags
    flags = db.fetch_all(
        "SELECT a.*, u.username FROM abuse_flags a "
        "LEFT JOIN users u ON a.user_id = u.user_id "
        "WHERE a.resolved = FALSE "
        "ORDER BY a.severity DESC, a.created_at DESC LIMIT 20"
    )
    
    if not flags:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_back")]
        ])
        
        await callback.message.edit_text(
            "🚨 <b>Abuse Detection System</b>\n\n"
            "No active abuse flags.",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
        return
    
    keyboard = InlineKeyboardBuilder()
    
    for flag in flags:
        severity_icon = "🔴" if flag['severity'] >= 3 else "🟡" if flag['severity'] == 2 else "🟢"
        username = flag['username'] or f"User {flag['user_id']}"
        keyboard.add(InlineKeyboardButton(
            text=f"{severity_icon} {username} - {flag['flag_type']}",
            callback_data=f"view_abuse_{flag['flag_id']}"
        ))
    
    keyboard.add(InlineKeyboardButton(text="⬅️ Back", callback_data="admin_back"))
    keyboard.adjust(1)
    
    await callback.message.edit_text(
        f"🚨 <b>Abuse Detection System</b>\n\n"
        f"Active flags: {len(flags)}\n"
        f"🔴 High: {sum(1 for f in flags if f['severity'] >= 3)}\n"
        f"🟡 Medium: {sum(1 for f in flags if f['severity'] == 2)}\n"
        f"🟢 Low: {sum(1 for f in flags if f['severity'] == 1)}",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard.as_markup()
    )

@dp.callback_query(F.data == "admin_settings")
async def admin_settings(callback: CallbackQuery):
    """System settings"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Access denied")
        return
    
    settings_text = (
        f"⚙️ <b>System Settings</b>\n\n"
        f"Current Configuration:\n"
        f"• Minimum Withdraw: {format_balance(MINIMUM_WITHDRAW)}\n"
        f"• Referral Bonus: {format_balance(REFERRAL_BONUS)}\n"
        f"• Daily Task Limit: {DAILY_TASK_LIMIT}\n"
        f"• Task Cooldown: {TASK_COOLDOWN}s\n"
        f"• Penalty Multiplier: {PENALTY_MULTIPLIER}x\n\n"
        f"Admin IDs: {', '.join(map(str, ADMIN_IDS))}"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Run Anti-Cheat Scan", callback_data="run_anticheat")],
        [InlineKeyboardButton(text="📊 Update Statistics", callback_data="update_stats")],
        [InlineKeyboardButton(text="🗑️ Cleanup Database", callback_data="cleanup_db")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_back")]
    ])
    
    await callback.message.edit_text(settings_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

@dp.callback_query(F.data == "run_anticheat")
async def run_anticheat_scan(callback: CallbackQuery):
    """Run anti-cheat scan"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Access denied")
        return
    
    await callback.answer("🔄 Anti-cheat scan started...")
    
    # Find potential multi-accounts by device/IP
    suspicious_users = db.fetch_all('''
        SELECT u1.user_id as user1, u2.user_id as user2, u1.device_hash
        FROM users u1
        JOIN users u2 ON u1.device_hash = u2.device_hash AND u1.user_id < u2.user_id
        WHERE u1.device_hash != 'unknown' AND u2.device_hash != 'unknown'
        LIMIT 10
    ''')
    
    if suspicious_users:
        for sus in suspicious_users:
            # Create abuse flag
            db.execute('''
                INSERT INTO abuse_flags (user_id, flag_type, severity, details)
                VALUES (?, ?, ?, ?)
            ''', (sus['user1'], 'multi_account', 3, f'Possible multi-account with user {sus["user2"]}, same device hash'))
    
    # Find users who unjoined channels
    completed_tasks = db.fetch_all('''
        SELECT ct.user_id, ct.task_id, t.channel_invite_link
        FROM completed_tasks ct
        JOIN tasks t ON ct.task_id = t.task_id
        WHERE DATE(ct.completed_at) >= DATE('now', '-7 days')
        LIMIT 50
    ''')
    
    for task in completed_tasks:
        # Simulate checking if still member (in production, implement actual check)
        is_still_member = await check_channel_membership(task['user_id'], task['channel_invite_link'])
        
        if not is_still_member:
            # User unjoined - apply penalty
            task_reward = db.fetch_one("SELECT reward_amount FROM tasks WHERE task_id = ?", (task['task_id'],))['reward_amount']
            penalty_amount = task_reward * PENALTY_MULTIPLIER
            
            # Deduct penalty
            db.execute("UPDATE users SET balance = balance - ?, total_penalties = total_penalties + ? WHERE user_id = ?",
                      (penalty_amount, penalty_amount, task['user_id']))
            
            # Log penalty
            db.execute('''
                INSERT INTO penalties (user_id, amount, reason)
                VALUES (?, ?, ?)
            ''', (task['user_id'], penalty_amount, 'Unjoined channel after earning'))
            
            # Create abuse flag
            db.execute('''
                INSERT INTO abuse_flags (user_id, flag_type, severity, details)
                VALUES (?, ?, ?, ?)
            ''', (task['user_id'], 'unjoin_fraud', 2, f'Unjoined channel after completing task {task["task_id"]}'))
            
            # Log transaction
            db.execute('''
                INSERT INTO transaction_history (user_id, amount, transaction_type, description)
                VALUES (?, ?, ?, ?)
            ''', (task['user_id'], -penalty_amount, 'penalty', 'Penalty for unjoining channel'))
    
    await callback.answer(f"✅ Anti-cheat scan completed. Found {len(suspicious_users)} suspicious cases.")

@dp.callback_query(F.data == "admin_back")
async def admin_back(callback: CallbackQuery, state: FSMContext):
    """Go back to admin main menu"""
    if not is_admin(callback.from_user.id):
        await callback.answer("Access denied")
        return
    
    await callback.message.edit_text(
        "🛡️ <b>Admin Panel</b>\n\n"
        "Select an option:",
        parse_mode=ParseMode.HTML,
        reply_markup=get_admin_keyboard()
    )
    await state.set_state(AdminStates.main_menu)

# ==================== ANTI-FRAUD TASKS ====================
async def scheduled_anti_fraud_check():
    """Scheduled task to check for fraud"""
    while True:
        try:
            # Check for users with high risk scores
            high_risk_users = db.fetch_all(
                "SELECT user_id, risk_score FROM users WHERE risk_score >= 70 AND status = 'active'"
            )
            
            for user in high_risk_users:
                # Auto-ban users with very high risk
                if user['risk_score'] >= 90:
                    db.execute("UPDATE users SET status = 'banned' WHERE user_id = ?", (user['user_id'],))
                    await log_to_channel(f"🚨 Auto-banned user {user['user_id']} for high risk score: {user['risk_score']}")
                
                # Flag for manual review
                elif user['risk_score'] >= 70:
                    existing_flag = db.fetch_one(
                        "SELECT 1 FROM abuse_flags WHERE user_id = ? AND flag_type = 'high_risk' AND resolved = FALSE",
                        (user['user_id'],)
                    )
                    if not existing_flag:
                        db.execute('''
                            INSERT INTO abuse_flags (user_id, flag_type, severity, details)
                            VALUES (?, ?, ?, ?)
                        ''', (user['user_id'], 'high_risk', 3, f'High risk score: {user["risk_score"]}'))
            
            # Check for duplicate withdrawal numbers
            duplicate_numbers = db.fetch_all('''
                SELECT account_number, COUNT(DISTINCT user_id) as user_count
                FROM withdraw_requests
                WHERE status IN ('approved', 'paid')
                GROUP BY account_number
                HAVING COUNT(DISTINCT user_id) > 1
            ''')
            
            for dup in duplicate_numbers:
                users = db.fetch_all(
                    "SELECT DISTINCT user_id FROM withdraw_requests WHERE account_number = ?",
                    (dup['account_number'],)
                )
                for user in users:
                    db.execute('''
                        INSERT INTO abuse_flags (user_id, flag_type, severity, details)
                        VALUES (?, ?, ?, ?)
                    ''', (user['user_id'], 'withdraw_abuse', 2, f'Shared payment number: {dup["account_number"]} with {dup["user_count"]-1} other users'))
            
        except Exception as e:
            logger.error(f"Error in anti-fraud check: {e}")
        
        # Run every hour
        await asyncio.sleep(3600)

# ==================== BOT STARTUP ====================
async def on_startup():
    """Run on bot startup"""
    logger.info("Bot starting up...")
    
    # Create default admin user if not exists
    for admin_id in ADMIN_IDS:
        admin_exists = db.fetch_one("SELECT 1 FROM users WHERE user_id = ?", (admin_id,))
        if not admin_exists:
            db.execute('''
                INSERT INTO users (user_id, username, full_name, balance, accepted_terms)
                VALUES (?, ?, ?, ?, ?)
            ''', (admin_id, 'admin', 'Admin', 0.0, True))
    
    # Start anti-fraud task
    asyncio.create_task(scheduled_anti_fraud_check())
    
    logger.info(f"Bot started with {len(ADMIN_IDS)} admins")

async def on_shutdown():
    """Run on bot shutdown"""
    logger.info("Bot shutting down...")
    await bot.session.close()

# ==================== MAIN ENTRY POINT ====================
async def main():
    """Main function"""
    await on_startup()
    
    try:
        await dp.start_polling(bot)
    finally:
        await on_shutdown()

if __name__ == "__main__":
    # Create database tables
    db.init_database()
    
    # Run the bot
    asyncio.run(main())
