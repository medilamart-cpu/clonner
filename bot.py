import os
import sys
import re
import asyncio
import logging
import random
from typing import Optional, Dict, Any
from dotenv import load_dotenv, set_key

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
        sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        pass


from telethon import TelegramClient, events, errors, Button, utils
from telethon.sessions import StringSession
from telethon.tl import types

from database import Database
from cloner_core import (
    resolve_peer,
    get_message_topic_id,
    sync_forum_topics,
    ensure_topic_for_message,
    send_single_message_fast,
    send_album_fast,
    forward_batch_messages,
    cleanup_temp_dir,
    ensure_temp_dir,
    extract_topic_id_from_updates
)
from telethon.tl.functions.messages import GetForumTopicsRequest, CreateForumTopicRequest


# Logging
logging.basicConfig(
    format="[%(asctime)s] %(levelname)s: %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("TGBotController")

async def safe_edit_status(msg, text: str):
    """Safely edits a status message, catching FloodWait and EditMessageRequest errors without crashing."""
    if not msg:
        return
    try:
        await msg.edit(text)
    except errors.FloodWaitError as fwe:
        logger.warning(f"Telegram FloodWait on status edit ({fwe.seconds}s). Skipping UI update.")
    except Exception as ex:
        logger.debug(f"Status edit ignored: {ex}")

CONFIG_PATH = "config.env"
load_dotenv(CONFIG_PATH)

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID_STR = os.getenv("OWNER_ID")
OWNER_ID = int(OWNER_ID_STR) if OWNER_ID_STR and OWNER_ID_STR.strip().isdigit() else None
DELAY_BETWEEN_MESSAGES = float(os.getenv("DELAY_BETWEEN_MESSAGES", "0.05"))

# Conversation States
STATE_IDLE = "IDLE"
STATE_WAITING_PHONE = "WAITING_PHONE"
STATE_WAITING_OTP = "WAITING_OTP"
STATE_WAITING_2FA = "WAITING_2FA"
STATE_WAITING_SOURCE = "WAITING_SOURCE"
STATE_WAITING_DEST = "WAITING_DEST"
STATE_WAITING_RESUME_DECISION = "WAITING_RESUME_DECISION"
STATE_WAITING_MODE = "WAITING_MODE"
STATE_WAITING_RANGE = "WAITING_RANGE"
STATE_WAITING_RANGE_FORCE = "WAITING_RANGE_FORCE"
STATE_WAITING_CAPTION_REMOVE = "WAITING_CAPTION_REMOVE"
STATE_WAITING_CAPTION_REPLACE = "WAITING_CAPTION_REPLACE"
STATE_WAITING_CAPTION_SUFFIX = "WAITING_CAPTION_SUFFIX"
STATE_WAITING_SYNC_SOURCE = "WAITING_SYNC_SOURCE"
STATE_WAITING_SYNC_SOURCE_RANGE = "WAITING_SYNC_SOURCE_RANGE"
STATE_WAITING_SYNC_DEST = "WAITING_SYNC_DEST"
STATE_WAITING_SYNC_DEST_RANGE = "WAITING_SYNC_DEST_RANGE"
STATE_WAITING_SETUP_NAME = "WAITING_SETUP_NAME"
STATE_WAITING_SETUP_SOURCE = "WAITING_SETUP_SOURCE"
STATE_WAITING_SETUP_DEST = "WAITING_SETUP_DEST"

users_data: Dict[int, Dict[str, Any]] = {}
active_user_clients: Dict[int, TelegramClient] = {}

def get_user_data(user_id: int) -> Dict[str, Any]:
    if user_id not in users_data:
        users_data[user_id] = {
            "state": STATE_IDLE,
            "phone": None,
            "phone_code_hash": None,
            "source_peer": None,
            "source_entity": None,
            "dest_peer": None,
            "dest_entity": None,
            "clone_task": None,
            "live_handler": None,
            "is_running": False,
            "range_start_id": None,
            "range_limit": None,
            "range_end_id": None,
            "force_reclone": False,
            "sync_source_peer": None,
            "sync_source_entity": None,
            "sync_source_start": None,
            "sync_source_end": None,
            "sync_dest_peer": None,
            "sync_dest_entity": None,
            "sync_dest_start": None,
            "sync_dest_end": None,
            "new_setup_name": None,
            "new_setup_source_peer": None,
            "new_setup_source_entity": None,
            "stats": {"cloned": 0, "skipped": 0, "current_topic": "None"}
        }
    return users_data[user_id]

async def get_user_client(user_id: int) -> TelegramClient:
    global active_user_clients
    if user_id in active_user_clients:
        client = active_user_clients[user_id]
        if not client.is_connected():
            try:
                await client.connect()
            except Exception:
                pass
        return client

    # Check if user has saved session in database
    session_str = db.get_user_session(user_id)
    if session_str:
        client = TelegramClient(StringSession(session_str), int(API_ID), API_HASH)
        await client.connect()
        active_user_clients[user_id] = client
        return client

    # If owner and no separate session in db, use default master user_client
    if is_owner(user_id):
        if not user_client.is_connected():
            try:
                await user_client.connect()
            except Exception:
                pass
        return user_client

    # For authorized user without saved session, provide ephemeral client for login flow
    client = TelegramClient(StringSession(""), int(API_ID), API_HASH)
    await client.connect()
    active_user_clients[user_id] = client
    return client

db = Database()

def save_env_var(key: str, value: str):
    try:
        set_key(CONFIG_PATH, key, str(value))
    except Exception:
        pass

def is_owner(sender_id: int) -> bool:
    global OWNER_ID
    if OWNER_ID is None:
        OWNER_ID = sender_id
        save_env_var("OWNER_ID", str(sender_id))
        logger.info(f"Automatically bound Owner ID: {sender_id}")
        return True
    return sender_id == OWNER_ID

def is_authorized(sender_id: int) -> bool:
    if is_owner(sender_id):
        return True
    return db.is_user_authorized(sender_id)

# Initialize clients
if not API_ID or not API_HASH:
    print("ERROR: API_ID and API_HASH must be present in config.env!")
    exit(1)

bot = TelegramClient("bot_session", int(API_ID), API_HASH)
user_client = TelegramClient("user_session", int(API_ID), API_HASH)


# --------------------- BOT COMMAND HANDLERS ---------------------

@bot.on(events.NewMessage(pattern=r"^/start$"))
async def start_handler(event):
    if not is_authorized(event.sender_id):
        return await event.reply(
            f"⛔ **Access Restricted**\n\n"
            f"You are not authorized to use this bot.\n"
            f"Your Telegram User ID: `{event.sender_id}`\n\n"
            f"Send your User ID to the bot Owner to request access."
        )

    sender_is_owner = is_owner(event.sender_id)
    u_client = await get_user_client(event.sender_id)
    is_connected = False
    acc_name = "Not Logged In"
    try:
        if await u_client.is_user_authorized():
            is_connected = True
            me = await u_client.get_me()
            acc_name = f"{me.first_name} (@{me.username or me.id})"
    except Exception:
        pass

    status_icon = "🟢" if is_connected else "🔴"

    if sender_is_owner:
        welcome_text = (
            f"👑 **Owner Control Panel — Telegram Cloner Bot**\n\n"
            f"**Your Account**: {status_icon} `{acc_name}`\n\n"
            f"**Available Commands**:\n"
            f"📱 `/login` — Connect your Telegram account\n"
            f"⚙️ `/setup` — Saved channel profile & 1-click quick start\n"
            f"🚀 `/clone` — Start cloning a channel or forum group\n"
            f"🎯 `/extract` — Extract specific message range (e.g. 100 from #4)\n"
            f"📝 `/synccaption` — Sync/fix captions for existing videos (0 data download)\n"
            f"🔧 `/settings` — Caption rules (remove/replace words, clean mode)\n"
            f"📊 `/status` — View current cloning progress\n"
            f"💾 `/storage` — View VPS disk space, RAM, CPU & temp files\n"
            f"👥 `/users` — View authorized users\n"
            f"➕ `/add_user` — Authorize a new user (Owner only)\n"
            f"➖ `/remove_user` — Revoke user access (Owner only)\n"
            f"🗑️ `/reset` — Clear cloned history database to start fresh\n"
            f"⏹️ `/stop` — Stop current active cloning\n"
            f"🚪 `/logout` — Disconnect your user session\n"
            f"❓ `/help` — How to use this bot"
        )
    else:
        status_line = f"**Your Account**: {status_icon} `{acc_name}`" if is_connected else "**Your Account**: 🔴 Not Logged In\n👉 *Send /login to connect your Telegram account and clone your channels!*"
        welcome_text = (
            f"🤖 **Telegram Cloner Bot**\n\n"
            f"{status_line}\n\n"
            f"**Available Commands**:\n"
            f"📱 `/login` — Log in your Telegram account to access your channels\n"
            f"⚙️ `/setup` — Saved channel profile & 1-click quick start\n"
            f"🚀 `/clone` — Start cloning a channel or forum group\n"
            f"🎯 `/extract` — Extract specific message range (e.g. 100 from #4)\n"
            f"📝 `/synccaption` — Sync/fix captions for existing videos (0 data download)\n"
            f"🔧 `/settings` — Caption rules (remove/replace words, clean mode)\n"
            f"📊 `/status` — View live cloning progress & remaining messages\n"
            f"💾 `/storage` — View VPS disk space & system stats\n"
            f"⏹️ `/stop` — Stop current active cloning\n"
            f"🚪 `/logout` — Log out your Telegram session\n"
            f"❓ `/help` — How to use this bot"
        )
    await event.reply(welcome_text)


@bot.on(events.NewMessage(pattern=r"^/help$"))
async def help_handler(event):
    if not is_authorized(event.sender_id):
        return

    if is_owner(event.sender_id):
        help_text = (
            "📖 **How to Use This Bot (Owner)**:\n\n"
            "1️⃣ **Login**: Send `/login` and enter your phone number.\n"
            "2️⃣ **Enter OTP**: When Telegram sends code, enter with spaces (`1 2 3 4 5`).\n"
            "3️⃣ **Clone / Extract**: Send `/clone` or `/extract <start_id> <count>`.\n"
            "4️⃣ **Monitor**: Send `/status` or `/storage` at any time.\n"
            "5️⃣ **Users**: Send `/add_user <user_id>` or `/users` to manage permissions."
        )
    else:
        help_text = (
            "📖 **How to Use This Bot**:\n\n"
            "1️⃣ **Clone Channel**: Send `/clone` and follow the steps to paste source and destination links.\n"
            "2️⃣ **Extract Specific Range**: Send `/extract <start_id> <count>` (e.g. `/extract 4 100` to clone 100 messages from #4).\n"
            "3️⃣ **Check Progress**: Send `/status` to see current speed, topic, and message counts.\n"
            "4️⃣ **Stop**: Send `/stop` to stop the current transfer."
        )
    await event.reply(help_text)


@bot.on(events.NewMessage(pattern=r"^/users$"))
async def list_users_handler(event):
    if not is_authorized(event.sender_id):
        return await event.reply("⛔ Unauthorized.")

    users = db.get_authorized_users()
    owner_str = f"• `{OWNER_ID}` *(Bot Owner)*"

    if not users:
        users_list = "• _No additional users authorized yet._"
    else:
        lines = []
        for u in users:
            uid, added_by, uname, created_at = u
            u_title = f"@{uname}" if uname else f"User `{uid}`"
            lines.append(f"• `{uid}` — {u_title}")
        users_list = "\n".join(lines)

    msg = (
        f"👥 **Authorized Users Directory**\n\n"
        f"👑 **Owner**:\n{owner_str}\n\n"
        f"🔓 **Authorized Users** ({len(users)}):\n{users_list}\n\n"
        f"💡 Owner can use `/add_user <id>` or `/remove_user <id>`."
    )
    await event.reply(msg)


@bot.on(events.NewMessage(pattern=r"^/(add_user|allow)(?:\s+(.*))?$"))
async def add_user_handler(event):
    if not is_owner(event.sender_id):
        return await event.reply("⛔ Only the bot Owner can authorize new users.")

    args = (event.pattern_match.group(2) or "").strip()
    target_id = None
    target_username = None

    if event.is_reply:
        reply_msg = await event.get_reply_message()
        if reply_msg and reply_msg.sender_id:
            target_id = reply_msg.sender_id
            try:
                sender = await reply_msg.get_sender()
                target_username = getattr(sender, "username", None)
            except Exception:
                pass

    if not target_id and args:
        cleaned = args.replace("@", "").strip()
        if cleaned.isdigit():
            target_id = int(cleaned)
        else:
            try:
                entity = await bot.get_entity(args)
                target_id = entity.id
                target_username = getattr(entity, "username", None)
            except Exception as e:
                return await event.reply(f"❌ Could not resolve user `{args}`: {e}")

    if not target_id:
        return await event.reply(
            "ℹ️ **How to Authorize a User**:\n\n"
            "• Reply to their message with `/add_user`\n"
            "• Or send: `/add_user <user_id>` (e.g. `/add_user 123456789`)\n"
            "• Or send: `/add_user @username`"
        )

    db.add_authorized_user(target_id, added_by=event.sender_id, username=target_username)
    u_display = f"@{target_username}" if target_username else f"User ID `{target_id}`"
    await event.reply(
        f"✅ **User Authorized!**\n\n"
        f"Successfully granted access to {u_display} (`{target_id}`).\n"
        f"They can now use `/start`, `/clone`, `/status`, and `/storage`."
    )


@bot.on(events.NewMessage(pattern=r"^/(remove_user|revoke|del_user)(?:\s+(.*))?$"))
async def remove_user_handler(event):
    if not is_owner(event.sender_id):
        return await event.reply("⛔ Only the bot Owner can revoke user access.")

    args = (event.pattern_match.group(2) or "").strip()
    target_id = None

    if event.is_reply:
        reply_msg = await event.get_reply_message()
        if reply_msg and reply_msg.sender_id:
            target_id = reply_msg.sender_id

    if not target_id and args:
        cleaned = args.replace("@", "").strip()
        if cleaned.isdigit():
            target_id = int(cleaned)
        else:
            try:
                entity = await bot.get_entity(args)
                target_id = entity.id
            except Exception as e:
                return await event.reply(f"❌ Could not resolve user `{args}`: {e}")

    if not target_id:
        return await event.reply(
            "ℹ️ **How to Revoke User Access**:\n\n"
            "• Reply to their message with `/remove_user`\n"
            "• Or send: `/remove_user <user_id>`"
        )

    if target_id == OWNER_ID:
        return await event.reply("❌ You cannot revoke access from the bot Owner.")

    removed = db.remove_authorized_user(target_id)
    if removed:
        await event.reply(f"🗑️ **User Removed!**\nAccess revoked for user ID `{target_id}`.")
    else:
        await event.reply(f"⚠️ User ID `{target_id}` was not in the authorized list.")


def get_temp_dir_size() -> int:
    ensure_temp_dir()
    total_size = 0
    try:
        from cloner_core import TEMP_DIR
        if os.path.exists(TEMP_DIR):
            for dirpath, _, filenames in os.walk(TEMP_DIR):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    if not os.path.islink(fp) and os.path.exists(fp):
                        total_size += os.path.getsize(fp)
    except Exception:
        pass
    return total_size


def make_progress_bar(percent: float, length: int = 10) -> str:
    filled = int(round(percent / (100.0 / length)))
    filled = max(0, min(length, filled))
    return "█" * filled + "░" * (length - filled)


@bot.on(events.NewMessage(pattern=r"^/(storage|disk)$"))
async def storage_cmd_handler(event):
    if not is_authorized(event.sender_id):
        return

    import psutil
    import time

    disk = psutil.disk_usage("/")
    disk_total_gb = disk.total / (1024 ** 3)
    disk_used_gb = disk.used / (1024 ** 3)
    disk_free_gb = disk.free / (1024 ** 3)
    disk_percent = disk.percent
    disk_bar = make_progress_bar(disk_percent)

    mem = psutil.virtual_memory()
    mem_total_gb = mem.total / (1024 ** 3)
    mem_used_gb = mem.used / (1024 ** 3)
    mem_percent = mem.percent
    mem_bar = make_progress_bar(mem_percent)

    cpu_percent = psutil.cpu_percent(interval=0.2)

    temp_bytes = get_temp_dir_size()
    if temp_bytes >= 1024 ** 3:
        temp_str = f"{temp_bytes / (1024 ** 3):.2f} GB"
    elif temp_bytes >= 1024 ** 2:
        temp_str = f"{temp_bytes / (1024 ** 2):.1f} MB"
    else:
        temp_str = f"{temp_bytes / 1024:.1f} KB"

    boot_time = psutil.boot_time()
    uptime_sec = int(time.time() - boot_time)
    days = uptime_sec // 86400
    hours = (uptime_sec % 86400) // 3600
    minutes = (uptime_sec % 3600) // 60
    uptime_str = f"{days}d {hours}h {minutes}m" if days > 0 else f"{hours}h {minutes}m"

    cloned_in_db = db.get_cloned_count()

    msg_text = (
        f"🖥️ **VPS Storage & System Dashboard**\n\n"
        f"💾 **Disk Space (`/`):**\n"
        f"• **Free:** `{disk_free_gb:.2f} GB` / `{disk_total_gb:.2f} GB`\n"
        f"• **Used:** `{disk_used_gb:.2f} GB` (`{disk_percent:.1f}%`)\n"
        f"[{disk_bar}] {disk_percent:.1f}%\n\n"
        f"📁 **Bot Temp Cache:** `{temp_str}`\n\n"
        f"🧠 **Memory (RAM):**\n"
        f"• **Used:** `{mem_used_gb:.2f} GB` / `{mem_total_gb:.2f} GB` (`{mem_percent:.1f}%`)\n"
        f"[{mem_bar}] {mem_percent:.1f}%\n\n"
        f"⚡ **CPU Load:** `{cpu_percent:.1f}%`\n"
        f"⏱️ **VPS Uptime:** `{uptime_str}`\n"
        f"🗃️ **Recorded Cloned Items:** `{cloned_in_db}`"
    )

    buttons = [
        [Button.inline("🧹 Wipe Temp Cache", data=b"clean_temp")],
        [Button.inline("🔄 Refresh Stats", data=b"refresh_storage")]
    ]
    await event.reply(msg_text, buttons=buttons)


@bot.on(events.CallbackQuery(data=re.compile(b"^(clean_temp|refresh_storage)$")))
async def storage_callback_handler(event):
    if not is_authorized(event.sender_id):
        return await event.answer("⛔ Unauthorized", alert=True)

    data = event.data.decode("utf-8")
    if data == "clean_temp":
        cleanup_temp_dir()
        await event.answer("🧹 Temp download cache wiped successfully!", alert=True)
    elif data == "refresh_storage":
        await event.answer("🔄 Refreshing...")

    import psutil
    import time

    disk = psutil.disk_usage("/")
    disk_total_gb = disk.total / (1024 ** 3)
    disk_used_gb = disk.used / (1024 ** 3)
    disk_free_gb = disk.free / (1024 ** 3)
    disk_percent = disk.percent
    disk_bar = make_progress_bar(disk_percent)

    mem = psutil.virtual_memory()
    mem_total_gb = mem.total / (1024 ** 3)
    mem_used_gb = mem.used / (1024 ** 3)
    mem_percent = mem.percent
    mem_bar = make_progress_bar(mem_percent)

    cpu_percent = psutil.cpu_percent(interval=0.1)

    temp_bytes = get_temp_dir_size()
    if temp_bytes >= 1024 ** 3:
        temp_str = f"{temp_bytes / (1024 ** 3):.2f} GB"
    elif temp_bytes >= 1024 ** 2:
        temp_str = f"{temp_bytes / (1024 ** 2):.1f} MB"
    else:
        temp_str = f"{temp_bytes / 1024:.1f} KB"

    boot_time = psutil.boot_time()
    uptime_sec = int(time.time() - boot_time)
    days = uptime_sec // 86400
    hours = (uptime_sec % 86400) // 3600
    minutes = (uptime_sec % 3600) // 60
    uptime_str = f"{days}d {hours}h {minutes}m" if days > 0 else f"{hours}h {minutes}m"

    cloned_in_db = db.get_cloned_count()

    msg_text = (
        f"🖥️ **VPS Storage & System Dashboard**\n\n"
        f"💾 **Disk Space (`/`):**\n"
        f"• **Free:** `{disk_free_gb:.2f} GB` / `{disk_total_gb:.2f} GB`\n"
        f"• **Used:** `{disk_used_gb:.2f} GB` (`{disk_percent:.1f}%`)\n"
        f"[{disk_bar}] {disk_percent:.1f}%\n\n"
        f"📁 **Bot Temp Cache:** `{temp_str}`\n\n"
        f"🧠 **Memory (RAM):**\n"
        f"• **Used:** `{mem_used_gb:.2f} GB` / `{mem_total_gb:.2f} GB` (`{mem_percent:.1f}%`)\n"
        f"[{mem_bar}] {mem_percent:.1f}%\n\n"
        f"⚡ **CPU Load:** `{cpu_percent:.1f}%`\n"
        f"⏱️ **VPS Uptime:** `{uptime_str}`\n"
        f"🗃️ **Recorded Cloned Items:** `{cloned_in_db}`"
    )

    buttons = [
        [Button.inline("🧹 Wipe Temp Cache", data=b"clean_temp")],
        [Button.inline("🔄 Refresh Stats", data=b"refresh_storage")]
    ]
    try:
        await event.edit(msg_text, buttons=buttons)
    except Exception:
        pass


def make_caption_settings_text() -> str:
    rules = db.get_caption_rules()
    is_strip = db.is_strip_all_captions()
    strip_status = "🟢 **ON** (Captions stripped completely)" if is_strip else "🔴 **OFF** (Captions kept & modified)"

    rules_summary = ""
    if not rules:
        rules_summary = "• _No word modification rules added yet._\n"
    else:
        lines = []
        for r in rules:
            rid, rtype, pat, rep, _ = r
            if rtype == "remove":
                lines.append(f"• `[#{rid}]` ✂️ **Remove**: `{pat}`")
            elif rtype == "replace":
                lines.append(f"• `[#{rid}]` 🔄 **Replace**: `{pat}` ➡️ `{rep}`")
            elif rtype == "prefix":
                lines.append(f"• `[#{rid}]` 🏷️ **Header/Prefix**: `{pat}`")
            elif rtype == "suffix":
                lines.append(f"• `[#{rid}]` 🏷️ **Footer/Suffix**: `{pat}`")
        rules_summary = "\n".join(lines) + "\n"

    return (
        f"⚙️ **Caption & Forwarding Settings**\n\n"
        f"Customize how video captions are formatted when forwarded:\n\n"
        f"🚫 **Clean Mode (Strip All Captions)**: {strip_status}\n\n"
        f"📝 **Active Modification Rules** ({len(rules)}):\n"
        f"{rules_summary}\n"
        f"💡 Choose an action below to add or manage rules:"
    )

def make_caption_settings_buttons():
    is_strip = db.is_strip_all_captions()
    strip_btn_text = "🚫 Clean Mode: ON (Strip All)" if is_strip else "🚫 Clean Mode: OFF (Keep)"
    return [
        [Button.inline("✂️ Remove Word/Link", data=b"cap_add_remove"), Button.inline("🔄 Replace Word/Text", data=b"cap_add_replace")],
        [Button.inline("🏷️ Set Footer/Suffix", data=b"cap_add_suffix"), Button.inline(strip_btn_text, data=b"cap_toggle_strip")],
        [Button.inline("📋 Manage / Delete Rules", data=b"cap_manage_rules"), Button.inline("🗑️ Clear All Rules", data=b"cap_clear_all")],
        [Button.inline("🔄 Refresh", data=b"cap_refresh"), Button.inline("🔙 Close", data=b"cap_close")]
    ]

@bot.on(events.NewMessage(pattern=r"^/(settings|caption|captions)$"))
async def caption_settings_cmd_handler(event):
    if not is_authorized(event.sender_id):
        return await event.reply("⛔ Unauthorized.")

    text = make_caption_settings_text()
    buttons = make_caption_settings_buttons()
    await event.reply(text, buttons=buttons)


@bot.on(events.CallbackQuery(data=re.compile(b"^cap_(.*)$")))
async def caption_callback_handler(event):
    if not is_authorized(event.sender_id):
        return await event.answer("⛔ Unauthorized", alert=True)

    action = event.data.decode("utf-8").replace("cap_", "")
    u_data = get_user_data(event.sender_id)

    if action == "refresh":
        text = make_caption_settings_text()
        buttons = make_caption_settings_buttons()
        try:
            await event.edit(text, buttons=buttons)
        except Exception:
            pass
        await event.answer("🔄 Refreshed!")

    elif action == "close":
        try:
            await event.delete()
        except Exception:
            pass

    elif action == "toggle_strip":
        current = db.is_strip_all_captions()
        db.set_strip_all_captions(not current)
        status = "ON (All captions will be removed)" if not current else "OFF (Captions preserved)"
        await event.answer(f"Clean Mode is now {status}", alert=True)
        text = make_caption_settings_text()
        buttons = make_caption_settings_buttons()
        try:
            await event.edit(text, buttons=buttons)
        except Exception:
            pass

    elif action == "clear_all":
        db.clear_caption_rules()
        await event.answer("🗑️ All caption rules cleared!", alert=True)
        text = make_caption_settings_text()
        buttons = make_caption_settings_buttons()
        try:
            await event.edit(text, buttons=buttons)
        except Exception:
            pass

    elif action == "add_remove":
        u_data["state"] = STATE_WAITING_CAPTION_REMOVE
        await event.reply(
            "✂️ **Remove Word / Link from Captions**\n\n"
            "Please send the exact word, username, link, or phrase you want deleted from captions:\n\n"
            "👉 Example: `@spam_channel` or `t.me/oldgroup` or `Downloaded from XYZ`"
        )
        await event.answer()

    elif action == "add_replace":
        u_data["state"] = STATE_WAITING_CAPTION_REPLACE
        await event.reply(
            "🔄 **Replace Word / Text in Captions**\n\n"
            "Please send the text to find and its replacement in this format:\n"
            "`<old_text> => <new_text>`\n\n"
            "👉 Example: `@old_channel => @my_new_channel`\n"
            "👉 Example: `Course 2024 => Course 2025`"
        )
        await event.answer()

    elif action == "add_suffix":
        u_data["state"] = STATE_WAITING_CAPTION_SUFFIX
        await event.reply(
            "🏷️ **Set Custom Footer / Suffix**\n\n"
            "Please send the custom text/credit to append to the end of every video caption:\n\n"
            "👉 Example: `Join: @my_awesome_channel`"
        )
        await event.answer()

    elif action == "manage_rules":
        rules = db.get_caption_rules()
        if not rules:
            return await event.answer("ℹ️ No rules exist to delete.", alert=True)

        buttons = []
        for r in rules:
            rid, rtype, pat, rep, _ = r
            btn_label = f"❌ Delete #{rid} ({rtype}: {pat[:15]})"
            buttons.append([Button.inline(btn_label, data=f"cap_del_{rid}".encode())])
        buttons.append([Button.inline("🔙 Back to Settings", data=b"cap_refresh")])

        await event.edit("📋 **Select a Rule to Delete**:", buttons=buttons)
        await event.answer()

    elif action.startswith("del_"):
        rule_id_str = action.replace("del_", "")
        if rule_id_str.isdigit():
            db.remove_caption_rule(int(rule_id_str))
            await event.answer(f"🗑️ Deleted Rule #{rule_id_str}!", alert=True)
            text = make_caption_settings_text()
            buttons = make_caption_settings_buttons()
            try:
                await event.edit(text, buttons=buttons)
            except Exception:
                pass


async def safe_edit(msg, event, text: str, buttons=None):
    try:
        return await msg.edit(text, buttons=buttons)
    except errors.FloodWaitError as fe:
        logger.warning(f"FloodWait on edit ({fe.seconds}s), sending new reply instead...")
        try:
            return await event.reply(text, buttons=buttons)
        except Exception:
            pass
    except Exception:
        try:
            return await event.reply(text, buttons=buttons)
        except Exception:
            pass


@bot.on(events.NewMessage(pattern=r"^/login$"))
async def login_cmd_handler(event):
    global user_client
    if not is_authorized(event.sender_id):
        return await event.reply("⛔ Unauthorized.")

    u_client = await get_user_client(event.sender_id)
    u_data = get_user_data(event.sender_id)
    try:
        if await u_client.is_user_authorized():
            me = await u_client.get_me()
            return await event.reply(
                f"✅ You are already logged in as **{me.first_name}** (@{me.username or me.id})!\n\n"
                f"Use `/clone` or `/extract` to start cloning channels.\n"
                f"Or send `/logout` to switch accounts."
            )
    except Exception as e:
        logger.warning(f"Session not authorized or invalidated: {e}")
        if is_owner(event.sender_id):
            try:
                await user_client.disconnect()
            except Exception:
                pass
            if os.path.exists("user_session.session"):
                try:
                    os.remove("user_session.session")
                except Exception:
                    pass
            user_client = TelegramClient("user_session", int(API_ID), API_HASH)

    # Reset any ephemeral unauthenticated client to start fresh
    if event.sender_id in active_user_clients and not db.get_user_session(event.sender_id):
        try:
            await active_user_clients[event.sender_id].disconnect()
        except Exception:
            pass
        del active_user_clients[event.sender_id]

    await get_user_client(event.sender_id)
    u_data["state"] = STATE_WAITING_PHONE
    await event.reply(
        "📱 **Connect Your Telegram Account**\n\n"
        "Please send your **phone number** including country code:\n"
        "👉 Example: `+919876543210` or `+12345678900`\n\n"
        "🔒 *Your session will be used exclusively by your account to access and clone your channels.*"
    )


@bot.on(events.NewMessage(pattern=r"^/logout$"))
async def logout_cmd_handler(event):
    global user_client
    if not is_authorized(event.sender_id):
        return
    u_data = get_user_data(event.sender_id)
    try:
        db.delete_user_session(event.sender_id)
        if event.sender_id in active_user_clients:
            try:
                await active_user_clients[event.sender_id].log_out()
            except Exception:
                try:
                    await active_user_clients[event.sender_id].disconnect()
                except Exception:
                    pass
            del active_user_clients[event.sender_id]

        if is_owner(event.sender_id):
            try:
                await user_client.log_out()
            except Exception:
                pass
            try:
                await user_client.disconnect()
            except Exception:
                pass
            if os.path.exists("user_session.session"):
                try:
                    os.remove("user_session.session")
                except Exception:
                    pass
            user_client = TelegramClient("user_session", int(API_ID), API_HASH)

        u_data["state"] = STATE_IDLE
        await event.reply("🚪 **Successfully logged out.** Your Telegram session has been removed from the bot.")
    except Exception as e:
        await event.reply(f"❌ Error during logout: {e}")


@bot.on(events.NewMessage(pattern=r"^/stop$"))
async def stop_cmd_handler(event):
    if not is_authorized(event.sender_id):
        return
    u_data = get_user_data(event.sender_id)
    task = u_data.get("clone_task")
    if task and not task.done():
        task.cancel()
        u_data["clone_task"] = None
        u_data["is_running"] = False
        await event.reply("⏹️ Active cloning task has been cancelled.")
    else:
        await event.reply("ℹ️ No active cloning task is currently running.")


@bot.on(events.NewMessage(pattern=r"^/reset$"))
async def reset_cmd_handler(event):
    if not is_owner(event.sender_id):
        return await event.reply("⛔ Only the bot Owner can reset the database history.")
    count = db.get_cloned_count()
    db.clear_cloned_messages()
    await event.reply(
        f"🗑️ **Clone History Cleared!**\n\n"
        f"Wiped `{count}` recorded messages from the database.\n"
        f"Your next `/clone` will start completely fresh from message #1!"
    )


@bot.on(events.NewMessage(pattern=r"^/status$"))
async def status_cmd_handler(event):
    if not is_authorized(event.sender_id):
        return
    u_data = get_user_data(event.sender_id)
    is_running = u_data.get("is_running", False)
    stats = u_data.get("stats", {})
    stage = stats.get("stage", "Idle")
    source_title = getattr(u_data.get("source_entity"), "title", "None")
    dest_title = getattr(u_data.get("dest_entity"), "title", "None")

    if not is_running:
        status_header = "⚪ **Idle / Stopped**"
    elif stage == "live":
        status_header = "🟢 **Live Mirroring Active** *(History 100% Completed!)*"
    else:
        status_header = "📦 **History Cloning in Progress...**"

    total_target = stats.get("total_target")
    cloned = stats.get("cloned", 0)
    skipped = stats.get("skipped", 0)

    progress_line = f"• **Messages Cloned**: `{cloned}`\n"
    if total_target:
        remaining = max(0, total_target - cloned)
        pct = min(100.0, (cloned / total_target) * 100)
        bar = make_progress_bar(pct)
        progress_line = (
            f"• **Progress**: `{cloned}` / `{total_target}` messages (`{pct:.1f}%`)\n"
            f"[{bar}] `{pct:.1f}%`\n"
            f"• **Remaining**: `{remaining}` messages left\n"
        )

    status_text = (
        f"📊 **Cloner Status**\n\n"
        f"• **Status**: {status_header}\n"
        f"• **Source**: `{source_title}`\n"
        f"• **Destination**: `{dest_title}`\n"
        f"• **Current Topic**: `{stats.get('current_topic', 'None')}`\n"
        f"{progress_line}"
        f"• **Messages Skipped**: `{skipped}`\n"
    )
    await event.reply(status_text)


def make_setups_view(user_id: int):
    setups = db.get_user_setups(user_id)
    if not setups:
        text = (
            "⚙️ **Your Saved Channel Profiles**\n\n"
            "You don't have any saved channel setups yet.\n"
            "Tap **Create New Setup** below to create your first channel profile!"
        )
        buttons = [
            [Button.inline("➕ Create New Setup", data=b"setup_new")]
        ]
        return text, buttons

    text = f"⚙️ **Your Channel Setup Profiles** (Total: `{len(setups)}`):\n\n"
    buttons = []
    for s in setups:
        sid = s["id"]
        s_name = s["setup_name"]
        text += (
            f"🏷️ **#{sid}: {s_name}**\n"
            f"• 📥 **Source**: `{s['source_title']}`\n"
            f"• 📤 **Dest**: `{s['dest_title']}`\n\n"
        )
        buttons.append([Button.inline(f"▶️ Use #{sid}: {s_name[:16]}", data=f"setup_sel_{sid}".encode())])

    buttons.append([
        Button.inline("➕ Create New Setup", data=b"setup_new"),
        Button.inline("🗑️ Delete a Setup", data=b"setup_dellist")
    ])
    return text, buttons


@bot.on(events.NewMessage(pattern=r"^/(setup|resume|config|setups)$"))
async def setup_cmd_handler(event):
    if not is_authorized(event.sender_id):
        return await event.reply("⛔ Unauthorized.")

    u_client = await get_user_client(event.sender_id)
    if not await u_client.is_user_authorized():
        return await event.reply("⚠️ You must log in first! Send `/login` to connect your Telegram account.")

    text, buttons = make_setups_view(event.sender_id)
    await event.reply(text, buttons=buttons)


@bot.on(events.CallbackQuery(data=re.compile(b"^setup_(.*)$")))
async def setup_callback_handler(event):
    if not is_authorized(event.sender_id):
        return await event.answer("⛔ Unauthorized.", alert=True)

    action = event.pattern_match.group(1).decode("utf-8")
    u_client = await get_user_client(event.sender_id)
    u_data = get_user_data(event.sender_id)

    if action == "back":
        text, buttons = make_setups_view(event.sender_id)
        return await event.edit(text, buttons=buttons)

    if action == "new":
        u_data["state"] = STATE_WAITING_SETUP_NAME
        await event.delete()
        return await bot.send_message(
            event.chat_id,
            "🏷️ **Create New Channel Setup (Step 1/3)**\n\n"
            "Please enter a **Name** for this setup profile:\n"
            "👉 Example: `OBGY Course`, `Pediatrics`, or `Channel A to B`"
        )

    if action == "dellist":
        setups = db.get_user_setups(event.sender_id)
        if not setups:
            return await event.answer("ℹ️ No setups to delete.", alert=True)
        buttons = []
        for s in setups:
            sid = s["id"]
            name = s["setup_name"]
            buttons.append([Button.inline(f"❌ Delete #{sid}: {name[:18]}", data=f"setup_dodel_{sid}".encode())])
        buttons.append([Button.inline("🔙 Back", data=b"setup_back")])
        return await event.edit("📋 **Select a Setup Profile to Delete**:", buttons=buttons)

    if action.startswith("dodel_"):
        sid_str = action.replace("dodel_", "")
        if sid_str.isdigit():
            db.delete_user_setup(int(sid_str), event.sender_id)
            await event.answer(f"🗑️ Deleted Setup #{sid_str}!", alert=True)
            text, buttons = make_setups_view(event.sender_id)
            return await event.edit(text, buttons=buttons)

    if action.startswith("sel_"):
        sid_str = action.replace("sel_", "")
        if not sid_str.isdigit():
            return
        setup = db.get_user_setup_by_id(int(sid_str), event.sender_id)
        if not setup:
            return await event.answer("❌ Setup not found.", alert=True)

        text = (
            f"⚙️ **Selected Profile**: **{setup['setup_name']}**\n\n"
            f"• 📥 **Source**: `{setup['source_title']}`\n"
            f"  `{setup['source_peer']}`\n\n"
            f"• 📤 **Destination**: `{setup['dest_title']}`\n"
            f"  `{setup['dest_peer']}`\n\n"
            f"Choose an action to proceed with this profile:"
        )
        buttons = [
            [
                Button.inline("🚀 Quick Clone (Full)", data=f"setup_run_clone_{setup['id']}".encode()),
                Button.inline("🎯 Extract Range", data=f"setup_run_extract_{setup['id']}".encode())
            ],
            [
                Button.inline("📝 Sync Captions (0 Data)", data=f"setup_run_sync_{setup['id']}".encode()),
                Button.inline("❌ Delete Profile", data=f"setup_dodel_{setup['id']}".encode())
            ],
            [
                Button.inline("🔙 Back to All Setups", data=b"setup_back")
            ]
        ]
        return await event.edit(text, buttons=buttons)

    if action.startswith("run_"):
        parts = action.split("_")
        mode = parts[1]  # clone, extract, sync
        setup_id = int(parts[2])
        setup = db.get_user_setup_by_id(setup_id, event.sender_id)
        if not setup:
            return await event.answer("❌ Setup not found.", alert=True)

        # Pre-resolve channels
        s_peer = setup["source_peer"]
        d_peer = setup["dest_peer"]
        try:
            s_entity = await resolve_peer(u_client, s_peer)
            d_entity = await resolve_peer(u_client, d_peer)
            u_data["source_peer"] = s_peer
            u_data["source_entity"] = s_entity
            u_data["dest_peer"] = d_peer
            u_data["dest_entity"] = d_entity
            u_data["sync_source_peer"] = s_peer
            u_data["sync_source_entity"] = s_entity
            u_data["sync_dest_peer"] = d_peer
            u_data["sync_dest_entity"] = d_entity
        except Exception as e:
            return await event.answer(f"❌ Failed to resolve channels: {e}", alert=True)

        await event.delete()

        if mode == "clone":
            source_chat_id = utils.get_peer_id(s_entity) if s_entity else None
            count = db.get_cloned_count(source_chat_id)
            if count > 0:
                u_data["state"] = STATE_WAITING_RESUME_DECISION
                await bot.send_message(
                    event.chat_id,
                    f"📋 **Resume Cloning with Profile '{setup['setup_name']}'?**\n\n"
                    f"Found `{count}` previously cloned messages for `{setup['source_title']}`.\n\n"
                    f"**1** ➡️ **Resume** (Skip already cloned messages)\n"
                    f"**2** ➡️ **Restart Fresh** (Wipe database records for this channel & re-clone from beginning)\n\n"
                    f"Reply with `1` or `2`:"
                )
            else:
                u_data["state"] = STATE_WAITING_MODE
                await bot.send_message(
                    event.chat_id,
                    f"⚙️ **Select Cloning Mode for '{setup['setup_name']}'**:\n\n"
                    f"**1** ➡️ **Clone History + Live Mirror**\n"
                    f"**2** ➡️ **Clone History Only**\n"
                    f"**3** ➡️ **Live Mirror Only**\n"
                    f"**5** ➡️ **Subject-Header Forum Clone** 📚 *(Auto-sorts lectures into subject topics)*\n\n"
                    f"Reply with `1`, `2`, `3`, or `5`:"
                )

        elif mode == "extract":
            u_data["state"] = STATE_WAITING_RANGE
            await bot.send_message(
                event.chat_id,
                f"🎯 **Extract Selective Message Range** ({setup['setup_name']})\n\n"
                f"• Source: `{getattr(s_entity, 'title', s_peer)}`\n"
                f"• Destination: `{getattr(d_entity, 'title', d_peer)}`\n\n"
                f"Enter starting message ID and number of messages:\n"
                f"👉 Example: `4 100` *(100 messages starting from #4)* or `4-100`:"
            )

        elif mode == "sync":
            u_data["state"] = STATE_WAITING_SYNC_SOURCE_RANGE
            await bot.send_message(
                event.chat_id,
                f"📝 **Sync Captions (Zero-Data)** ({setup['setup_name']})\n\n"
                f"• Source: `{getattr(s_entity, 'title', s_peer)}`\n"
                f"• Destination: `{getattr(d_entity, 'title', d_peer)}`\n\n"
                f"👉 Enter the **Source Message Range** (e.g. `1-100`):"
            )


@bot.on(events.NewMessage(pattern=r"^/clone$"))
async def clone_cmd_handler(event):
    if not is_authorized(event.sender_id):
        return await event.reply(
            f"⛔ **Access Restricted**\n\n"
            f"You are not authorized to use this bot.\n"
            f"Your Telegram User ID: `{event.sender_id}`"
        )

    u_client = await get_user_client(event.sender_id)
    u_data = get_user_data(event.sender_id)

    if not await u_client.is_user_authorized():
        return await event.reply("⚠️ You must log in first! Send `/login` to connect your Telegram account.")

    if u_data.get("is_running", False):
        return await event.reply("⚠️ A clone job is already running! Send `/status` or `/stop` first.")

    setup = db.get_user_setup(event.sender_id)
    setup_hint = f"\n\n💡 *Tip: You already have a saved setup profile! Send `/setup` to resume instantly without entering channels.*" if setup else ""

    u_data["range_start_id"] = None
    u_data["range_limit"] = None
    u_data["range_end_id"] = None
    u_data["force_reclone"] = False
    u_data["state"] = STATE_WAITING_SOURCE
    await event.reply(
        f"📥 **Step 1/3: Source Chat**{setup_hint}\n\n"
        "Please send the **Source Channel or Forum**:\n"
        "• Public @username: `@channel_name`\n"
        "• Invite link: `https://t.me/+AbCdEf...`\n"
        "• Chat ID: `-1001234567890`"
    )


@bot.on(events.NewMessage(pattern=r"^/(synccaption|sync_captions|sync_caption|fixcaption|fix_captions)$"))
async def sync_caption_cmd_handler(event):
    if not is_authorized(event.sender_id):
        return await event.reply("⛔ Unauthorized.")

    u_client = await get_user_client(event.sender_id)
    u_data = get_user_data(event.sender_id)

    if not await u_client.is_user_authorized():
        return await event.reply("⚠️ You must log in first! Send `/login` to connect your Telegram account.")

    if u_data.get("is_running", False):
        return await event.reply("⚠️ Another cloning or syncing task is already running! Send `/status` or `/stop` first.")

    u_data["state"] = STATE_WAITING_SYNC_SOURCE
    await event.reply(
        "📝 **Caption Sync & Fix Tool (Zero Video Download)**\n\n"
        "This tool matches videos from your source channel and copies/edits their captions into existing videos in your destination channel without downloading or re-uploading any files.\n\n"
        "✨ *Any removal or replacement rules configured in `/settings` will automatically be applied to the synced captions!*\n\n"
        "👉 **Step 1/4**: Please send the **Source Channel ID or Link** (the channel that HAS the original captions):\n"
        "• Private Channel ID: `-1001234567890`\n"
        "• Invite Link: `https://t.me/+xxxxxx`"
    )


@bot.on(events.NewMessage(pattern=r"^/(extract|range)(?:\s+(.*))?$"))
async def extract_cmd_handler(event):
    if not is_authorized(event.sender_id):
        return await event.reply(
            f"⛔ **Access Restricted**\n\n"
            f"You are not authorized to use this bot.\n"
            f"Your Telegram User ID: `{event.sender_id}`"
        )

    u_client = await get_user_client(event.sender_id)
    u_data = get_user_data(event.sender_id)

    if not await u_client.is_user_authorized():
        return await event.reply("⚠️ You must log in first! Send `/login` to connect your Telegram account.")

    if u_data.get("is_running", False):
        return await event.reply("⚠️ A clone/extract job is already running! Send `/status` or `/stop` first.")

    raw_args = (event.pattern_match.group(2) or "").strip()
    start_id = None
    limit = None
    end_id = None

    if raw_args:
        match_dash = re.match(r"^(\d+)\s*[-:]\s*(\d+)$", raw_args)
        match_space = re.match(r"^(\d+)\s+(\d+)$", raw_args)
        if match_dash:
            start_id = int(match_dash.group(1))
            end_id = int(match_dash.group(2))
            if end_id < start_id:
                start_id, end_id = end_id, start_id
            limit = end_id - start_id + 1
        elif match_space:
            start_id = int(match_space.group(1))
            limit = int(match_space.group(2))
            end_id = start_id + limit - 1
        elif raw_args.isdigit():
            start_id = int(raw_args)
            limit = 100
            end_id = start_id + 99

    u_data["range_start_id"] = start_id
    u_data["range_limit"] = limit
    u_data["range_end_id"] = end_id
    u_data["force_reclone"] = False

    # If source & destination entities are already set from previous clone
    if u_data.get("source_entity") and u_data.get("dest_entity"):
        s_title = getattr(u_data['source_entity'], 'title', 'Source')
        d_title = getattr(u_data['dest_entity'], 'title', 'Destination')
        if start_id is not None:
            u_data["state"] = STATE_WAITING_RANGE_FORCE
            return await event.reply(
                f"🎯 **Target Range Configured**:\n\n"
                f"• **Source**: `{s_title}`\n"
                f"• **Destination**: `{d_title}`\n"
                f"• **Start Message ID**: `#{start_id}`\n"
                f"• **Target Limit**: `{limit}` messages (IDs `{start_id}` to `#{end_id}`)\n\n"
                f"How should previous records in database be handled for these messages?\n"
                f"**1** ➡️ **Skip Duplicates** (Skip messages already cloned)\n"
                f"**2** ➡️ **Force Re-clone** (Re-clone all {limit} messages)\n\n"
                f"Reply with `1` or `2`:"
            )
        else:
            u_data["state"] = STATE_WAITING_RANGE
            return await event.reply(
                f"🎯 **Extract Selective Messages**\n\n"
                f"• **Current Source**: `{s_title}`\n"
                f"• **Current Destination**: `{d_title}`\n\n"
                f"Enter the starting message ID and the number of messages to extract:\n"
                f"• Example: `4 100` *(extracts 100 messages starting from message #4)*\n"
                f"• Or: `4-100` *(extracts messages with IDs from 4 to 100)*\n\n"
                f"👉 Send your message range:"
            )

    u_data["state"] = STATE_WAITING_SOURCE
    range_hint = f" *(Target: {limit} messages starting from #{start_id})*" if start_id else ""
    await event.reply(
        f"📥 **Step 1/3: Source Chat for Extraction**{range_hint}\n\n"
        f"Please send the **Source Channel or Forum**:\n"
        f"• Public @username: `@channel_name`\n"
        f"• Invite link: `https://t.me/+AbCdEf...`\n"
        f"• Chat ID: `-1001234567890`"
    )


# --------------------- CONVERSATION STATE MACHINE ---------------------

@bot.on(events.NewMessage())
async def message_dispatcher(event):
    if not is_authorized(event.sender_id):
        return

    text = event.raw_text.strip()
    if text.startswith("/"):
        return  # Handled by command handlers above

    user_data = get_user_data(event.sender_id)
    user_client = await get_user_client(event.sender_id)
    state = user_data.get("state", STATE_IDLE)

    # 1. Waiting for Phone Number
    if state == STATE_WAITING_PHONE:
        phone = re.sub(r'[\s\-()]', '', text)
        if not phone.startswith("+") or not phone[1:].isdigit():
            return await event.reply("❌ Invalid format. Please include `+` and country code, e.g. `+919876543210`.")

        user_data["phone"] = phone
        msg = await event.reply("⏳ Sending login code to your Telegram app...")
        try:
            # Always use a fresh StringSession client for login so session.save() works reliably
            login_client = TelegramClient(StringSession(""), int(API_ID), API_HASH)
            await login_client.connect()
            try:
                res = await login_client.send_code_request(phone)
            except errors.AuthKeyDuplicatedError:
                logger.warning("AuthKeyDuplicatedError on send_code_request! Retrying with fresh session...")
                await login_client.disconnect()
                login_client = TelegramClient(StringSession(""), int(API_ID), API_HASH)
                await login_client.connect()
                res = await login_client.send_code_request(phone)

            # Store the login client so it can be used for sign_in
            active_user_clients[event.sender_id] = login_client
            user_data["phone_code_hash"] = res.phone_code_hash
            user_data["state"] = STATE_WAITING_OTP

            await safe_edit(
                msg, event,
                "📩 **Login Code Sent!**\n\n"
                "Telegram has sent an official login code to your Telegram app.\n\n"
                "⚠️ **IMPORTANT Anti-Detection Instruction**:\n"
                "Telegram automatically blocks messages containing raw 5-digit login codes.\n"
                "**Enter the code with SPACES between digits**:\n\n"
                "👉 If your code is `58291`, send: `5 8 2 9 1`"
            )
        except errors.FloodWaitError as fwe:
            user_data["state"] = STATE_IDLE
            await safe_edit(msg, event, f"⏳ **Telegram Rate Limit**: Please wait `{fwe.seconds}` seconds before requesting another code.\n\nPlease try `/login` again once the timer expires.")
        except Exception as e:
            user_data["state"] = STATE_IDLE
            await safe_edit(msg, event, f"❌ Failed to request login code: `{e}`\n\nPlease try `/login` again.")

    # 2. Waiting for Spaced OTP
    elif state == STATE_WAITING_OTP:
        clean_code = re.sub(r'[^0-9]', '', text)
        if len(clean_code) < 4:
            return await event.reply("❌ Code looks too short. Please enter the digits with spaces, e.g. `1 2 3 4 5`.")

        msg = await event.reply("⏳ Verifying code...")
        try:
            await user_client.sign_in(
                phone=user_data["phone"],
                code=clean_code,
                phone_code_hash=user_data["phone_code_hash"]
            )
            me = await user_client.get_me()
            try:
                session_str = user_client.session.save()
                if session_str and len(session_str) > 10:
                    db.save_user_session(event.sender_id, session_str, user_data.get("phone"), me.first_name, me.username)
                    logger.info(f"Session saved to DB for user {event.sender_id}")
                else:
                    logger.warning(f"Empty session string for user {event.sender_id}")
            except Exception as se:
                logger.error(f"Error saving session string: {se}")
            user_data["state"] = STATE_IDLE
            await safe_edit(
                msg, event,
                f"🎉 **Login Successful!**\n\n"
                f"Logged in as: **{me.first_name}** (@{me.username or me.id})\n\n"
                f"Send `/clone` to start cloning channels or forum groups!"
            )
        except errors.SessionPasswordNeededError:
            user_data["state"] = STATE_WAITING_2FA
            await safe_edit(msg, event, "🔒 **Two-Factor Authentication (2FA) Detected**\n\nPlease enter your Telegram 2FA Cloud Password:")
        except Exception as e:
            await safe_edit(msg, event, f"❌ Login failed: {e}\n\nTry sending the code again with spaces (e.g. `1 2 3 4 5`) or send `/login` to restart.")

    # 3. Waiting for 2FA Password
    elif state == STATE_WAITING_2FA:
        password = text
        msg = await event.reply("⏳ Verifying 2FA password...")
        try:
            await user_client.sign_in(password=password)
            me = await user_client.get_me()
            try:
                session_str = user_client.session.save()
                db.save_user_session(event.sender_id, session_str, user_data.get("phone"), me.first_name, me.username)
            except Exception as se:
                logger.error(f"Error saving session string: {se}")
            user_data["state"] = STATE_IDLE
            await safe_edit(
                msg, event,
                f"🎉 **Login Successful!**\n\n"
                f"Logged in as: **{me.first_name}** (@{me.username or me.id})\n\n"
                f"Send `/clone` to start cloning channels or forum groups!"
            )
        except Exception as e:
            await safe_edit(msg, event, f"❌ 2FA verification failed: {e}\n\nPlease try sending your password again, or send `/login` to restart.")

    # 4. Waiting for Source Chat
    elif state == STATE_WAITING_SOURCE:
        msg = await event.reply(f"⏳ Resolving source: `{text}`...")
        try:
            entity = await resolve_peer(user_client, text)
            if not entity:
                return await msg.edit("❌ Could not resolve source chat. Please verify username or invite link.")

            user_data["source_peer"] = text
            user_data["source_entity"] = entity
            user_data["state"] = STATE_WAITING_DEST
            title = getattr(entity, "title", getattr(entity, "first_name", text))
            await msg.edit(
                f"✅ **Source Selected**: `{title}`\n\n"
                f"📤 **Step 2/3: Destination Forum Group**\n\n"
                f"Please send the **Destination Forum Group** (@username, invite link, or ID):"
            )
        except Exception as e:
            await msg.edit(f"❌ Error resolving source: {e}\nPlease check and send again.")

    # 5. Waiting for Destination Chat
    elif state == STATE_WAITING_DEST:
        msg = await event.reply(f"⏳ Resolving destination: `{text}`...")
        try:
            entity = await resolve_peer(user_client, text)
            if not entity:
                return await msg.edit("❌ Could not resolve destination chat. Please verify username or invite link.")

            user_data["dest_peer"] = text
            user_data["dest_entity"] = entity
            try:
                s_peer = str(user_data.get("source_peer", ""))
                d_peer = str(text)
                s_title = getattr(user_data.get("source_entity"), "title", s_peer)
                d_title = getattr(entity, "title", d_peer)
                db.save_user_setup(event.sender_id, s_peer, d_peer, s_title, d_title)
            except Exception as e:
                logger.error(f"Failed to auto-save user setup: {e}")
            title = getattr(entity, "title", getattr(entity, "first_name", text))

            warning = ""
            if not getattr(entity, "forum", False):
                warning = "\n⚠️ *Note: Destination group does not currently have 'Topics' enabled. All messages will be placed in the main chat unless you toggle Topics ON in group settings.*"

            if user_data.get("range_start_id") is not None:
                start_id = user_data["range_start_id"]
                limit = user_data["range_limit"]
                end_id = user_data["range_end_id"]
                user_data["state"] = STATE_WAITING_RANGE_FORCE
                return await msg.edit(
                    f"✅ **Destination Selected**: `{title}`{warning}\n\n"
                    f"🎯 **Target Range Configured**:\n"
                    f"• **Start Message ID**: `#{start_id}`\n"
                    f"• **Target Limit**: `{limit}` messages (IDs `{start_id}` to `#{end_id}`)\n\n"
                    f"How should previous records in database be handled for these messages?\n"
                    f"**1** ➡️ **Skip Duplicates** (Skip messages already in database)\n"
                    f"**2** ➡️ **Force Re-clone** (Re-clone all {limit} messages)\n\n"
                    f"Reply with `1` or `2`:"
                )

            source_entity = user_data.get("source_entity")
            source_chat_id = utils.get_peer_id(source_entity) if source_entity else None
            existing_count = db.get_cloned_count(source_chat_id)
            if existing_count > 0:
                user_data["state"] = STATE_WAITING_RESUME_DECISION
                await safe_edit_status(
                    msg,
                    f"✅ **Destination Selected**: `{title}`{warning}\n\n"
                    f"⚠️ **Previous Clone History Detected** (`{existing_count}` messages already recorded in database for this channel).\n\n"
                    f"What would you like to do?\n"
                    f"**1** ➡️ **Resume / Continue** (Skip already cloned messages, only clone missing ones)\n"
                    f"**2** ➡️ **Start Over from Scratch** (Wipe records for this channel & re-clone everything from #1)\n\n"
                    f"Reply with `1` or `2`:"
                )
            else:
                user_data["state"] = STATE_WAITING_MODE
                await safe_edit_status(
                    msg,
                    f"✅ **Destination Selected**: `{title}`{warning}\n\n"
                    f"⚙️ **Step 3/3: Choose Clone Mode**\n\n"
                    f"Reply with a number:\n"
                    f"**1** ➡️ **Full History + Live Mirror** (Clone past posts, then keep forwarding new ones)\n"
                    f"**2** ➡️ **Past History Only** (Clone all past posts and stop)\n"
                    f"**3** ➡️ **Live Mirror Only** (Only forward newly incoming posts from now on)\n"
                    f"**4** ➡️ **Selective Range / Batch** (e.g. 100 messages starting from message #4)\n"
                    f"**5** ➡️ **Subject-Header Forum Clone** 📚 (Auto-sorts flat channel lectures into subject topics)\n\n"
                    f"Send `1`, `2`, `3`, `4`, or `5`:"
                )
        except Exception as e:
            await safe_edit_status(msg, f"❌ Error resolving destination: {e}\nPlease check and send again.")

    # 6. Waiting for Resume / Start Over Decision
    elif state == STATE_WAITING_RESUME_DECISION:
        choice = text.strip()
        if choice not in ("1", "2"):
            return await event.reply("❌ Invalid choice. Please reply with `1` (Resume) or `2` (Start Over).")

        prefix = ""
        source_entity = user_data.get("source_entity")
        source_chat_id = utils.get_peer_id(source_entity) if source_entity else None
        if choice == "2":
            db.clear_cloned_messages(source_chat_id)
            prefix = "🗑️ *Previous clone history for this channel wiped! Re-cloning fresh from message #1.*\n\n"
        else:
            prefix = "▶️ *Continuing / Resuming previous progress.*\n\n"

        user_data["state"] = STATE_WAITING_MODE
        await event.reply(
            f"{prefix}⚙️ **Choose Clone Mode**\n\n"
            f"Reply with a number:\n"
            f"**1** ➡️ **Full History + Live Mirror**\n"
            f"**2** ➡️ **Past History Only**\n"
            f"**3** ➡️ **Live Mirror Only**\n"
            f"**4** ➡️ **Selective Range / Batch** (e.g. 100 messages starting from message #4)\n"
            f"**5** ➡️ **Subject-Header Forum Clone** 📚 (Auto-sorts flat channel lectures into subject topics)\n\n"
            f"Send `1`, `2`, `3`, `4`, or `5`:"
        )

    # 7. Waiting for Mode Selection
    elif state == STATE_WAITING_MODE:
        choice = text.strip()
        if choice not in ("1", "2", "3", "4", "5"):
            return await event.reply("❌ Invalid choice. Please reply with `1`, `2`, `3`, `4`, or `5`.")

        if choice == "4":
            user_data["state"] = STATE_WAITING_RANGE
            return await event.reply(
                "🔢 **Step 4: Specify Message Range / Batch**\n\n"
                "Enter the starting message ID and the number of messages to extract:\n\n"
                "• Format 1: `<start_id> <count>`\n"
                "  👉 Example: `4 100` *(extracts 100 messages starting from message #4)*\n\n"
                "• Format 2: `<start_id>-<end_id>`\n"
                "  👉 Example: `4-100` *(extracts messages with IDs from 4 to 100)*\n\n"
                "👉 Send your message range:"
            )

        if choice == "5":
            user_data["state"] = STATE_IDLE
            user_data["is_running"] = True
            user_data["stats"] = {"cloned": 0, "skipped": 0, "current_topic": "Starting Subject Cloner..."}

            status_msg = await event.reply("🚀 **Initializing Subject-Header Lecture Cloner...**")
            task = asyncio.create_task(run_subject_header_cloner_worker(event.sender_id, status_msg))
            user_data["clone_task"] = task
            return

        clone_history = choice in ("1", "2")
        live_forward = choice in ("1", "3")

        user_data["state"] = STATE_IDLE
        user_data["is_running"] = True
        user_data["stats"] = {"cloned": 0, "skipped": 0, "current_topic": "Starting..."}

        status_msg = await event.reply("🚀 **Initializing Clone Process...**")
        task = asyncio.create_task(run_cloner_worker(event.sender_id, status_msg, clone_history, live_forward))
        user_data["clone_task"] = task

    # 8. Waiting for Range Selection (e.g. "4 100" or "4-100")
    elif state == STATE_WAITING_RANGE:
        raw = text.strip()
        match_dash = re.match(r"^(\d+)\s*[-:]\s*(\d+)$", raw)
        match_space = re.match(r"^(\d+)\s+(\d+)$", raw)

        start_id = None
        limit = None
        end_id = None

        if match_dash:
            start_id = int(match_dash.group(1))
            end_id = int(match_dash.group(2))
            if end_id < start_id:
                start_id, end_id = end_id, start_id
            limit = end_id - start_id + 1
        elif match_space:
            start_id = int(match_space.group(1))
            limit = int(match_space.group(2))
            end_id = start_id + limit - 1
        elif raw.isdigit():
            start_id = int(raw)
            limit = 100
            end_id = start_id + 99
        else:
            return await event.reply("❌ Invalid format. Please send `<start_id> <count>` (e.g. `4 100`) or `<start_id>-<end_id>` (e.g. `4-100`).")

        user_data["range_start_id"] = start_id
        user_data["range_limit"] = limit
        user_data["range_end_id"] = end_id

        user_data["state"] = STATE_WAITING_RANGE_FORCE
        await event.reply(
            f"🎯 **Target Range Configured**:\n"
            f"• **Starting Message ID**: `#{start_id}`\n"
            f"• **Count / Limit**: `{limit}` messages (IDs `{start_id}` to `#{end_id}`)\n\n"
            f"How should previous records in database be handled for these messages?\n"
            f"**1** ➡️ **Skip Duplicates** (Skip messages already in database)\n"
            f"**2** ➡️ **Force Re-clone** (Re-clone all {limit} messages)\n\n"
            f"Reply with `1` or `2`:"
        )

    # 9. Waiting for Force Re-clone Decision for Range
    elif state == STATE_WAITING_RANGE_FORCE:
        choice = text.strip()
        if choice not in ("1", "2"):
            return await event.reply("❌ Please reply with `1` (Skip duplicates) or `2` (Force re-clone).")

        force_reclone = (choice == "2")
        user_data["force_reclone"] = force_reclone
        user_data["state"] = STATE_IDLE
        user_data["is_running"] = True
        user_data["stats"] = {"cloned": 0, "skipped": 0, "current_topic": "Starting Range Extract..."}

        start_id = user_data["range_start_id"]
        limit = user_data["range_limit"]
        end_id = user_data["range_end_id"]

        status_msg = await event.reply(f"🚀 **Initializing Batch Extraction for {limit} messages starting from #{start_id}...**")
        task = asyncio.create_task(run_cloner_worker(
            user_id=event.sender_id,
            status_msg=status_msg,
            clone_history=True,
            live_forward=False,
            start_id=start_id,
            limit=limit,
            end_id=end_id,
            force_reclone=force_reclone
        ))
        user_data["clone_task"] = task

    # 10. Waiting for Caption Text to Remove
    elif state == STATE_WAITING_CAPTION_REMOVE:
        target = text.strip()
        if not target:
            return await event.reply("❌ Text cannot be empty.")
        db.add_caption_rule("remove", target, "")
        user_data["state"] = STATE_IDLE
        await event.reply(
            f"✅ **Rule Saved!**\n\n"
            f"The phrase `{target}` will now be removed from all forwarded video captions.\n\n"
            f"Send `/settings` to view or manage active rules."
        )

    # 11. Waiting for Caption Text to Replace
    elif state == STATE_WAITING_CAPTION_REPLACE:
        if "=>" not in text and "->" not in text:
            return await event.reply(
                "❌ Invalid format. Please use `=>` between old text and new text:\n"
                "👉 Example: `@old_channel => @my_channel`"
            )
        sep = "=>" if "=>" in text else "->"
        parts = text.split(sep, 1)
        old_part = parts[0].strip()
        new_part = parts[1].strip()
        if not old_part:
            return await event.reply("❌ Old text cannot be empty.")
        db.add_caption_rule("replace", old_part, new_part)
        user_data["state"] = STATE_IDLE
        await event.reply(
            f"✅ **Replacement Rule Saved!**\n\n"
            f"Whenever `{old_part}` appears in captions, it will be replaced with `{new_part}`.\n\n"
            f"Send `/settings` to view or manage active rules."
        )

    # 12. Waiting for Caption Footer / Suffix
    elif state == STATE_WAITING_CAPTION_SUFFIX:
        suffix_text = text.strip()
        if not suffix_text:
            return await event.reply("❌ Footer text cannot be empty.")
        db.add_caption_rule("suffix", "\n\n" + suffix_text, "")
        user_data["state"] = STATE_IDLE
        await event.reply(
            f"✅ **Footer Saved!**\n\n"
            f"This text will be appended to every video caption:\n`{suffix_text}`\n\n"
            f"Send `/settings` to view or manage active rules."
        )

    # 13. Waiting for Sync Source Chat
    elif state == STATE_WAITING_SYNC_SOURCE:
        msg = await event.reply(f"⏳ Resolving source: `{text}`...")
        try:
            entity = await resolve_peer(user_client, text)
            if not entity:
                return await msg.edit("❌ Could not resolve source chat. Please verify ID or link.")
            user_data["sync_source_peer"] = text
            user_data["sync_source_entity"] = entity
            user_data["state"] = STATE_WAITING_SYNC_SOURCE_RANGE
            title = getattr(entity, "title", getattr(entity, "first_name", text))
            await msg.edit(
                f"✅ **Source Chat Set**: `{title}`\n\n"
                f"👉 **Step 2/4**: Enter the **Source Message Range**:\n"
                f"• Example: `1-100` *(messages #1 to #100)*\n"
                f"• Or: `4 100` *(100 messages starting from #4)*"
            )
        except Exception as e:
            await msg.edit(f"❌ Failed to resolve source chat: {e}")

    # 14. Waiting for Sync Source Range
    elif state == STATE_WAITING_SYNC_SOURCE_RANGE:
        def parse_range_str(raw: str):
            raw = raw.strip()
            match_dash = re.match(r"^(\d+)\s*[-:]\s*(\d+)$", raw)
            match_space = re.match(r"^(\d+)\s+(\d+)$", raw)
            if match_dash:
                s = int(match_dash.group(1))
                e = int(match_dash.group(2))
                if e < s:
                    s, e = e, s
                return s, e
            elif match_space:
                s = int(match_space.group(1))
                val = int(match_space.group(2))
                if val > s:
                    return s, val
                else:
                    return s, s + val - 1
            elif raw.isdigit():
                s = int(raw)
                return s, s + 99
            return None, None

        s_start, s_end = parse_range_str(text)
        if s_start is None:
            return await event.reply("❌ Invalid format. Please send e.g. `1-100` or `1 100`.")

        user_data["sync_source_start"] = s_start
        user_data["sync_source_end"] = s_end
        user_data["state"] = STATE_WAITING_SYNC_DEST
        await event.reply(
            f"✅ **Source Range Set**: `#{s_start}` to `#{s_end}`\n\n"
            f"👉 **Step 3/4**: Please send the **Destination Channel ID or Link** (where the videos are currently sitting without captions):\n"
            f"• Private Channel ID: `-1009876543210`\n"
            f"• Invite Link: `https://t.me/+yyyyyy`"
        )

    # 15. Waiting for Sync Destination Chat
    elif state == STATE_WAITING_SYNC_DEST:
        msg = await event.reply(f"⏳ Resolving destination: `{text}`...")
        try:
            entity = await resolve_peer(user_client, text)
            if not entity:
                return await msg.edit("❌ Could not resolve destination chat. Please verify ID or link.")
            user_data["sync_dest_peer"] = text
            user_data["sync_dest_entity"] = entity
            user_data["state"] = STATE_WAITING_SYNC_DEST_RANGE
            title = getattr(entity, "title", getattr(entity, "first_name", text))
            await msg.edit(
                f"✅ **Destination Chat Set**: `{title}`\n\n"
                f"👉 **Step 4/4**: Enter the **Destination Message Range**:\n"
                f"• Example: `5-105` *(videos sitting at IDs #5 to #105)*\n"
                f"• Or: `1-100`"
            )
        except Exception as e:
            await msg.edit(f"❌ Failed to resolve destination chat: {e}")

    # 16. Waiting for Sync Destination Range
    elif state == STATE_WAITING_SYNC_DEST_RANGE:
        def parse_range_str(raw: str):
            raw = raw.strip()
            match_dash = re.match(r"^(\d+)\s*[-:]\s*(\d+)$", raw)
            match_space = re.match(r"^(\d+)\s+(\d+)$", raw)
            if match_dash:
                s = int(match_dash.group(1))
                e = int(match_dash.group(2))
                if e < s:
                    s, e = e, s
                return s, e
            elif match_space:
                s = int(match_space.group(1))
                val = int(match_space.group(2))
                if val > s:
                    return s, val
                else:
                    return s, s + val - 1
            elif raw.isdigit():
                s = int(raw)
                return s, s + 99
            return None, None

        d_start, d_end = parse_range_str(text)
        if d_start is None:
            return await event.reply("❌ Invalid format. Please send e.g. `5-105` or `5 100`.")

        user_data["sync_dest_start"] = d_start
        user_data["sync_dest_end"] = d_end
        user_data["state"] = STATE_IDLE
        user_data["is_running"] = True

        status_msg = await event.reply("🚀 **Initializing Zero-Data Caption Sync...**")
        task = asyncio.create_task(run_caption_sync_worker(event.sender_id, status_msg))
        user_data["clone_task"] = task

    # 17. Waiting for Setup Name
    elif state == STATE_WAITING_SETUP_NAME:
        setup_name = text.strip()
        if not setup_name:
            return await event.reply("❌ Name cannot be empty.")
        user_data["new_setup_name"] = setup_name
        user_data["state"] = STATE_WAITING_SETUP_SOURCE
        await event.reply(
            f"✅ **Profile Name**: `{setup_name}`\n\n"
            f"📥 **Step 2/3: Source Channel**\n"
            f"Please send the **Source Channel** (@username, invite link, or ID):"
        )

    # 18. Waiting for Setup Source
    elif state == STATE_WAITING_SETUP_SOURCE:
        msg = await event.reply(f"⏳ Resolving source: `{text}`...")
        try:
            _uclient = await get_user_client(event.sender_id)
            entity = await resolve_peer(_uclient, text)
            if not entity:
                return await msg.edit("❌ Could not resolve source chat. Please verify username or invite link.")
            user_data["new_setup_source_peer"] = text
            user_data["new_setup_source_entity"] = entity
            user_data["state"] = STATE_WAITING_SETUP_DEST
            title = getattr(entity, "title", getattr(entity, "first_name", text))
            await msg.edit(
                f"✅ **Source Channel**: `{title}`\n\n"
                f"📤 **Step 3/3: Destination Channel or Forum Group**\n"
                f"Please send the **Destination Channel or Forum Group** (@username, invite link, or ID):"
            )
        except Exception as e:
            logger.error(f"Setup source resolve error: {e}")
            await msg.edit(f"❌ Failed to resolve source: `{e}`\n\nMake sure you're a member of this channel/group and your account is logged in.")

    # 19. Waiting for Setup Destination
    elif state == STATE_WAITING_SETUP_DEST:
        msg = await event.reply(f"⏳ Resolving destination: `{text}`...")
        try:
            _uclient = await get_user_client(event.sender_id)
            entity = await resolve_peer(_uclient, text)
            if not entity:
                return await msg.edit("❌ Could not resolve destination chat. Please verify username or invite link.")
            s_peer = user_data.get("new_setup_source_peer", "")
            d_peer = text
            s_title = getattr(user_data.get("new_setup_source_entity"), "title", s_peer)
            d_title = getattr(entity, "title", getattr(entity, "first_name", text))
            name = user_data.get("new_setup_name", "Profile")

            sid = db.add_user_setup(event.sender_id, name, s_peer, d_peer, s_title, d_title)
            user_data["state"] = STATE_IDLE
            await msg.edit(
                f"🎉 **Setup Profile Saved Successfully!**\n\n"
                f"🏷️ **Profile**: `{name}` (ID: `#{sid}`)\n"
                f"• 📥 **Source**: `{s_title}`\n"
                f"• 📤 **Destination**: `{d_title}`\n\n"
                f"Send `/setup` anytime to see all your setups and start with 1-click!"
            )
        except Exception as e:
            logger.error(f"Setup destination resolve error: {e}")
            await msg.edit(f"❌ Failed to resolve destination: `{e}`\n\nMake sure you're a member of the destination group and your account is logged in.")


# --------------------- CAPTION SYNC WORKER ---------------------

async def run_caption_sync_worker(user_id: int, status_msg):
    user_data = get_user_data(user_id)
    user_client = await get_user_client(user_id)
    s_entity = user_data["sync_source_entity"]
    d_entity = user_data["sync_dest_entity"]
    s_start = user_data["sync_source_start"]
    s_end = user_data["sync_source_end"]
    d_start = user_data["sync_dest_start"]
    d_end = user_data["sync_dest_end"]

    try:
        await status_msg.edit(f"⏳ **Scanning Source Messages (`#{s_start}` to `#{s_end}`)...**\nIndexing video captions...")

        source_media_list = []
        s_min = max(0, s_start - 1)
        s_max = s_end + 1
        async for m in user_client.iter_messages(s_entity, min_id=s_min, max_id=s_max, reverse=True):
            if m.id < s_start or m.id > s_end:
                continue
            if m.media:
                size = getattr(m.file, "size", None)
                duration = None
                if hasattr(m, "document") and m.document:
                    for attr in m.document.attributes or []:
                        if isinstance(attr, types.DocumentAttributeVideo):
                            duration = attr.duration
                            break
                source_media_list.append({
                    "id": m.id,
                    "text": m.text or "",
                    "entities": m.entities,
                    "size": size,
                    "duration": duration
                })

        total_source = len(source_media_list)
        await status_msg.edit(
            f"✅ Indexed **{total_source} media messages** from source.\n"
            f"⏳ Scanning destination messages (`#{d_start}` to `#{d_end}`)..."
        )

        dest_media_list = []
        d_min = max(0, d_start - 1)
        d_max = d_end + 1
        async for m in user_client.iter_messages(d_entity, min_id=d_min, max_id=d_max, reverse=True):
            if m.id < d_start or m.id > d_end:
                continue
            if m.media:
                size = getattr(m.file, "size", None)
                duration = None
                if hasattr(m, "document") and m.document:
                    for attr in m.document.attributes or []:
                        if isinstance(attr, types.DocumentAttributeVideo):
                            duration = attr.duration
                            break
                dest_media_list.append({
                    "id": m.id,
                    "text": m.text or "",
                    "size": size,
                    "duration": duration
                })

        total_dest = len(dest_media_list)
        updated_count = 0
        skipped_count = 0
        last_edit = 0

        # Build lookup table by (file_size, video_duration) for 100% exact match
        source_by_meta = {}
        for sm in source_media_list:
            if sm["size"]:
                key = (sm["size"], sm["duration"])
                source_by_meta[key] = sm

        for idx, dm in enumerate(dest_media_list):
            if not user_data.get("is_running", False):
                break

            matched_source = None
            key = (dm["size"], dm["duration"])
            if key in source_by_meta:
                matched_source = source_by_meta[key]
            elif idx < len(source_media_list):
                matched_source = source_media_list[idx]

            if not matched_source:
                skipped_count += 1
                continue

            raw_caption = matched_source["text"]
            new_caption = db.apply_caption_rules(raw_caption) if raw_caption else ""

            if dm["text"] == new_caption:
                skipped_count += 1
                continue

            try:
                await user_client.edit_message(
                    d_entity,
                    dm["id"],
                    text=new_caption,
                    formatting_entities=matched_source["entities"] if raw_caption == new_caption else None
                )
                updated_count += 1
                await asyncio.sleep(0.3)
            except errors.FloodWaitError as fwe:
                await asyncio.sleep(fwe.seconds + 1)
                try:
                    await user_client.edit_message(d_entity, dm["id"], text=new_caption)
                    updated_count += 1
                except Exception:
                    pass
            except Exception as ex:
                logger.warning(f"Could not edit destination message #{dm['id']}: {ex}")
                skipped_count += 1

            now = asyncio.get_event_loop().time()
            if updated_count % 5 == 0 or (now - last_edit > 5):
                last_edit = now
                try:
                    await status_msg.edit(
                        f"📝 **Syncing Captions in Progress...**\n\n"
                        f"• **Current**: Dest Message `#{dm['id']}`\n"
                        f"• **Updated**: `{updated_count}` captions\n"
                        f"• **Skipped / Unchanged**: `{skipped_count}`\n"
                        f"• **Total Dest Videos**: `{total_dest}`"
                    )
                except Exception:
                    pass

        user_data["is_running"] = False
        await status_msg.edit(
            f"🎉 **Caption Synchronization Complete!**\n\n"
            f"• **Updated**: `{updated_count}` captions\n"
            f"• **Skipped / Already Matching**: `{skipped_count}`\n"
            f"• **Total Checked**: `{total_dest}` videos\n\n"
            f"All captions were synchronized and rules applied without downloading any videos!"
        )
    except Exception as e:
        user_data["is_running"] = False
        logger.error(f"Error in caption sync worker: {e}", exc_info=True)
        await status_msg.edit(f"❌ Caption sync failed: {e}")


# --------------------- BACKGROUND CLONER WORKER ---------------------

async def run_cloner_worker(
    user_id: int,
    status_msg, 
    clone_history: bool, 
    live_forward: bool,
    start_id: Optional[int] = None,
    limit: Optional[int] = None,
    end_id: Optional[int] = None,
    force_reclone: bool = False
):
    ensure_temp_dir()
    user_data = get_user_data(user_id)
    user_client = await get_user_client(user_id)
    source_entity = user_data["source_entity"]
    dest_entity = user_data["dest_entity"]

    source_title = getattr(source_entity, "title", "Source")
    dest_title = getattr(dest_entity, "title", "Destination")
    source_chat_id = utils.get_peer_id(source_entity) if source_entity else None

    try:
        # Step 1: Sync Topics
        await safe_edit_status(status_msg, f"🔄 **Synchronizing Topics...**\n`{source_title}` ➡️ `{dest_title}`")
        
        async def topic_progress(text_update):
            await safe_edit_status(status_msg, f"🔄 **Syncing Topics**:\n{text_update}")

        await sync_forum_topics(user_client, source_entity, dest_entity, db, topic_progress)
        await safe_edit_status(status_msg, "✅ **Topics Synchronized!** Preparing message clone...")

        # Step 2: Clone History
        if clone_history:
            cloned_count = 0
            skipped_count = 0
            last_edit_time = 0

            # Calculate total messages in source channel
            total_source_msgs = None
            try:
                history_res = await user_client.get_messages(source_entity, limit=0)
                total_source_msgs = history_res.total
            except Exception as e:
                logger.warning(f"Could not fetch total message count: {e}")

            if limit is not None and limit > 0:
                total_target = limit
            else:
                total_target = total_source_msgs

            user_data["stats"]["total_target"] = total_target

            def get_progress_block():
                if total_target:
                    remaining = max(0, total_target - cloned_count)
                    pct = min(100.0, (cloned_count / total_target) * 100)
                    bar = make_progress_bar(pct)
                    return (
                        f"• **Progress**: `{cloned_count}` / `{total_target}` messages (`{pct:.1f}%`)\n"
                        f"[{bar}] `{pct:.1f}%`\n"
                        f"• **Remaining**: `{remaining}` messages left\n"
                    )
                else:
                    return f"• **Progress**: `{cloned_count}` messages cloned\n"

            album_buffer = []
            current_grouped_id = None
            current_topic_id = None

            async def flush_album():
                nonlocal album_buffer, current_grouped_id, current_topic_id, cloned_count
                if not album_buffer:
                    return
                dest_topic = None
                if current_topic_id:
                    dest_topic = await ensure_topic_for_message(user_client, source_entity, dest_entity, db, current_topic_id)

                while True:
                    try:
                        first_text = album_buffer[0].text if album_buffer and album_buffer[0].text else None
                        custom_caption = db.apply_caption_rules(first_text) if first_text else None
                        await send_album_fast(user_client, album_buffer, dest_entity, dest_topic, custom_caption=custom_caption)
                        for m in album_buffer:
                            db.record_cloned_message(m.id, None, current_topic_id, source_chat_id)
                        cloned_count += len(album_buffer)
                        user_data["stats"]["cloned"] = cloned_count
                        await asyncio.sleep(DELAY_BETWEEN_MESSAGES)
                        break
                    except errors.FloodWaitError as fwe:
                        await asyncio.sleep(fwe.seconds + 2)
                    except Exception as e:
                        logger.error(f"Error in album: {e}")
                        break
                album_buffer = []
                current_grouped_id = None
                current_topic_id = None

            iter_kwargs = {"reverse": True}
            if start_id is not None and start_id > 0:
                iter_kwargs["min_id"] = start_id - 1
            if end_id is not None and end_id > 0:
                iter_kwargs["max_id"] = end_id + 1
            if limit is not None and limit > 0:
                iter_kwargs["limit"] = limit

            is_range_job = (start_id is not None or limit is not None or end_id is not None)
            total_target_str = f"/{limit}" if limit else ""

            async for msg in user_client.iter_messages(source_entity, **iter_kwargs):
                if not user_data.get("is_running", False):
                    break

                if msg.action:
                    skipped_count += 1
                    continue

                if not force_reclone and db.is_message_cloned(msg.id, source_chat_id):
                    skipped_count += 1
                    continue

                # Album handling
                if msg.grouped_id:
                    if current_grouped_id is None:
                        current_grouped_id = msg.grouped_id
                        current_topic_id = get_message_topic_id(msg)
                        album_buffer.append(msg)
                        continue
                    elif msg.grouped_id == current_grouped_id:
                        album_buffer.append(msg)
                        continue
                    else:
                        await flush_album()
                        current_grouped_id = msg.grouped_id
                        current_topic_id = get_message_topic_id(msg)
                        album_buffer.append(msg)
                        continue
                else:
                    if album_buffer:
                        await flush_album()

                # Single message
                source_topic_id = get_message_topic_id(msg)
                dest_topic_id = None
                if source_topic_id:
                    dest_topic_id = await ensure_topic_for_message(user_client, source_entity, dest_entity, db, source_topic_id)
                    user_data["stats"]["current_topic"] = f"Topic {source_topic_id}"

                async def speed_status_updater(progress_str):
                    try:
                        title_hdr = "🎯 **Extracting Message Range**" if is_range_job else "📦 **Cloning History in Progress**"
                        p_block = get_progress_block()
                        await status_msg.edit(
                            f"{title_hdr} *(High-Speed Mode)*\n\n"
                            f"• **Source**: `{source_title}`\n"
                            f"• **Destination**: `{dest_title}`\n"
                            f"• **Current Msg ID**: `#{msg.id}`\n"
                            f"{p_block}"
                            f"• **Topic**: `{user_data['stats']['current_topic']}`\n\n"
                            f"{progress_str}\n\n"
                            f"Send `/status` for info or `/stop` to cancel."
                        )
                    except Exception:
                        pass

                while True:
                    try:
                        custom_caption = db.apply_caption_rules(msg.text)
                        dest_msg_id = await send_single_message_fast(
                            user_client,
                            msg,
                            dest_entity,
                            dest_topic_id,
                            status_updater=speed_status_updater,
                            custom_caption=custom_caption
                        )
                        db.record_cloned_message(msg.id, dest_msg_id, source_topic_id, source_chat_id)
                        cloned_count += 1
                        user_data["stats"]["cloned"] = cloned_count
                        user_data["stats"]["skipped"] = skipped_count

                        # Live Telegram progress message edit every 10 messages
                        now = asyncio.get_event_loop().time()
                        if cloned_count % 10 == 0 or (now - last_edit_time > 5):
                            last_edit_time = now
                            title_hdr = "🎯 **Extracting Message Range**" if is_range_job else "📦 **Cloning History in Progress**"
                            p_block = get_progress_block()
                            try:
                                await status_msg.edit(
                                    f"{title_hdr} *(High-Speed Mode)*\n\n"
                                    f"• **Source**: `{source_title}`\n"
                                    f"• **Destination**: `{dest_title}`\n"
                                    f"• **Current Msg ID**: `#{msg.id}`\n"
                                    f"{p_block}"
                                    f"• **Skipped**: `{skipped_count}` messages\n"
                                    f"• **Topic**: `{user_data['stats']['current_topic']}`\n\n"
                                    f"Send `/status` for info or `/stop` to cancel."
                                )
                            except Exception:
                                pass

                        await asyncio.sleep(DELAY_BETWEEN_MESSAGES)
                        break
                    except errors.FloodWaitError as fwe:
                        try:
                            await status_msg.edit(f"⏳ **Rate Limited by Telegram**: Sleeping {fwe.seconds} seconds...")
                        except Exception:
                            pass
                        await asyncio.sleep(fwe.seconds + 2)
                    except Exception as e:
                        logger.error(f"Error copying #{msg.id}: {e}")
                        # Do not record failed copies so they can be retried on resume
                        break

            if album_buffer:
                await flush_album()

            finish_title = "✅ **Selective Message Extraction Complete!**" if is_range_job else "✅ **History Clone Complete!**"
            range_line = f"• **Requested Range**: Starting from `#{start_id or 1}` (Target: {limit or 'all'})\n" if is_range_job else ""
            await safe_edit_status(
                status_msg,
                f"{finish_title}\n\n"
                f"{range_line}"
                f"• **Total Cloned**: `{cloned_count}` messages\n"
                f"• **Skipped**: `{skipped_count}`\n"
                f"• **Source**: `{source_title}`\n"
                f"• **Destination**: `{dest_title}`"
            )

        # Step 3: Live Forwarder
        if live_forward and user_data.get("is_running", False):
            user_data["stats"]["stage"] = "live"
            user_data["stats"]["current_topic"] = "👂 Waiting for new posts (24/7)"
            await bot.send_message(
                status_msg.chat_id,
                f"🟢 **Live Mirroring is Now ACTIVE!**\n\n"
                f"✅ History clone is 100% complete ({cloned_count} messages cloned).\n"
                f"Listening for any new messages posted in `{source_title}` and forwarding them into `{dest_title}`.\n"
                f"Send `/stop` when you wish to terminate."
            )
        else:
            user_data["is_running"] = False
            user_data["stats"]["stage"] = "completed"
            user_data["stats"]["current_topic"] = "None (Finished)"

        if live_forward and user_data.get("is_running", False):
            @user_client.on(events.NewMessage(chats=source_entity))
            async def live_handler(event):
                msg = event.message
                if msg.action or db.is_message_cloned(msg.id, source_chat_id):
                    return
                s_topic = get_message_topic_id(msg)
                d_topic = None
                if s_topic:
                    d_topic = await ensure_topic_for_message(user_client, source_entity, dest_entity, db, s_topic)
                try:
                    custom_caption = db.apply_caption_rules(msg.text)
                    dest_msg_id = await send_single_message_fast(
                        user_client, 
                        msg, 
                        dest_entity, 
                        d_topic,
                        custom_caption=custom_caption
                    )
                    db.record_cloned_message(msg.id, dest_msg_id, s_topic, source_chat_id)
                    logger.info(f"[Live] Forwarded #{msg.id} -> topic {d_topic}")
                except Exception as ex:
                    logger.error(f"[Live] Error: {ex}")

            user_data["live_handler"] = live_handler

    except asyncio.CancelledError:
        user_data["is_running"] = False
        await bot.send_message(status_msg.chat_id, "⏹️ Cloning process stopped by user.")
    except Exception as e:
        user_data["is_running"] = False
        logger.error(f"Worker crashed: {e}", exc_info=True)
        await bot.send_message(status_msg.chat_id, f"❌ Cloner encountered an error: {e}")
    finally:
        cleanup_temp_dir()


# --------------------- SUBJECT-HEADER FORUM CLONER ---------------------

CANONICAL_SUBJECTS = {
    "Anatomy": ["anatomy", "anat", "embryology", "histology", "neuroanatomy"],
    "Physiology": ["physiology", "physio"],
    "Biochemistry": ["biochemistry", "biochem", "metabolism"],
    "Pharmacology": ["pharmacology", "pharma"],
    "Pathology": ["pathology", "patho", "hematology"],
    "Microbiology": ["microbiology", "micro", "bacteriology", "virology", "parasitology", "mycology"],
    "Forensic Medicine": ["forensic medicine", "forensic", "fmt", "toxicology"],
    "PSM (Community Medicine)": ["psm", "community medicine", "spm", "epidemiology"],
    "Ophthalmology": ["ophthalmology", "opthalmology", "optha", "eye"],
    "ENT": ["ent", "ear nose throat", "otorhinolaryngology"],
    "Medicine": ["medicine", "internal medicine", "general medicine"],
    "Surgery": ["surgery", "general surgery"],
    "Orthopedics": ["orthopedics", "orthopaedics", "ortho"],
    "OBGYN": ["obstetrics", "gynecology", "obg", "obgy", "obgyn", "obs and gynae", "obs & gynae", "obstetrics and gynecology"],
    "Pediatrics": ["pediatrics", "paediatrics", "peds"],
    "Dermatology": ["dermatology", "derma", "skin"],
    "Psychiatry": ["psychiatry", "psych"],
    "Radiology": ["radiology", "radio", "radio-diagnosis"],
    "Anesthesia": ["anesthesia", "anaesthesia"],
}

def detect_subject_header(message) -> Optional[str]:
    """
    Identifies if a message is a pure text delimiter header indicating a subject.
    Media (videos/photos/documents) NEVER match, avoiding false classification
    (e.g., 'Anatomy of Uterus' in OBGY stays in OBGY).
    """
    if not message or message.media is not None:
        return None
    raw_text = (message.text or "").strip()
    if not raw_text:
        return None

    # Strip usernames (@yourrwishh), URLs, hashtags, punctuation, and decorative emojis
    cleaned = re.sub(r"@\w+", "", raw_text)
    cleaned = re.sub(r"https?://\S+|t\.me/\S+", "", cleaned)
    cleaned = re.sub(r"[^\w\s]", " ", cleaned).strip().lower()
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    if not cleaned or len(cleaned) > 40:
        return None

    for canon, aliases in CANONICAL_SUBJECTS.items():
        if cleaned == canon.lower() or cleaned in aliases:
            return canon
        for alias in aliases:
            if re.fullmatch(rf"{re.escape(alias)}(\s+(lecture|lectures|part|unit|notes|video|videos))?", cleaned):
                return canon

    return None

async def run_subject_header_cloner_worker(
    user_id: int,
    status_msg,
    force_reclone: bool = False
):
    """
    Clones a flat channel where lectures are grouped under subject header text messages
    (e.g., 'Anatomy' header followed by anatomy videos), auto-creating and sorting into
    matching destination forum topics.
    """
    ensure_temp_dir()
    user_data = get_user_data(user_id)
    user_client = await get_user_client(user_id)
    source_entity = user_data["source_entity"]
    dest_entity = user_data["dest_entity"]

    source_title = getattr(source_entity, "title", "Source")
    dest_title = getattr(dest_entity, "title", "Destination")
    source_chat_id = utils.get_peer_id(source_entity) if source_entity else None

    try:
        await safe_edit_status(status_msg, f"🔍 **Reading Destination Forum Topics...**\n`{dest_title}`")
        dest_topics_cache: Dict[str, int] = {}
        try:
            offset_id = 0
            offset_topic = 0
            while True:
                res = await user_client(GetForumTopicsRequest(
                    peer=dest_entity,
                    offset_date=None,
                    offset_id=offset_id,
                    offset_topic=offset_topic,
                    limit=100
                ))
                for t in getattr(res, "topics", []):
                    if isinstance(t, types.ForumTopic):
                        dest_topics_cache[t.title.strip().lower()] = t.id
                if len(getattr(res, "topics", [])) < 100:
                    break
                last_topic = res.topics[-1]
                offset_topic = last_topic.id
                offset_id = getattr(last_topic, "top_message", 0)
        except Exception as e:
            logger.warning(f"Could not load destination topics cache: {e}")

        async def get_or_create_subject_topic(subject_name: str) -> Optional[int]:
            s_lower = subject_name.strip().lower()
            if s_lower in dest_topics_cache:
                return dest_topics_cache[s_lower]
            for attempt in range(2):
                try:
                    res = await user_client(CreateForumTopicRequest(
                        peer=dest_entity,
                        title=subject_name,
                        random_id=random.randint(1, 2**63 - 1)
                    ))
                    t_id = extract_topic_id_from_updates(res)
                    if t_id:
                        dest_topics_cache[s_lower] = t_id
                        return t_id
                except errors.FloodWaitError as fwe:
                    await asyncio.sleep(fwe.seconds + 2)
                except Exception as ex:
                    logger.error(f"Failed to create forum topic for '{subject_name}': {ex}")
                    break
            return None

        # Fetch total messages for progress calculation
        cloned_count = 0
        skipped_count = 0
        last_edit_time = 0
        total_source_msgs = None
        try:
            history_res = await user_client.get_messages(source_entity, limit=0)
            total_source_msgs = history_res.total
        except Exception as e:
            logger.warning(f"Could not fetch total message count: {e}")

        user_data["stats"]["total_target"] = total_source_msgs

        def get_progress_block():
            if total_source_msgs:
                remaining = max(0, total_source_msgs - cloned_count)
                pct = min(100.0, (cloned_count / total_source_msgs) * 100)
                bar = make_progress_bar(pct)
                return (
                    f"• **Progress**: `{cloned_count}` / `{total_source_msgs}` messages (`{pct:.1f}%`)\n"
                    f"[{bar}] `{pct:.1f}%`\n"
                    f"• **Remaining**: `{remaining}` messages left\n"
                )
            else:
                return f"• **Progress**: `{cloned_count}` messages cloned\n"

        current_subject = "General / Introduction"
        current_dest_topic_id = None
        user_data["stats"]["current_topic"] = current_subject

        album_buffer = []
        current_grouped_id = None

        async def flush_album():
            nonlocal album_buffer, current_grouped_id, cloned_count
            if not album_buffer:
                return
            target_topic = current_dest_topic_id
            while True:
                try:
                    first_text = album_buffer[0].text if album_buffer and album_buffer[0].text else None
                    custom_caption = db.apply_caption_rules(first_text) if first_text else None
                    await send_album_fast(user_client, album_buffer, dest_entity, target_topic, custom_caption=custom_caption)
                    for m in album_buffer:
                        db.record_cloned_message(m.id, None, target_topic, source_chat_id)
                    cloned_count += len(album_buffer)
                    user_data["stats"]["cloned"] = cloned_count
                    await asyncio.sleep(DELAY_BETWEEN_MESSAGES)
                    break
                except errors.FloodWaitError as fwe:
                    await asyncio.sleep(fwe.seconds + 2)
                except Exception as e:
                    logger.error(f"Error in subject album flush: {e}")
                    break
            album_buffer = []
            current_grouped_id = None

        await safe_edit_status(status_msg, f"🚀 **Starting Subject-Header Lecture Clone...**\n`{source_title}` ➡️ `{dest_title}`")

        async for msg in user_client.iter_messages(source_entity, reverse=True):
            if not user_data.get("is_running", False):
                break

            if msg.action:
                skipped_count += 1
                continue

            # Check if this message is a Subject Delimiter Header
            detected_subj = detect_subject_header(msg)
            if detected_subj:
                if album_buffer:
                    await flush_album()
                current_subject = detected_subj
                current_dest_topic_id = await get_or_create_subject_topic(current_subject)
                user_data["stats"]["current_topic"] = current_subject
                db.record_cloned_message(msg.id, None, current_dest_topic_id, source_chat_id)
                logger.info(f"📚 Subject Delimiter Detected: #{msg.id} -> '{current_subject}' (Topic ID: {current_dest_topic_id})")
                continue

            if not force_reclone and db.is_message_cloned(msg.id, source_chat_id):
                skipped_count += 1
                continue

            if current_dest_topic_id is None:
                current_dest_topic_id = await get_or_create_subject_topic(current_subject)
                user_data["stats"]["current_topic"] = current_subject

            # Album handling
            if msg.grouped_id:
                if current_grouped_id is None:
                    current_grouped_id = msg.grouped_id
                    album_buffer.append(msg)
                    continue
                elif msg.grouped_id == current_grouped_id:
                    album_buffer.append(msg)
                    continue
                else:
                    await flush_album()
                    current_grouped_id = msg.grouped_id
                    album_buffer.append(msg)
                    continue
            else:
                if album_buffer:
                    await flush_album()

            async def speed_status_updater(progress_str):
                try:
                    p_block = get_progress_block()
                    await status_msg.edit(
                        f"📚 **Subject-Header Lecture Cloner** *(High-Speed Mode)*\n\n"
                        f"• **Source**: `{source_title}`\n"
                        f"• **Destination**: `{dest_title}`\n"
                        f"• **Current Msg ID**: `#{msg.id}`\n"
                        f"{p_block}"
                        f"• **Active Subject**: `{current_subject}`\n\n"
                        f"{progress_str}\n\n"
                        f"Send `/status` for info or `/stop` to cancel."
                    )
                except Exception:
                    pass

            while True:
                try:
                    custom_caption = db.apply_caption_rules(msg.text)
                    dest_msg_id = await send_single_message_fast(
                        user_client,
                        msg,
                        dest_entity,
                        current_dest_topic_id,
                        status_updater=speed_status_updater,
                        custom_caption=custom_caption
                    )
                    db.record_cloned_message(msg.id, dest_msg_id, current_dest_topic_id, source_chat_id)
                    cloned_count += 1
                    user_data["stats"]["cloned"] = cloned_count
                    user_data["stats"]["skipped"] = skipped_count

                    now = asyncio.get_event_loop().time()
                    if cloned_count % 10 == 0 or (now - last_edit_time > 5):
                        last_edit_time = now
                        p_block = get_progress_block()
                        try:
                            await status_msg.edit(
                                f"📚 **Subject-Header Lecture Cloner** *(High-Speed Mode)*\n\n"
                                f"• **Source**: `{source_title}`\n"
                                f"• **Destination**: `{dest_title}`\n"
                                f"• **Current Msg ID**: `#{msg.id}`\n"
                                f"{p_block}"
                                f"• **Skipped**: `{skipped_count}` messages\n"
                                f"• **Active Subject**: `{current_subject}`\n\n"
                                f"Send `/status` for info or `/stop` to cancel."
                            )
                        except Exception:
                            pass

                    await asyncio.sleep(DELAY_BETWEEN_MESSAGES)
                    break
                except errors.FloodWaitError as fwe:
                    try:
                        await status_msg.edit(f"⏳ **Rate Limited by Telegram**: Sleeping {fwe.seconds} seconds...")
                    except Exception:
                        pass
                    await asyncio.sleep(fwe.seconds + 2)
                except Exception as e:
                    logger.error(f"Error copying #{msg.id} in subject cloner: {e}")
                    break

        if album_buffer:
            await flush_album()

        user_data["is_running"] = False
        user_data["stats"]["stage"] = "completed"
        user_data["stats"]["current_topic"] = "None (Finished)"

        await safe_edit_status(
            status_msg,
            f"✅ **Subject-Header Lecture Clone Complete!**\n\n"
            f"• **Total Cloned**: `{cloned_count}` messages\n"
            f"• **Skipped**: `{skipped_count}`\n"
            f"• **Source**: `{source_title}`\n"
            f"• **Destination**: `{dest_title}`\n\n"
            f"All lectures were automatically organized into their respective subject forum topics!"
        )

    except asyncio.CancelledError:
        user_data["is_running"] = False
        await bot.send_message(status_msg.chat_id, "⏹️ Subject cloning process stopped by user.")
    except Exception as e:
        user_data["is_running"] = False
        logger.error(f"Subject cloner worker crashed: {e}", exc_info=True)
        await bot.send_message(status_msg.chat_id, f"❌ Subject cloner encountered an error: {e}")
    finally:
        cleanup_temp_dir()



# --------------------- MAIN ENTRYPOINT ---------------------

async def main():
    global BOT_TOKEN
    # Wipe any leftover temp downloads from previous runs
    cleanup_temp_dir()
    print("""
+===============================================================+
|         TELEGRAM INTERACTIVE BOT CLONER CONTROLLER            |
+===============================================================+
    """)

    if not BOT_TOKEN:
        print("Bot Token not found in config.env.")
        BOT_TOKEN = input("Enter your Bot Token from @BotFather: ").strip()
        save_env_var("BOT_TOKEN", BOT_TOKEN)

    print(f"Connecting Bot Client with token: {BOT_TOKEN[:10]}...")
    await bot.start(bot_token=BOT_TOKEN)
    bot_me = await bot.get_me()
    print(f"Bot connected successfully as: @{bot_me.username}")

    # Register slash commands so they autocomplete when typing /
    try:
        from telethon.tl.functions.bots import SetBotCommandsRequest
        from telethon.tl.types import BotCommand, BotCommandScopeDefault
        await bot(SetBotCommandsRequest(
            scope=BotCommandScopeDefault(),
            lang_code="",
            commands=[
                BotCommand(command="start", description="Check bot status & menu"),
                BotCommand(command="login", description="Log in your Telegram account"),
                BotCommand(command="clone", description="Clone channel or forum"),
                BotCommand(command="extract", description="Extract specific message range"),
                BotCommand(command="settings", description="Caption rules & bot settings"),
                BotCommand(command="status", description="View live cloning progress"),
                BotCommand(command="storage", description="View VPS disk & system stats"),
                BotCommand(command="users", description="List authorized users"),
                BotCommand(command="add_user", description="Authorize user (Owner only)"),
                BotCommand(command="remove_user", description="Revoke user access (Owner only)"),
                BotCommand(command="reset", description="Reset database history"),
                BotCommand(command="stop", description="Stop current cloning"),
                BotCommand(command="logout", description="Log out your Telegram session"),
                BotCommand(command="help", description="Help and instructions"),
            ]
        ))
    except Exception as e:
        logger.warning(f"Could not register bot commands via MTProto: {e}")

    if OWNER_ID:
        try:
            print(f"Pre-connecting Owner session for ID {OWNER_ID}...")
            await get_user_client(OWNER_ID)
        except Exception as e:
            logger.warning(f"Could not pre-connect owner session: {e}")

    print("\n" + "=" * 60)
    print(f"SUCCESS: Bot is now RUNNING!")
    print(f"Open Telegram and message your bot: @{bot_me.username}")
    print("Send /start to begin!")
    print("=" * 60 + "\n")

    await bot.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped by user.")
    finally:
        cleanup_temp_dir()
