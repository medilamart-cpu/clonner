import os
import sys
import asyncio
import logging
from dotenv import load_dotenv

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
        sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        pass


from telethon import TelegramClient, events, errors
from database import Database
from cloner_core import (
    resolve_peer,
    get_message_topic_id,
    sync_forum_topics,
    ensure_topic_for_message,
    send_single_message,
    send_album,
    ensure_temp_dir,
    cleanup_temp_dir
)

logging.basicConfig(
    format="[%(asctime)s] %(levelname)s: %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("TGClonerCLI")

load_dotenv("config.env")
load_dotenv(".env")

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")
SOURCE_CHAT = os.getenv("SOURCE_CHAT")
DEST_CHAT = os.getenv("DEST_CHAT")
CLONE_HISTORY = os.getenv("CLONE_HISTORY", "True").lower() in ("true", "1", "yes")
LIVE_FORWARD = os.getenv("LIVE_FORWARD", "True").lower() in ("true", "1", "yes")
FORWARD_MODE = os.getenv("FORWARD_MODE", "copy").lower().strip()
DELAY_BETWEEN_MESSAGES = float(os.getenv("DELAY_BETWEEN_MESSAGES", "1.2"))


async def clone_history(client: TelegramClient, source_entity, dest_entity, db: Database):
    logger.info("Starting past history cloning...")
    album_buffer = []
    current_grouped_id = None
    current_topic_id = None

    async def flush_album():
        nonlocal album_buffer, current_grouped_id, current_topic_id
        if not album_buffer:
            return
        dest_topic = None
        if current_topic_id:
            dest_topic = await ensure_topic_for_message(client, source_entity, dest_entity, db, current_topic_id)
        
        while True:
            try:
                await send_album(client, album_buffer, dest_entity, dest_topic, FORWARD_MODE)
                for m in album_buffer:
                    db.record_cloned_message(m.id, None, current_topic_id)
                logger.info(f"Cloned album with {len(album_buffer)} items into topic {dest_topic or 'General'}")
                await asyncio.sleep(DELAY_BETWEEN_MESSAGES)
                break
            except errors.FloodWaitError as fwe:
                logger.warning(f"FloodWait reached. Waiting {fwe.seconds} seconds...")
                await asyncio.sleep(fwe.seconds + 2)
            except Exception as e:
                logger.error(f"Error cloning album: {e}")
                break
        album_buffer = []
        current_grouped_id = None
        current_topic_id = None

    cloned_count = 0
    skipped_count = 0

    async for msg in client.iter_messages(source_entity, reverse=True):
        if msg.action:
            skipped_count += 1
            continue

        if db.is_message_cloned(msg.id):
            skipped_count += 1
            continue

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

        source_topic_id = get_message_topic_id(msg)
        dest_topic_id = None
        if source_topic_id:
            dest_topic_id = await ensure_topic_for_message(client, source_entity, dest_entity, db, source_topic_id)

        while True:
            try:
                dest_msg_id = await send_single_message(client, msg, dest_entity, dest_topic_id, FORWARD_MODE)
                db.record_cloned_message(msg.id, dest_msg_id, source_topic_id)
                cloned_count += 1
                if cloned_count % 10 == 0:
                    logger.info(f"Cloned {cloned_count} messages so far (Topic: {dest_topic_id or 'General'})...")
                await asyncio.sleep(DELAY_BETWEEN_MESSAGES)
                break
            except errors.FloodWaitError as fwe:
                logger.warning(f"Telegram FloodWait hit! Sleeping {fwe.seconds}s...")
                await asyncio.sleep(fwe.seconds + 2)
            except Exception as e:
                logger.error(f"Error copying message {msg.id}: {e}")
                db.record_cloned_message(msg.id, None, source_topic_id)
                break

    if album_buffer:
        await flush_album()

    logger.info(f"History cloning complete! Total cloned: {cloned_count}, Skipped: {skipped_count}")


def register_live_listener(client: TelegramClient, source_entity, dest_entity, db: Database):
    logger.info("Setting up real-time live forwarder...")

    @client.on(events.NewMessage(chats=source_entity))
    async def handler(event):
        msg = event.message
        if msg.action or db.is_message_cloned(msg.id):
            return

        source_topic_id = get_message_topic_id(msg)
        dest_topic_id = None
        if source_topic_id:
            dest_topic_id = await ensure_topic_for_message(client, source_entity, dest_entity, db, source_topic_id)

        try:
            dest_msg_id = await send_single_message(client, msg, dest_entity, dest_topic_id, FORWARD_MODE)
            db.record_cloned_message(msg.id, dest_msg_id, source_topic_id)
            logger.info(f"[LIVE] Forwarded new message #{msg.id} -> Topic {dest_topic_id or 'General'}")
        except errors.FloodWaitError as fwe:
            logger.warning(f"[LIVE] FloodWait on live message. Sleeping {fwe.seconds}s...")
            await asyncio.sleep(fwe.seconds + 2)
        except Exception as e:
            logger.error(f"[LIVE] Failed to forward message #{msg.id}: {e}")

    logger.info("Live forwarder active! Listening for new incoming messages...")


async def main():
    print("""
╔═══════════════════════════════════════════════════════════════╗
║         TELEGRAM FORUM TOPIC CLONER (TERMINAL CLI)            ║
╚═══════════════════════════════════════════════════════════════╝
    """)
    ensure_temp_dir()
    db = Database()

    api_id_val = API_ID or input("Enter your API_ID: ").strip()
    api_hash_val = API_HASH or input("Enter your API_HASH: ").strip()
    source_val = SOURCE_CHAT or input("Enter SOURCE chat (@username, link, or ID): ").strip()
    dest_val = DEST_CHAT or input("Enter DESTINATION forum chat (@username, link, or ID): ").strip()

    client = TelegramClient("cloner_user_session", int(api_id_val), api_hash_val)

    logger.info("Connecting to Telegram...")
    await client.start(phone=PHONE_NUMBER if PHONE_NUMBER else None)
    me = await client.get_me()
    logger.info(f"Logged in as: {me.first_name} (@{me.username or me.id})")

    source_entity = await resolve_peer(client, source_val)
    dest_entity = await resolve_peer(client, dest_val)

    await sync_forum_topics(client, source_entity, dest_entity, db)

    if CLONE_HISTORY:
        await clone_history(client, source_entity, dest_entity, db)

    if LIVE_FORWARD:
        register_live_listener(client, source_entity, dest_entity, db)
        logger.info("Cloner running in Live Mode. Press Ctrl+C to exit.")
        await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Process stopped.")
    finally:
        cleanup_temp_dir()
