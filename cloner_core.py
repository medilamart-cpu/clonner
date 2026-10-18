import os
import asyncio
import logging
import random
import shutil
from typing import Optional, List, Dict, Callable
from telethon import TelegramClient, events, errors, utils
from telethon.tl import types, functions
from telethon.tl.functions.messages import (
    GetForumTopicsRequest,
    CreateForumTopicRequest,
    ForwardMessagesRequest,
)
from database import Database
from fast_telethon import fast_download_media, fast_upload_file

logger = logging.getLogger("TGClonerCore")
TEMP_DIR = "temp_downloads"

def ensure_temp_dir():
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR, exist_ok=True)

def cleanup_temp_dir():
    if os.path.exists(TEMP_DIR):
        try:
            shutil.rmtree(TEMP_DIR)
        except Exception:
            pass

def get_message_topic_id(message) -> Optional[int]:
    """Returns the forum topic ID (message_thread_id) of the message, or None if in General/No Topic."""
    if not message.reply_to:
        return None
    if getattr(message.reply_to, "reply_to_top_id", None):
        return message.reply_to.reply_to_top_id
    if getattr(message.reply_to, "forum_topic", False) or getattr(message.reply_to, "reply_to_msg_id", None):
        return message.reply_to.reply_to_msg_id
    return None

async def resolve_peer(client: TelegramClient, peer_str: str):
    """Resolves chat input (username, ID, or invite link) to a Telethon entity."""
    entity = None
    if peer_str.startswith("-100") or (peer_str.isdigit() or (peer_str.startswith("-") and peer_str[1:].isdigit())):
        try:
            entity = await client.get_entity(int(peer_str))
        except Exception:
            pass

    if entity is None:
        if "t.me/+" in peer_str or "joinchat" in peer_str:
            hash_part = peer_str.split("+")[-1] if "t.me/+" in peer_str else peer_str.split("/")[-1]
            try:
                from telethon.tl.functions.messages import ImportChatInviteRequest
                res = await client(ImportChatInviteRequest(hash=hash_part))
                entity = res.chats[0]
            except errors.UserAlreadyParticipantError:
                from telethon.tl.functions.messages import CheckChatInviteRequest
                res = await client(CheckChatInviteRequest(hash=hash_part))
                entity = getattr(res, "chat", None)
            except Exception:
                pass

    if entity is None:
        entity = await client.get_entity(peer_str)

    # Auto-resolve migrated supergroups: if channel only contains a migration action from a legacy chat
    if isinstance(entity, types.Channel):
        try:
            first_msgs = await client.get_messages(entity, limit=2)
            if len(first_msgs) == 1 and isinstance(first_msgs[0].action, types.MessageActionChannelMigrateFrom):
                legacy_id = -first_msgs[0].action.chat_id
                logger.info(f"Detected migrated supergroup {getattr(entity, 'id', None)}! Automatically redirecting to legacy chat: {legacy_id}")
                return await client.get_entity(legacy_id)
        except Exception as e:
            logger.debug(f"Migrated check exception: {e}")

    return entity


def extract_topic_id_from_updates(result) -> Optional[int]:
    if hasattr(result, "updates"):
        for u in result.updates:
            if isinstance(u, types.UpdateNewChannelMessage) and hasattr(u.message, "id"):
                return u.message.id
            if isinstance(u, types.UpdateNewMessage) and hasattr(u.message, "id"):
                return u.message.id
    if hasattr(result, "id"):
        return result.id
    return None

async def sync_forum_topics(client: TelegramClient, source_entity, dest_entity, db: Database, progress_cb: Optional[Callable] = None) -> Dict[int, int]:
    """Reads all topics from source forum and creates corresponding topics in destination forum."""
    dest_chat_id = utils.get_peer_id(dest_entity) if dest_entity else None
    topic_map = db.get_all_topic_mappings(dest_chat_id)

    dest_existing_topics = {}
    try:
        offset_id = 0
        offset_topic = 0
        while True:
            res = await client(GetForumTopicsRequest(
                peer=dest_entity,
                offset_date=None,
                offset_id=offset_id,
                offset_topic=offset_topic,
                limit=100
            ))
            for t in getattr(res, "topics", []):
                if isinstance(t, types.ForumTopic):
                    dest_existing_topics[t.title.strip().lower()] = t.id
            if len(getattr(res, "topics", [])) < 100:
                break
            last_topic = res.topics[-1]
            offset_topic = last_topic.id
            offset_id = getattr(last_topic, "top_message", 0)
    except Exception as e:
        logger.warning(f"Could not read destination topics: {e}")

    source_topics: List[types.ForumTopic] = []
    try:
        offset_id = 0
        offset_topic = 0
        while True:
            res = await client(GetForumTopicsRequest(
                peer=source_entity,
                offset_date=None,
                offset_id=offset_id,
                offset_topic=offset_topic,
                limit=100
            ))
            for t in getattr(res, "topics", []):
                if isinstance(t, types.ForumTopic):
                    source_topics.append(t)
            if len(getattr(res, "topics", [])) < 100:
                break
            last_topic = res.topics[-1]
            offset_topic = last_topic.id
            offset_id = getattr(last_topic, "top_message", 0)
    except Exception as e:
        logger.warning(f"Failed to fetch source topics: {e}")

    total = len(source_topics)
    for idx, s_topic in enumerate(source_topics, start=1):
        if s_topic.id in topic_map:
            continue

        normalized_title = s_topic.title.strip().lower()
        if normalized_title in dest_existing_topics:
            dest_id = dest_existing_topics[normalized_title]
            db.save_topic_mapping(s_topic.id, dest_id, s_topic.title, dest_chat_id)
            topic_map[s_topic.id] = dest_id
            continue

        try:
            if progress_cb:
                await progress_cb(f"Creating topic ({idx}/{total}): {s_topic.title}")
            res = await client(CreateForumTopicRequest(
                peer=dest_entity,
                title=s_topic.title,
                icon_color=getattr(s_topic, "icon_color", None),
                icon_emoji_id=getattr(s_topic, "icon_emoji_id", None),
                random_id=random.randint(1, 2**63 - 1)
            ))
            dest_id = extract_topic_id_from_updates(res)
            if dest_id:
                db.save_topic_mapping(s_topic.id, dest_id, s_topic.title, dest_chat_id)
                topic_map[s_topic.id] = dest_id
                dest_existing_topics[normalized_title] = dest_id
            await asyncio.sleep(0.3)
        except errors.FloodWaitError as fwe:
            if progress_cb:
                await progress_cb(f"Rate limited by Telegram. Waiting {fwe.seconds}s...")
            await asyncio.sleep(fwe.seconds + 2)
        except Exception as e:
            logger.error(f"Error creating topic '{s_topic.title}': {e}")

    return topic_map

async def ensure_topic_for_message(client: TelegramClient, source_entity, dest_entity, db: Database, source_topic_id: int) -> Optional[int]:
    dest_chat_id = utils.get_peer_id(dest_entity) if dest_entity else None
    dest_topic_id = db.get_dest_topic_id(source_topic_id, dest_chat_id)
    if dest_topic_id:
        return dest_topic_id

    topic_title = f"Topic {source_topic_id}"
    try:
        res = await client(functions.channels.GetMessagesRequest(
            channel=source_entity,
            id=[source_topic_id]
        ))
        if res.messages and hasattr(res.messages[0], "action") and isinstance(res.messages[0].action, types.MessageActionTopicCreate):
            topic_title = res.messages[0].action.title
    except Exception:
        pass

    try:
        res = await client(CreateForumTopicRequest(
            peer=dest_entity,
            title=topic_title,
            random_id=random.randint(1, 2**63 - 1)
        ))
        dest_topic_id = extract_topic_id_from_updates(res)
        if dest_topic_id:
            db.save_topic_mapping(source_topic_id, dest_topic_id, topic_title, dest_chat_id)
            return dest_topic_id
    except Exception as e:
        logger.error(f"Could not auto-create topic for ID {source_topic_id}: {e}")

    return None

async def forward_batch_messages(client: TelegramClient, messages: List, dest_entity, dest_topic_id: Optional[int]) -> bool:
    """Attempts to forward a batch of up to 100 messages in a single MTProto call."""
    if not messages:
        return True
    try:
        source_input = await client.get_input_entity(messages[0].chat_id)
        dest_input = await client.get_input_entity(dest_entity)
        msg_ids = [m.id for m in messages]
        random_ids = [random.randint(1, 2**63 - 1) for _ in messages]

        await client(ForwardMessagesRequest(
            from_peer=source_input,
            id=msg_ids,
            to_peer=dest_input,
            top_msg_id=dest_topic_id,
            random_id=random_ids
        ))
        return True
    except errors.ChatForwardsRestrictedError:
        return False
    except Exception as e:
        logger.warning(f"Batch forward not available: {e}")
        return False

def make_progress_cb(action_label: str, filename: str, status_updater: Optional[Callable]):
    """Creates a non-blocking progress callback that reports transfer speed and progress bar."""
    if not status_updater:
        return None
    start_time = asyncio.get_event_loop().time()
    last_edit = 0
    is_editing = False

    async def cb(current: int, total: int):
        nonlocal last_edit, is_editing
        now = asyncio.get_event_loop().time()
        if total <= 0:
            return
        # Throttle edits to at least 8 seconds to prevent Telegram EditMessageRequest FloodWait!
        if (now - last_edit < 8.0 and current < total) or is_editing:
            return
        last_edit = now

        elapsed = now - start_time
        speed = current / elapsed if elapsed > 0 else 0
        percent = min(100.0, (current / total) * 100)
        filled = int(percent // 10)
        bar = "█" * filled + "░" * (10 - filled)

        speed_str = f"{speed / (1024 * 1024):.1f} MB/s" if speed >= 1024 * 1024 else f"{speed / 1024:.1f} KB/s"
        cur_str = f"{current / (1024 * 1024):.1f} MB" if total >= 1024 * 1024 else f"{current / 1024:.1f} KB"
        tot_str = f"{total / (1024 * 1024):.1f} MB" if total >= 1024 * 1024 else f"{total / 1024:.1f} KB"
        eta = (total - current) / speed if speed > 0 else 0
        eta_str = f"{int(eta)}s" if eta < 60 else f"{int(eta // 60)}m {int(eta % 60)}s"

        text = (
            f"{action_label}: `{filename}`\n"
            f"[{bar}] {percent:.1f}%\n"
            f"⚡ Speed: `{speed_str}` | ETA: `{eta_str}`\n"
            f"💾 {cur_str} / {tot_str}"
        )

        async def do_edit():
            nonlocal is_editing
            is_editing = True
            try:
                await status_updater(text)
            except Exception:
                pass
            finally:
                is_editing = False

        # Fire in background: NEVER pause or block the download stream!
        asyncio.create_task(do_edit())

    return cb

async def send_single_message_fast(
    client: TelegramClient,
    message,
    dest_entity,
    dest_topic_id: Optional[int],
    status_updater: Optional[Callable] = None,
    custom_caption: Optional[str] = None
) -> Optional[int]:
    """
    High-speed message copy with playable video streaming (supports_streaming=True),
    zero-download shortcut, and FastTelethon parallel streamer with live speed progress.
    """
    final_text = custom_caption if custom_caption is not None else message.text
    formatting = None if custom_caption is not None else message.entities

    # 1. Text-only message
    if not message.media and final_text:
        sent = await client.send_message(
            dest_entity,
            final_text,
            formatting_entities=formatting,
            reply_to=dest_topic_id
        )
        return sent.id

    if message.media:
        if isinstance(message.media, types.MessageMediaWebPage):
            sent = await client.send_message(
                dest_entity,
                final_text,
                formatting_entities=formatting,
                reply_to=dest_topic_id,
                link_preview=True
            )
            return sent.id

        # Video metadata extraction for native in-app playable streaming
        is_video = False
        attributes = []
        thumb_path = None

        if hasattr(message, "document") and message.document:
            for attr in message.document.attributes or []:
                if isinstance(attr, types.DocumentAttributeVideo):
                    is_video = True
                    # Set supports_streaming=True so Telegram natively plays & streams the video!
                    attributes.append(types.DocumentAttributeVideo(
                        duration=attr.duration,
                        w=attr.w,
                        h=attr.h,
                        supports_streaming=True
                    ))
                elif not isinstance(attr, types.DocumentAttributeFilename):
                    attributes.append(attr)

        # 2. Zero-Download Instant Copy (Server-side reference duplication)
        try:
            sent = await client.send_file(
                dest_entity,
                message.media,
                caption=final_text,
                formatting_entities=formatting,
                reply_to=dest_topic_id,
                attributes=attributes if attributes else None,
                supports_streaming=is_video
            )
            return sent.id
        except Exception:
            pass  # Fall through to FastTelethon parallel streamer for restricted content

        # 3. High-Speed Parallel Transfer for Restricted Content
        ensure_temp_dir()
        filename = f"file_{message.id}"
        if hasattr(message.file, "name") and message.file.name:
            filename = message.file.name
        elif hasattr(message.file, "ext") and message.file.ext:
            filename += message.file.ext

        temp_name = f"temp_{message.id}_{random.randint(1000, 9999)}_{filename}"
        dl_path = os.path.join(TEMP_DIR, temp_name)

        # Download thumbnail to give the video its native player preview
        if is_video:
            try:
                thumb_name = f"thumb_{message.id}_{random.randint(1000, 9999)}.jpg"
                thumb_path = await client.download_media(message, file=os.path.join(TEMP_DIR, thumb_name), thumb=-1)
            except Exception:
                thumb_path = None

        try:
            # Parallel Multi-Connection Download (50-100 MB/s) with Live Speed Progress
            dl_cb = make_progress_cb("📥 Downloading", filename, status_updater)
            downloaded = await fast_download_media(client, message, dl_path, progress_callback=dl_cb)

            if downloaded and os.path.exists(downloaded):
                # If Telegram thumbnail wasn't downloaded or failed, generate a crisp thumbnail using ffmpeg directly from the downloaded video!
                if is_video and (not thumb_path or not os.path.exists(thumb_path)):
                    gen_thumb = os.path.join(TEMP_DIR, f"thumb_{message.id}_{random.randint(1000, 9999)}.jpg")
                    try:
                        proc = await asyncio.create_subprocess_exec(
                            "ffmpeg", "-y", "-ss", "00:00:01", "-i", downloaded,
                            "-vframes", "1", "-vf", "scale=320:-1", gen_thumb,
                            stdout=asyncio.subprocess.DEVNULL,
                            stderr=asyncio.subprocess.DEVNULL
                        )
                        await proc.wait()
                        if os.path.exists(gen_thumb) and os.path.getsize(gen_thumb) > 0:
                            thumb_path = gen_thumb
                    except Exception as e:
                        logger.warning(f"ffmpeg thumbnail generation failed: {e}")

                # Parallel Multi-Connection Upload with Live Speed Progress
                up_cb = make_progress_cb("📤 Uploading", filename, status_updater)
                input_file = await fast_upload_file(client, downloaded, filename=filename, progress_callback=up_cb)

                sent = await client.send_file(
                    dest_entity,
                    input_file,
                    caption=final_text,
                    formatting_entities=formatting,
                    reply_to=dest_topic_id,
                    attributes=attributes if attributes else None,
                    thumb=thumb_path,
                    supports_streaming=is_video
                )
                return sent.id
        except Exception as ex:
            logger.error(f"FastTelethon stream error on #{message.id}: {ex}")
            # Final fallback to standard download/upload
            dl = await client.download_media(message, file=TEMP_DIR)
            if dl:
                try:
                    if is_video and (not thumb_path or not os.path.exists(thumb_path)):
                        gen_thumb = os.path.join(TEMP_DIR, f"thumb_{message.id}_{random.randint(1000, 9999)}.jpg")
                        try:
                            proc = await asyncio.create_subprocess_exec(
                                "ffmpeg", "-y", "-ss", "00:00:01", "-i", dl,
                                "-vframes", "1", "-vf", "scale=320:-1", gen_thumb,
                                stdout=asyncio.subprocess.DEVNULL,
                                stderr=asyncio.subprocess.DEVNULL
                            )
                            await proc.wait()
                            if os.path.exists(gen_thumb) and os.path.getsize(gen_thumb) > 0:
                                thumb_path = gen_thumb
                        except Exception:
                            pass

                    sent = await client.send_file(
                        dest_entity,
                        dl,
                        caption=final_text,
                        formatting_entities=formatting,
                        reply_to=dest_topic_id,
                        attributes=attributes if attributes else None,
                        thumb=thumb_path,
                        supports_streaming=is_video
                    )
                    return sent.id
                finally:
                    if os.path.exists(dl):
                        os.remove(dl)
        finally:
            if os.path.exists(dl_path):
                try:
                    os.remove(dl_path)
                except Exception:
                    pass
            if thumb_path and os.path.exists(thumb_path):
                try:
                    os.remove(thumb_path)
                except Exception:
                    pass

    return None

async def send_album_fast(
    client: TelegramClient, 
    album_messages: List, 
    dest_entity, 
    dest_topic_id: Optional[int],
    custom_caption: Optional[str] = None
):
    """Sends album using parallel media downloads & uploads."""
    if not album_messages:
        return

    # Attempt forward if allowed (only if no custom caption requested)
    if custom_caption is None:
        fwd_success = await forward_batch_messages(client, album_messages, dest_entity, dest_topic_id)
        if fwd_success:
            return

    ensure_temp_dir()
    downloaded_files = []
    caption = custom_caption if custom_caption is not None else ""
    entities = None

    for m in album_messages:
        if m.text and not caption and custom_caption is None:
            caption = m.text
            entities = m.entities
        filename = f"album_{m.id}_{random.randint(1000, 9999)}"
        if hasattr(m.file, "name") and m.file.name:
            filename = f"album_{m.id}_" + m.file.name
        out_p = os.path.join(TEMP_DIR, filename)

        dl = await fast_download_media(client, m, out_p)
        if dl and os.path.exists(dl):
            downloaded_files.append(dl)

    if downloaded_files:
        try:
            await client.send_file(
                dest_entity,
                downloaded_files,
                caption=caption,
                formatting_entities=entities,
                reply_to=dest_topic_id
            )
        finally:
            for p in downloaded_files:
                if os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass
