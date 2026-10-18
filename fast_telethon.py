"""
FastTelethon - Parallel multi-connection file transfer for Telethon.
Enables 50-100 MB/s parallel chunked download & upload for restricted Telegram media.
"""
import asyncio
import hashlib
import inspect
import logging
import math
import os
from collections import defaultdict
from typing import (
    AsyncGenerator,
    Awaitable,
    BinaryIO,
    Callable,
    DefaultDict,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

from telethon import TelegramClient, helpers, utils
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.auth import (
    ExportAuthorizationRequest,
    ImportAuthorizationRequest,
)
from telethon.tl.functions.upload import (
    GetFileRequest,
    SaveBigFilePartRequest,
    SaveFilePartRequest,
)
from telethon.tl.types import (
    Document,
    InputDocumentFileLocation,
    InputFile,
    InputFileBig,
    InputFileLocation,
    InputPeerPhotoFileLocation,
    InputPhotoFileLocation,
    TypeInputFile,
)

log: logging.Logger = logging.getLogger("FastTelethon")

TypeLocation = Union[
    Document,
    InputDocumentFileLocation,
    InputPeerPhotoFileLocation,
    InputFileLocation,
    InputPhotoFileLocation,
]


class DownloadSender:
    client: TelegramClient
    sender: MTProtoSender
    request: GetFileRequest
    remaining: int
    stride: int

    def __init__(
        self,
        client: TelegramClient,
        sender: MTProtoSender,
        file: TypeLocation,
        offset: int,
        limit: int,
        stride: int,
        count: int,
    ) -> None:
        self.sender = sender
        self.client = client
        self.request = GetFileRequest(file, offset=offset, limit=limit)
        self.stride = stride
        self.remaining = count

    async def next(self) -> Optional[bytes]:
        if not self.remaining:
            return None
        result = await self.client._call(self.sender, self.request)
        self.remaining -= 1
        self.request.offset += self.stride
        return result.bytes

    def disconnect(self) -> Awaitable[None]:
        return self.sender.disconnect()


class UploadSender:
    client: TelegramClient
    sender: MTProtoSender
    request: Union[SaveFilePartRequest, SaveBigFilePartRequest]
    part_count: int
    stride: int
    previous: Optional[asyncio.Task]
    loop: asyncio.AbstractEventLoop

    def __init__(
        self,
        client: TelegramClient,
        sender: MTProtoSender,
        file_id: int,
        part_count: int,
        big: bool,
        index: int,
        stride: int,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self.client = client
        self.sender = sender
        self.part_count = part_count
        if big:
            self.request = SaveBigFilePartRequest(file_id, index, part_count, b"")
        else:
            self.request = SaveFilePartRequest(file_id, index, b"")
        self.stride = stride
        self.previous = None
        self.loop = loop

    async def next(self, data: bytes) -> None:
        if self.previous:
            await self.previous
        self.previous = self.loop.create_task(self._next(data))

    async def _next(self, data: bytes) -> None:
        self.request.bytes = data
        await self.client._call(self.sender, self.request)
        self.request.file_part += self.stride

    async def disconnect(self) -> None:
        if self.previous:
            await self.previous
        return await self.sender.disconnect()


class ParallelTransferrer:
    client: TelegramClient
    loop: asyncio.AbstractEventLoop
    dc_id: int
    senders: Optional[List[Union[DownloadSender, UploadSender]]]
    auth_key: AuthKey
    upload_ticker: int

    def __init__(self, client: TelegramClient, dc_id: Optional[int] = None) -> None:
        self.client = client
        self.loop = self.client.loop
        self.dc_id = dc_id or self.client.session.dc_id
        self.auth_key = (
            None
            if dc_id and self.client.session.dc_id != dc_id
            else self.client.session.auth_key
        )
        self.senders = None
        self.upload_ticker = 0

    async def _cleanup(self) -> None:
        if self.senders:
            await asyncio.gather(*[sender.disconnect() for sender in self.senders])
            self.senders = None

    @staticmethod
    def _get_connection_count(
        file_size: int, max_count: int = 6, full_size: int = 30 * 1024 * 1024
    ) -> int:
        if file_size > full_size:
            return max_count
        return max(2, min(max_count, math.ceil((file_size / full_size) * max_count)))

    async def _init_download(
        self, connections: int, file: TypeLocation, part_count: int, part_size: int
    ) -> None:
        pass

    async def _create_download_sender(
        self,
        file: TypeLocation,
        index: int,
        part_size: int,
        stride: int,
        part_count: int,
    ) -> DownloadSender:
        return DownloadSender(
            self.client,
            await self._create_sender(),
            file,
            index * part_size,
            part_size,
            stride,
            part_count,
        )

    async def _init_upload(
        self, connections: int, file_id: int, part_count: int, big: bool
    ) -> None:
        first_sender = await self._create_upload_sender(file_id, part_count, big, 0, connections)
        if connections > 1:
            other_senders = await asyncio.gather(
                *[
                    self._create_upload_sender(file_id, part_count, big, i, connections)
                    for i in range(1, connections)
                ]
            )
            self.senders = [first_sender, *other_senders]
        else:
            self.senders = [first_sender]

    async def _create_upload_sender(
        self, file_id: int, part_count: int, big: bool, index: int, stride: int
    ) -> UploadSender:
        return UploadSender(
            self.client,
            await self._create_sender(),
            file_id,
            part_count,
            big,
            index,
            stride,
            loop=self.loop,
        )

    async def _create_sender(self) -> MTProtoSender:
        dc = await self.client._get_dc(self.dc_id)
        sender = MTProtoSender(self.auth_key, loggers=self.client._log)
        await sender.connect(
            self.client._connection(
                dc.ip_address,
                dc.port,
                dc.id,
                loggers=self.client._log,
                proxy=self.client._proxy,
            )
        )
        if not self.auth_key:
            auth = await self.client(ExportAuthorizationRequest(self.dc_id))
            self.client._init_request.query = ImportAuthorizationRequest(
                id=auth.id, bytes=auth.bytes
            )
            req = InvokeWithLayerRequest(LAYER, self.client._init_request)
            await sender.send(req)
            self.auth_key = sender.auth_key
        return sender

    async def init_upload(
        self,
        file_id: int,
        file_size: int,
        part_size_kb: Optional[float] = None,
        connection_count: Optional[int] = None,
    ) -> Tuple[int, int, bool]:
        connection_count = connection_count or self._get_connection_count(file_size)
        part_size = 512 * 1024  # Force maximum 512KB chunks for max throughput
        part_count = (file_size + part_size - 1) // part_size
        is_large = file_size > 10 * 1024 * 1024
        await self._init_upload(connection_count, file_id, part_count, is_large)
        return part_size, part_count, is_large

    async def upload(self, part: bytes) -> None:
        await self.senders[self.upload_ticker].next(part)
        self.upload_ticker = (self.upload_ticker + 1) % len(self.senders)

    async def finish_upload(self) -> None:
        await self._cleanup()

    async def download_to_file(
        self,
        file: TypeLocation,
        file_size: int,
        out_path: str,
        progress_callback: Optional[Callable] = None,
        connection_count: Optional[int] = None,
        refresh_media_cb: Optional[Callable] = None,
    ) -> str:
        connection_count = connection_count or self._get_connection_count(file_size)
        part_size = 512 * 1024  # Force maximum 512KB chunks for max throughput
        part_count = math.ceil(file_size / part_size)

        # Connect first sender to authenticate with DC and establish self.auth_key
        first_sender = await self._create_sender()
        if connection_count > 1:
            other_senders = await asyncio.gather(
                *[self._create_sender() for _ in range(connection_count - 1)]
            )
            self.senders = [first_sender, *other_senders]
        else:
            self.senders = [first_sender]

        queue: asyncio.Queue = asyncio.Queue()
        for i in range(part_count):
            queue.put_nowait(i)

        results: Dict[int, bytes] = {}
        events: Dict[int, asyncio.Event] = {i: asyncio.Event() for i in range(part_count)}
        max_buffered = asyncio.Semaphore(connection_count + 8)
        current_file = file
        refresh_lock = asyncio.Lock()

        async def refresh_location():
            nonlocal current_file
            async with refresh_lock:
                if refresh_media_cb:
                    try:
                        fresh = await refresh_media_cb()
                        if fresh:
                            current_file = fresh
                            return fresh
                    except Exception as ex:
                        log.warning(f"Could not refresh file location: {ex}")
            return current_file

        async def worker(sender: MTProtoSender):
            while True:
                try:
                    part_idx = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                await max_buffered.acquire()

                retries = 3
                chunk_data = None
                for attempt in range(retries):
                    try:
                        req = GetFileRequest(current_file, offset=part_idx * part_size, limit=part_size)
                        res = await self.client._call(sender, req)
                        chunk_data = res.bytes
                        break
                    except Exception as e:
                        err_str = str(e).lower()
                        if "file reference" in err_str or "expired" in err_str:
                            log.warning(f"File reference expired on part {part_idx}. Refreshing...")
                            await refresh_location()
                        elif attempt == retries - 1:
                            log.error(f"Error fetching part {part_idx} after {retries} attempts: {e}")
                            raise
                        await asyncio.sleep(0.5)

                results[part_idx] = chunk_data or b""
                events[part_idx].set()
                queue.task_done()

        async def staggered_worker(idx: int, sender: MTProtoSender):
            if idx > 0:
                await asyncio.sleep(idx * 0.05)
            await worker(sender)

        worker_tasks = [self.loop.create_task(staggered_worker(i, s)) for i, s in enumerate(self.senders)]

        downloaded_bytes = 0
        try:
            with open(out_path, "wb") as f:
                for part_idx in range(part_count):
                    await events[part_idx].wait()
                    chunk = results.pop(part_idx, b"")
                    if chunk:
                        f.write(chunk)
                        downloaded_bytes += len(chunk)
                    max_buffered.release()

                    if progress_callback:
                        res = progress_callback(downloaded_bytes, file_size)
                        if inspect.isawaitable(res):
                            await res

            await asyncio.gather(*worker_tasks)
            return out_path
        finally:
            await self._cleanup()


def stream_file(file_to_stream: BinaryIO, chunk_size=512 * 1024):
    while True:
        data_read = file_to_stream.read(chunk_size)
        if not data_read:
            break
        yield data_read


async def fast_upload_file(
    client: TelegramClient,
    file_path: str,
    filename: Optional[str] = None,
    progress_callback: Optional[Callable] = None
) -> TypeInputFile:
    """Uploads a file using parallel MTProto connections at max speed with progress reporting."""
    file_id = helpers.generate_random_long()
    file_size = os.path.getsize(file_path)
    if filename is None:
        filename = os.path.basename(file_path)

    part_size = 512 * 1024
    part_count = (file_size + part_size - 1) // part_size
    is_large = file_size > 10 * 1024 * 1024

    uploader = ParallelTransferrer(client)
    connection_count = uploader._get_connection_count(file_size, max_count=16)

    # Establish connections to DC
    first_sender = await uploader._create_sender()
    if connection_count > 1:
        other_senders = await asyncio.gather(
            *[uploader._create_sender() for _ in range(connection_count - 1)]
        )
        senders = [first_sender, *other_senders]
    else:
        senders = [first_sender]
    uploader.senders = senders

    # Pre-calculate MD5 if small file
    hash_md5 = hashlib.md5() if not is_large else None
    if not is_large:
        with open(file_path, "rb") as f:
            while chunk := f.read(part_size):
                hash_md5.update(chunk)

    queue: asyncio.Queue = asyncio.Queue()
    for i in range(part_count):
        queue.put_nowait(i)

    uploaded_parts = 0
    uploaded_lock = asyncio.Lock()

    async def upload_worker(sender: MTProtoSender):
        nonlocal uploaded_parts
        with open(file_path, "rb") as f:
            while True:
                try:
                    part_idx = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                offset = part_idx * part_size
                f.seek(offset)
                chunk = f.read(part_size)

                retries = 3
                for attempt in range(retries):
                    try:
                        if is_large:
                            req = SaveBigFilePartRequest(file_id, part_idx, part_count, chunk)
                        else:
                            req = SaveFilePartRequest(file_id, part_idx, chunk)
                        await client._call(sender, req)
                        break
                    except Exception as e:
                        if attempt == retries - 1:
                            log.error(f"Failed to upload part {part_idx} after {retries} attempts: {e}")
                            raise
                        await asyncio.sleep(0.5)

                async with uploaded_lock:
                    uploaded_parts += 1
                    current_bytes = min(file_size, uploaded_parts * part_size)
                    if progress_callback:
                        res = progress_callback(current_bytes, file_size)
                        if inspect.isawaitable(res):
                            await res

                queue.task_done()

    async def staggered_up_worker(idx: int, sender: MTProtoSender):
        if idx > 0:
            await asyncio.sleep(idx * 0.05)
        await upload_worker(sender)

    worker_tasks = [asyncio.create_task(staggered_up_worker(i, s)) for i, s in enumerate(senders)]

    try:
        await asyncio.gather(*worker_tasks)
    finally:
        await uploader._cleanup()

    if is_large:
        return InputFileBig(file_id, part_count, filename)
    else:
        return InputFile(file_id, part_count, filename, hash_md5.hexdigest())


async def fast_download_media(
    client: TelegramClient,
    message,
    out_path: str,
    progress_callback: Optional[Callable] = None
) -> Optional[str]:
    """Downloads media from message using parallel connections at max speed with progress reporting."""
    media = message.media
    if not media:
        return None

    doc = getattr(media, "document", None)
    photo = getattr(media, "photo", None)
    location = None
    size = 0

    if doc:
        size = doc.size
        location = doc
    elif photo:
        size = 0
        location = photo
    else:
        return await client.download_media(message, file=out_path, progress_callback=progress_callback)

    async def refresh_media():
        try:
            fresh_msg = await client.get_messages(message.chat_id, ids=message.id)
            if fresh_msg and fresh_msg.media:
                _, fresh_loc = utils.get_input_location(fresh_msg.media.document or fresh_msg.media)
                return fresh_loc
        except Exception as e:
            log.warning(f"Failed to refresh message #{message.id}: {e}")
        return None

    try:
        dc_id, input_location = utils.get_input_location(location)
        if not size:
            return await client.download_media(message, file=out_path, progress_callback=progress_callback)

        downloader = ParallelTransferrer(client, dc_id)
        return await downloader.download_to_file(
            input_location,
            size,
            out_path,
            progress_callback=progress_callback,
            refresh_media_cb=refresh_media
        )
    except Exception as e:
        log.warning(f"Fast download failed ({e}), falling back to standard download.")
        return await client.download_media(message, file=out_path, progress_callback=progress_callback)
