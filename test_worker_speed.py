import asyncio
import time
import os
import math
from telethon import TelegramClient, utils
from telethon.tl.functions.upload import GetFileRequest
from dotenv import load_dotenv

load_dotenv('/home/ubuntu/telegram-transfer-bot/config.env')
api_id = int(os.getenv('API_ID'))
api_hash = os.getenv('API_HASH')

async def main():
    client = TelegramClient('/home/ubuntu/telegram-transfer-bot/user_session', api_id, api_hash)
    await client.connect()
    
    dialogs = await client.get_dialogs(limit=50)
    target = None
    for d in dialogs:
        if 'prep' in (d.title or '').lower():
            target = d.entity
            break
            
    if not target:
        print("Target not found")
        await client.disconnect()
        return

    msg = None
    async for m in client.iter_messages(target, limit=100):
        if m.media and hasattr(m.media, 'document') and m.media.document and m.media.document.size > 50*1024*1024:
            msg = m
            break
            
    if not msg:
        print("Media message not found")
        await client.disconnect()
        return

    doc = msg.media.document
    file_size = doc.size
    print(f"Testing on file: {file_size / (1024*1024):.1f} MB (ID: {doc.id})")
    
    dc_id, input_location = utils.get_input_location(doc)
    part_size = 512 * 1024
    part_count = math.ceil(file_size / part_size)
    
    from fast_telethon import ParallelTransferrer
    
    for num_conns in [12, 16, 20]:
        print(f"\n--- Testing with {num_conns} parallel workers ---")
        pt = ParallelTransferrer(client, dc_id)
        
        t_init = time.time()
        senders = await asyncio.gather(*[pt._create_sender() for _ in range(num_conns)])
        print(f"Connected {num_conns} senders in {time.time() - t_init:.2f}s")
        
        queue = asyncio.Queue()
        for i in range(part_count):
            queue.put_nowait(i)
            
        results = {}
        events = {i: asyncio.Event() for i in range(part_count)}
        max_buffered = asyncio.Semaphore(num_conns + 8)
        
        async def worker(sender):
            while True:
                try:
                    part_idx = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                await max_buffered.acquire()
                req = GetFileRequest(input_location, offset=part_idx * part_size, limit=part_size)
                res = await client._call(sender, req)
                results[part_idx] = res.bytes
                events[part_idx].set()
                queue.task_done()
                
        workers = [asyncio.create_task(worker(s)) for s in senders]
        
        t0 = time.time()
        bytes_received = 0
        out_file = f"/tmp/bench_{num_conns}.bin"
        
        with open(out_file, "wb") as f:
            for part_idx in range(part_count):
                await events[part_idx].wait()
                chunk = results.pop(part_idx)
                f.write(chunk)
                max_buffered.release()
                bytes_received += len(chunk)
                
        await asyncio.gather(*workers)
        t_total = time.time() - t0
        speed_mb = (bytes_received / (1024 * 1024)) / t_total
        print(f"Downloaded {bytes_received / (1024*1024):.1f} MB in {t_total:.2f}s -> SPEED: {speed_mb:.2f} MB/s ({speed_mb * 8:.1f} Mbps)")
        
        # Cleanup
        await asyncio.gather(*[s.disconnect() for s in senders])
        if os.path.exists(out_file):
            os.remove(out_file)

    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
