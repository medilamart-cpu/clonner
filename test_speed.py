import asyncio
import time
import os
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv('/home/ubuntu/telegram-transfer-bot/config.env')
api_id = int(os.getenv('API_ID'))
api_hash = os.getenv('API_HASH')

async def benchmark():
    client = TelegramClient('/home/ubuntu/telegram-transfer-bot/user_session', api_id, api_hash)
    await client.connect()
    
    test_file = "test_50mb.bin"
    with open(test_file, "wb") as f:
        f.write(os.urandom(50 * 1024 * 1024))
    
    from fast_telethon import ParallelTransferrer, stream_file
    from telethon import helpers
    
    for conn_count in [4, 8, 16]:
        print(f"\n--- Testing upload with {conn_count} parallel connections ---")
        file_id = helpers.generate_random_long()
        file_size = os.path.getsize(test_file)
        uploader = ParallelTransferrer(client)
        part_size = 512 * 1024
        part_count = (file_size + part_size - 1) // part_size
        
        t0 = time.time()
        await uploader._init_upload(conn_count, file_id, part_count, True)
        t_init = time.time() - t0
        print(f"Init {conn_count} connections: {t_init:.2f}s")
        
        t_start = time.time()
        with open(test_file, "rb") as f:
            for data in stream_file(f, chunk_size=part_size):
                await uploader.upload(data)
        await uploader.finish_upload()
        t_transfer = time.time() - t_start
        speed = 50.0 / t_transfer
        print(f"Uploaded 50MB in {t_transfer:.2f}s -> Transfer Speed: {speed:.2f} MB/s ({speed * 8:.1f} Mbps)")

    if os.path.exists(test_file):
        os.remove(test_file)
    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(benchmark())
