
import asyncio

async def keep_alive():
	while True:
		print("im alive")
		await asyncio.sleep(5)
