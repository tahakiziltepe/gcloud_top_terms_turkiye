from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from dotenv import load_dotenv
from os import getenv
load_dotenv()

async def send_message(message:str) -> None:
    token = getenv("BOT_TOKEN")
    chat_id = getenv("CHAT_ID")
    message = message

    async with Bot(
        token=token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    ) as bot:
        await bot.send_message(chat_id=chat_id, text=message)