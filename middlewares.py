from contextvars import ContextVar

import redis.asyncio as aioredis
from aiogram import types
from aiogram.dispatcher.middlewares.base import BaseMiddleware

import config
from utils import detect_user_locale, get_translations


redis_pool = ContextVar("redis_pool")

def create_redis_pool():
    return aioredis.from_url(
        config.REDIS_URL,
        decode_responses=True,
        encoding="utf-8",
    )

class RateLimitMiddleware(BaseMiddleware):
    def __init__(self):
        super().__init__()
        self.max_requests = config.MAX_REQUESTS
        self.window       = config.TIME_WINDOW

    async def __call__(self, handler, event, data):
        if isinstance(event, types.Message):
            redis = redis_pool.get()
            key   = f"rate_limit:{event.from_user.id}"

            current = await redis.incr(key)
            if current == 1:
                await redis.expire(key, self.window)

            if current > self.max_requests:
                locale       = detect_user_locale(event)
                translations = get_translations(locale)
                ttl          = await redis.ttl(key)
                await event.answer(translations['rate_limit_message'].format(seconds=ttl))
                return  # drop the actual handler call

        return await handler(event, data)
