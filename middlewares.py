from contextvars import ContextVar

import redis.asyncio as aioredis
from aiogram import types
from aiogram.fsm.context import FSMContext
from aiogram.dispatcher.middlewares.base import BaseMiddleware

import config
from states import QuestionnaireState
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
        if not isinstance(event, types.Message):
            return await handler(event, data)

        key   = f"rate_limit:{event.from_user.id}"
        redis = redis_pool.get()

        locale       = detect_user_locale(event)
        translations = get_translations(locale)

        current = int(await redis.get(key) or 0)
        if current >= self.max_requests:
            ttl  = await redis.ttl(key)
            await event.answer(translations['rate_limit_message'].format(seconds=ttl))
            return

        # skip throttle if this is a valid questionnaire answer
        state: FSMContext = data.get("state")
        if await QuestionnaireState.is_valid_answer(event, state):
            await redis.delete(key)
            return await handler(event, data)

        current = await redis.incr(key)
        if current == 1:
            await redis.expire(key, self.window)
        return await handler(event, data)
