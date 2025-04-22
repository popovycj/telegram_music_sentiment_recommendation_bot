from aiogram import Bot, Dispatcher, types
from aiogram.types import BotCommand
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import ReplyKeyboardBuilder

import config
from states import QuestionnaireState
from database import create_db_pool, check_table_exists, save_user, db_pool
from utils import detect_user_locale, get_translations, get_questions
from middlewares import RateLimitMiddleware, create_redis_pool, redis_pool


bot = Bot(token=config.TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

async def start_handler(message: types.Message, state: FSMContext):
    pool = db_pool.get()
    await save_user(message.from_user.id, pool)

    locale = detect_user_locale(message)
    translations = get_translations(locale)

    questions = get_questions(translations)
    thank_you_answer = translations.get('thank_you_answer')
    invalid_answer = translations.get('invalid_answer')

    await state.update_data(
        locale=locale,
        answers=[],
        questions=questions,
        current_index=0,
        total_questions=len(questions),
        thank_you_answer=thank_you_answer,
        invalid_answer=invalid_answer
    )

    await ask_question(message, state)

async def ask_question(message: types.Message, state: FSMContext):
    data = await state.get_data()
    questions = data['questions']
    current_index = data['current_index']

    question_key = list(questions.keys())[current_index]
    question = questions[question_key]

    builder = ReplyKeyboardBuilder()
    for answer in question['answers']:
        builder.add(types.KeyboardButton(text=answer))
    builder.adjust(1)

    await message.answer(
        question['text'],
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(QuestionnaireState.IN_QUESTIONNAIRE)

async def handle_answer(message: types.Message, state: FSMContext):
    data = await state.get_data()
    current_index = data['current_index']
    invalid_answer = data['invalid_answer']
    thank_you_answer = data['thank_you_answer']

    questions = data['questions']
    question_key = list(questions.keys())[current_index]
    valid_answers = questions[question_key]['answers']

    if message.text not in valid_answers:
        await message.answer(invalid_answer)
        return

    data['answers'].append(message.text)
    data['current_index'] += 1
    await state.update_data(data)

    if data['current_index'] >= data['total_questions']:
        await message.answer(
            thank_you_answer,
            reply_markup=types.ReplyKeyboardRemove()
        )
        print(f"User answers: {data['answers']}")
        await state.clear()
        return

    await ask_question(message, state)

async def default_handler(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state != QuestionnaireState.IN_QUESTIONNAIRE:
        locale = detect_user_locale(message)
        translations = get_translations(locale)
        await message.answer(translations.get('entry_message'))

def setup_handlers():
    dp.message.register(start_handler, Command("start"))
    dp.message.register(handle_answer, QuestionnaireState.IN_QUESTIONNAIRE)
    dp.message.register(default_handler, lambda message: True)

async def on_startup(bot: Bot):
    pool = await create_db_pool()
    await check_table_exists(pool)
    db_pool.set(pool)

    r = await create_redis_pool()
    redis_pool.set(r)

    dp.message.middleware(RateLimitMiddleware())

    await bot.set_my_commands([
        BotCommand(command="start", description="Start questionnaire"),
    ])

async def on_shutdown(bot: Bot):
    await db_pool.get().close()
    await redis_pool.get().close()

async def main():
    setup_handlers()
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
