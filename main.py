import os
import yaml
from contextvars import ContextVar

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.types import BotCommand
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import ReplyKeyboardBuilder

from database import create_pool, check_table_exists, save_user


load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

SUPPORTED_LANGS = ['en', 'uk']

class QuestionnaireState(StatesGroup):
    IN_QUESTIONNAIRE = State()

db_pool = ContextVar("db_pool")

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

def get_questions(locale: str) -> dict:
    """Load questions from YAML and auto-detect question count"""
    with open(f'config/i18n/{locale}.yml') as f:
        translations = yaml.safe_load(f)

    # Auto-detect questions (keys starting with q + number)
    questions = {k: v for k, v in translations.items() if k.startswith('q') and k[1:].isdigit()}
    # Sort questions by their numeric suffix
    return dict(sorted(questions.items(), key=lambda item: int(item[0][1:])))

async def start_handler(message: types.Message, state: FSMContext):
    # Detect language with fallback to English
    user_lang = message.from_user.language_code
    locale = 'uk' if user_lang == 'uk' else 'en'

    if locale not in SUPPORTED_LANGS:
        locale = 'en'

    pool = db_pool.get()
    await save_user(message.from_user.id, pool)

    questions = get_questions(locale)

    with open(f'config/i18n/{locale}.yml') as f:
        translations = yaml.safe_load(f)
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

def setup_handlers():
    dp.message.register(start_handler, Command("start"))
    dp.message.register(handle_answer, QuestionnaireState.IN_QUESTIONNAIRE)

async def on_startup(bot: Bot):
    pool = await create_pool()
    await check_table_exists(pool)
    db_pool.set(pool)
    await bot.set_my_commands([
        BotCommand(command="start", description="Start questionnaire")
    ])

async def on_shutdown(bot: Bot):
    pool = db_pool.get()
    await pool.close()

async def main():
    setup_handlers()
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
