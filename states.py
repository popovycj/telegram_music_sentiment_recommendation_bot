from aiogram.fsm.state import State, StatesGroup
from aiogram import types
from aiogram.fsm.context import FSMContext

class QuestionnaireState(StatesGroup):
    IN_QUESTIONNAIRE = State()

    @classmethod
    async def is_valid_answer(
        cls,
        message: types.Message,
        state: FSMContext
    ) -> bool:
        """
        Return True if:
        1. we're in IN_QUESTIONNAIRE state, AND
        2. message.text is one of the allowed answers for the current question.
        """
        # 1) are we in the questionnaire?
        if await state.get_state() != cls.IN_QUESTIONNAIRE.state:
            return False

        # 2) fetch data and valid answers
        data = await state.get_data()
        questions = data.get("questions", {})
        idx       = data.get("current_index", 0)

        try:
            valid = list(questions.values())[idx]["answers"]
        except (IndexError, KeyError):
            return False

        return message.text in valid
