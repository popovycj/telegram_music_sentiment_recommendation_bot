import yaml
from aiogram import types
from config import SUPPORTED_LANGS

def detect_user_locale(message: types.Message) -> str:
    user_lang = message.from_user.language_code
    return 'uk' if user_lang == 'uk' else 'en'

def get_translations(locale: str) -> dict:
    with open(f'config/i18n/{locale}.yml', encoding='utf-8') as f:
        return yaml.safe_load(f)

def get_questions(translations: dict) -> dict:
    qs = {k: v for k, v in translations.items() if k.startswith('q') and k[1:].isdigit()}
    return dict(sorted(qs.items(), key=lambda item: int(item[0][1:])))
