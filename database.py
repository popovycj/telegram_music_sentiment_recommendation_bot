import os
from contextvars import ContextVar

import asyncpg
from dotenv import load_dotenv

load_dotenv()

db_pool = ContextVar("db_pool")

async def create_db_pool():
    return await asyncpg.create_pool(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

async def check_table_exists(pool):
    async with pool.acquire() as conn:
        exists = await conn.fetchval('''
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'users'
            )
        ''')
        if not exists:
            await conn.execute('''
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            print("Created users table")

async def save_user(user_id: int, pool):
    async with pool.acquire() as conn:
        try:
            await conn.execute('''
                INSERT INTO users (user_id)
                VALUES ($1)
                ON CONFLICT (user_id) DO NOTHING
            ''', user_id)
        except Exception as e:
            print(f"Error saving user: {e}")
