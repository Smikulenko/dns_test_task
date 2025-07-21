import asyncio
import asyncpg
from dotenv import load_dotenv
import os
import random

BATCH_SIZE = 100000
MAX_TASK = 10
MIN_SIZE = 1


async def get_total_batches(conn):
    """
    Подсчет общего количество батчей.
    """
    row = await conn.fetchrow(
        """
        SELECT COUNT (*) AS  total FROM Branch_product
        """
    )
    total_count = row['total']
    total_batches = (total_count + BATCH_SIZE - 1) // BATCH_SIZE
    return total_batches


async def get_logdays(conn):
    """
    Получение строк из таблицы Logdays.
    """
    rows = await conn.fetch(
        """
        SELECT Branch_ID,Logdays FROM Logdays
        """
    )
    logdays = {row["branch_id"]: row["logdays"] for row in rows}
    return logdays


async def get_rows(conn, butch_number):
    """
    Получение строк из таблицы Branch_product.
    """
    rows = await conn.fetch(f"""
            SELECT DISTINCT
                "Фирма" AS Branch_ID,
                "Товар" AS Product_ID,
                "Остаток"
            FROM Branch_product
            ORDER BY Branch_ID,Product_ID
            LIMIT {BATCH_SIZE} OFFSET {BATCH_SIZE*butch_number}

        """)
    return rows


def calculate_need(stock, logdays):
    """
    Вычисление Need.
    """
    if stock == 0:
        stock = 150
    result = random.randint(
        1,
        (min(150, int(stock))*int(logdays))
    )
    return result


async def processing_batch(
        pool,
        semaphore,
        batch_number,
        logdays_dict,
        total_batches):
    """
    Обработка данных
    """
    async with semaphore:
        async with pool.acquire() as conn:
            print(
                f"Начало обработки батча {batch_number+1} из {total_batches+1}"
                )
            rows = await get_rows(conn, batch_number)
            data = []
            for row in rows:
                store = row["branch_id"]
                product = row["product_id"]
                stock = row["Остаток"]
                logdays = logdays_dict.get(store, 7)
                needs = calculate_need(stock, logdays)
                data.append((store, product, needs))
            await conn.executemany(
                """
                INSERT INTO Needs (
                    Branch_ID,
                Product_ID,Needs
                ) VALUES ($1,$2,$3)
                """, data)
            print(
                f"Готов {batch_number+1} из {total_batches+1}"
            )


async def insert_needs():
    """
    Запуск процесса.
    """
    load_dotenv()
    pool = await asyncpg.create_pool(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        database=os.getenv("DB_NAME"),
        password=os.getenv("DB_PASSWORD"),
    )
    async with pool.acquire() as conn:
        total_batches = await get_total_batches(conn)
        logdays_dict = await get_logdays(conn)
    semaphore = asyncio.Semaphore(MAX_TASK)
    tasks = [
        processing_batch(
            pool,
            semaphore,
            batch_number,
            logdays_dict,
            total_batches
        )
        for batch_number in range(total_batches)
        ]
    await asyncio.gather(*tasks)
    await pool.close()
    print("Успешно заполнена таблица Needs")
