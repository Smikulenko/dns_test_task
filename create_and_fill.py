import os
import psycopg2
import asyncio
from dotenv import load_dotenv

from scripts.create_table import create_tables
from scripts.load_csv import load_csv_to_db
from scripts.insert_logdays import insert_logdays
from scripts.insert_stores import insert_stores
from scripts.insert_needs import insert_needs

PATH_BRANCH = r"data\branch_products.csv"
PATH_RC = r"data\rc_products.csv"
PATH_PRODUCTS = r"data\products.csv"


def main():
    """
    Создает таблицы и заполняет их данными таблицы.
    """
    load_dotenv()

    conn = psycopg2.connect(
      dbname=os.getenv("DB_NAME"),
      user=os.getenv("DB_USER"),
      password=os.getenv("DB_PASSWORD"),
      host=os.getenv("DB_HOST"),
      port=os.getenv("DB_PORT"),
    )
    create_tables(conn)
    load_csv_to_db(
        path=PATH_BRANCH,
        table_name="Branch_product",
        conn=conn
    )
    load_csv_to_db(
        path=PATH_RC,
        table_name="Rc_product",
        conn=conn
    )
    load_csv_to_db(
        path=PATH_PRODUCTS,
        table_name="Products",
        conn=conn
    )
    insert_logdays(conn)
    insert_stores(conn)
    conn.close()
    asyncio.run(insert_needs())


if __name__ == "__main__":
    main()
