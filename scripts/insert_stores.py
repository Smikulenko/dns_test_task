def insert_stores(conn):
    """
    Заполняет таблицу Stores.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO Stores (Branch_ID,Priority)
            SELECT
                b."Фирма" AS Branch_ID,
                CEILING(RANDOM() * 10) AS Priority
            FROM (
                SELECT DISTINCT "Фирма"
                FROM Branch_product
            )b
            WHERE b."Фирма" NOT IN (SELECT Branch_ID FROM Stores)
            """
            )
        conn.commit()
        print("Успешно заполнена таблица Stores")
