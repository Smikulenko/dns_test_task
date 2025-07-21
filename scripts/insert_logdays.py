def insert_logdays(conn):
    """
    Заполняет таблицу Logdays.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO Logdays (Branch_ID,Category_ID,Logdays)
            SELECT DISTINCT
                b."Фирма" AS Branch_ID,
                p.Category_ID,
                7 AS Logdays
            FROM Branch_product b
            JOIN Products p ON b."Товар" = p.Product_ID
            """
            )
        conn.commit()
        print("Успешно заполнена таблица Logdays")
