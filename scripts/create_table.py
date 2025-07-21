def create_tables(conn):
    """
    Создание таблиц.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS Branch_product (
              Товар UUID NOT NULL,
              Фирма UUID NOT NULL,
              Остаток NUMERIC NOT NULL,
              Резерв NUMERIC NOT NULL,
              Транзит NUMERIC NOT NULL
            );

            CREATE TABLE IF NOT EXISTS Rc_product (
              Товар UUID NOT NULL,
              РЦ UUID NOT NULL,
              Остаток NUMERIC NOT NULL,
              Резерв NUMERIC NOT NULL,
              Транзит NUMERIC NOT NULL
            );

            CREATE TABLE IF NOT EXISTS Logdays (
              Branch_ID UUID,
              Category_ID UUID,
              Logdays NUMERIC
            );

            CREATE TABLE IF NOT EXISTS Stores (
              Branch_ID UUID,
              Priority NUMERIC
            );

            CREATE TABLE IF NOT EXISTS Needs (
              Branch_ID UUID,
              Product_ID UUID,
              Needs NUMERIC
            );

            CREATE TABLE IF NOT EXISTS Products (
              Product_ID  UUID,
              Category_ID  UUID
            );
            """
        )
    conn.commit()
    print("Таблицы успешно созданы.")
