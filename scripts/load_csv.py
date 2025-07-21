
def load_csv_to_db(conn, path, table_name):
    """
    Копирует данные из csv файла.
    """
    with open(path, "r", encoding="cp1251") as file:
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE {table_name};")
        cur.copy_expert(
            f"COPY {table_name} From STDIN WITH CSV HEADER DELIMITER ','",
            file
            )
        conn.commit()
        print(f"Успешно скопирована информация в таблицу {table_name}")
