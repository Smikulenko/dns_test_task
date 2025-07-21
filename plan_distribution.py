import os
import psycopg2
from dotenv import load_dotenv


def main():
    """
    Распределяет товары по магазинам.
    """
    load_dotenv()

    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS Planned(
              Branch_ID UUID NOT NULL,
              Rc_ID UUID NOT NULL,
              Product_ID UUID NOT NULL,
              Quantity NUMERIC NOT NULL
            );
            """
        )
        cur.execute(
            """
            TRUNCATE TABLE Planned
            """
        )
        cur.execute("""
          WITH
          Branch_Product_Need AS (
                SELECT
                    n.Branch_ID,
                    n.Product_ID,
                    GREATEST(
                        n.Needs - (
                            bp."Остаток" - bp."Резерв" + bp."Транзит"),0
                        )AS Actual_Need
                FROM Needs n
                LEFT JOIN Branch_product bp
                    ON n.Branch_ID =bp."Фирма" AND n.Product_ID =bp."Товар"
                WHERE GREATEST(
                    n.Needs - (
                        bp."Остаток" - bp."Резерв" + bp."Транзит"), 0) > 0
          ),
          RC_Product_Stock AS (
                SELECT
                    "РЦ" AS Rc_ID,
                    "Товар" AS Product_ID,
                    ("Остаток" - "Резерв" - "Транзит") AS Available
                FROM rc_product
                WHERE ("Остаток" - "Резерв" - "Транзит") > 0
          ),
          Need_With_Details AS (
              SELECT
                  bpn.Branch_ID,
                  bpn.Product_ID,
                  bpn.Actual_Need,
                  s.Priority,
                  l.Logdays,
                  rs.Rc_ID,
                  rs.Available
              FROM Branch_Product_Need bpn
              JOIN Products p ON bpn.Product_ID = p.Product_ID
              LEFT JOIN Logdays l ON  bpn.Branch_ID =  l.Branch_ID
                    AND p.Category_ID = l.Category_ID
              LEFT JOIN Stores s ON  bpn.Branch_ID = s.Branch_ID
              JOIN RC_Product_Stock rs ON bpn.Product_ID = rs.Product_ID
          ),
          Distribution AS (
              SELECT
                  Branch_ID,
                  Rc_ID,
                  Product_ID,
                  LEAST(Actual_Need, Available) AS Quantity,
                  ROW_NUMBER() OVER (
                    PARTITION BY Product_ID, Branch_ID
                    ORDER BY Priority DESC, Rc_ID
                  ) AS rn
              FROM Need_With_Details
          )
          INSERT INTO Planned (Branch_ID,Rc_ID,Product_ID, Quantity)
          SELECT Branch_ID,Rc_ID,Product_ID, Quantity
          FROM Distribution
          WHERE Quantity > 0;
    """)
    print("Успешно создана и заполнена таблица Planned")
    conn.commit()
    conn.close()



if __name__ == "__main__":
    main()
