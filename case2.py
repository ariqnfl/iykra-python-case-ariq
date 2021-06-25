import sqlite3
from sqlite3 import Error
from helper import create_connection,create_table,create_sales

def main():
    database = "data.db"

    sql_create_sls_table = """
    CREATE TABLE IF NOT EXISTS sales (
        date DATE,
        sales_amt FLOAT
    );
    """
    conn = create_connection(database)
    with conn:

        create_table(conn,sql_create_sls_table)
        sls_1 = ("2021-01-01",100)
        sls_2 = ("2021-01-02",34)
        sls_3 = ("2021-01-02",123)
        sls_4 = ("2021-01-02",134)
        sls_5 = ("2021-01-03",145)
        sls_6 = ("2021-01-03",24)
        sls_7 = ("2021-01-04",541)
        sls_8 = ("2021-01-04",636)
        sls_9 = ("2021-01-05",322)
        sls_10 = ("2021-01-06",242)
        sls_11 = ("2021-01-07",22)
        sls_12 = ("2021-01-08",46)

        create_sales(conn,sls_1)
        create_sales(conn,sls_2)
        create_sales(conn,sls_3)
        create_sales(conn,sls_4)
        create_sales(conn,sls_5)
        create_sales(conn,sls_6)
        create_sales(conn,sls_7)
        create_sales(conn,sls_8)
        create_sales(conn,sls_9)
        create_sales(conn,sls_10)
        create_sales(conn,sls_11)
        create_sales(conn,sls_12)

        c = conn.cursor()
        c.execute("select date,sum(sales_amt) from sales group by date")
        for row in c.fetchall():
            print(row)
        c.execute("drop table sales")


if __name__ == "__main__":
    main()