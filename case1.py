import sqlite3
from sqlite3 import Error
from helper import create_connection,create_table,create_trx

def main():
    database = "data.db"

    sql_create_trx_table = """
    CREATE TABLE IF NOT EXISTS transactions (
        id INT,
        insert_time DATETIME,
        tx_amount FLOAT,
        tx_type VARCHAR(100),
        status VARCHAR(100)
    );
    """
    conn = create_connection(database)
    with conn:

        create_table(conn,sql_create_trx_table)
        trx_1 = (1,'2021-01-01 12:30:30',20,'Buy','Open')
        trx_2 = (2,'2021-01-05 12:30:35',50,'Sell','Open')
        trx_3 = (3,'2021-01-04 12:30:36',10,'Buy','Open')
        trx_4 = (4,'2021-01-08 12:30:37',30,'Sell','Open')
        trx_5 = (1,'2021-01-02 12:31:35',50,'Sell','Close')
        trx_6 = (1,'2021-01-03 12:40:35',40,'Buy','Close')
        trx_7 = (3,'2021-01-01 12:40:30',20,'Buy','Open')
        trx_8 = (4,'2021-01-12 12:50:30',80,'Buy','Close')
        trx_9 = (2,'2021-01-04 12:20:30',90,'Buy','Close')
        trx_10 = (3,'2021-01-05 12:30:30',50,'Buy','Open')
        create_trx(conn,trx_1)
        create_trx(conn,trx_2)
        create_trx(conn,trx_3)
        create_trx(conn,trx_4)
        create_trx(conn,trx_5)
        create_trx(conn,trx_6)
        create_trx(conn,trx_7)
        create_trx(conn,trx_8)
        create_trx(conn,trx_9)
        create_trx(conn,trx_10)

        c = conn.cursor()
        c.execute("select distinct id,max(insert_time),tx_amount,tx_type,status from transactions group by id")
        for row in c.fetchall():
            print(row)
        c.execute("drop table transactions")


if __name__ == "__main__":
    main()