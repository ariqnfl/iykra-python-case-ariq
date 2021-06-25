import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    # set conn var as None
    conn = None

    # try connection
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    
    return conn

def create_table(conn,create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def create_trx(conn,trx):
    sql = """
    INSERT INTO transactions(id,insert_time,tx_amount,tx_type,status)
    VALUES(?,?,?,?,?)
    """
    cur = conn.cursor()
    cur.execute(sql,trx)
    conn.commit()
    return cur.lastrowid

def create_sales(conn,sls):
    sql = """
    INSERT INTO sales(date,sales_amt)
    VALUES(?,?)
    """
    cur = conn.cursor()
    cur.execute(sql,sls)
    conn.commit()
    return cur.lastrowid