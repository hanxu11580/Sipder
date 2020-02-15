import pymysql


def connect_db():
    conn = pymysql.connect(
        host='47.99.183.154',
        user='mj',
        passwd='kloe.dfjTe',
        db='meijing-data-db',
        port=31002,
    )
    print('连接成功！')
    cursor = conn.cursor()
    return conn, cursor
