import pymysql


def connect_db():
    conn = pymysql.connect(
        host='你的ip地址',
        user='数据库用户',
        passwd='密码',
        db='数据库名字',
        port='端口号',
    )
    print('连接成功！')
    cursor = conn.cursor()
    return conn, cursor
