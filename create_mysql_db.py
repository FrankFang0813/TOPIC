# mySQL python document: https://www.w3schools.com/python/python_mysql_create_table.asp

import pymysql
import time
import json


# [functions for open and export JSON file]--------------------------------

def open_json_file(CACHE_FNAME):
    try:
        cache_file = open(CACHE_FNAME, 'r')
        cache_contents = cache_file.read()
        CACHE_DICTION = json.loads(cache_contents)
        cache_file.close()
        return CACHE_DICTION

    except:
        CACHE_DICTION = {}
        return CACHE_DICTION


def dump_json_file(query_dict, file_name):
    dumped_json_cache = json.dumps(query_dict)
    fw = open(file_name, "w")
    fw.write(dumped_json_cache)
    fw.close()
    print('dump the data successfully')


# [functions for MySQL operation ]--------------------------------


def connect_to_db(DB_NAME):
    # 設定資料庫連線資訊
    host = 'localhost'
    port = 3306
    user = 'root'
    passwd = 'root'
    db = DB_NAME
    charset = 'utf8mb4'

    # 建立連線
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset=charset)
    print('Successfully connected to DB : {} !'.format(DB_NAME))
    return conn


def create_table(DB_NAME):
    conn = connect_to_db(DB_NAME)  # 連接資料庫
    cursor = conn.cursor()  # 建立游標

    # 試著建立DB
    sql_create_table = '''
        CREATE TABLE IF NOT EXISTS table2 (
        foo_column INT(2))

    '''

    cursor.execute(sql_create_table)  # 將指令放進 cursor 物件，並執行
    conn.commit()

    cursor.close()  # 關閉連線
    conn.close()


def insert_one_data(DB_NAME):
    # 產生時間的方式如下
    t = time.localtime()
    dt = time.strftime('%Y-%m-%d %H:%M:%S', t)

    conn = connect_to_db(DB_NAME)  # 連接資料庫
    cursor = conn.cursor()  # 建立游標

    # 試著再 INSERT 一筆資料
    sql = """
    INSERT INTO Staff (JobID, Name, DeptId, Age, Gender, Salary, recordDt)
    VALUES ('002', 'Jenny', '001', 30, 'F', 47000, '{}');
    """.format(dt)

    cursor.execute(sql)  # 將指令放進 cursor 物件，並執行
    conn.commit()

    cursor.close()  # 關閉連線
    conn.close()


def insert_multi_data(DB_NAME, lst_data):
    conn = connect_to_db(DB_NAME)  # 連接資料庫
    cursor = conn.cursor()  # 建立游標

    # 先寫好 SQL 語法
    # 並將語法中會不斷改變的部分挖空 ( %s )
    sql_insert = """
    INSERT INTO Staff (ID, Name, DeptId, Age, Gender, Salary, recordDt)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    # # 整理 lst_data 成可 insert 進資料庫的樣子，格式如下
    # '''
    # [('001', 'Jay', '001', '50', 'M', '56000'),
    #  ('002', 'Jenny', '001', '30', 'F', '47000'),
    #  ('003', 'Rick', '002', '45', 'M', '50000'),
    #  ('004', 'David', '003', '47', 'M', '45000'),
    #  ('005', 'Jake', '002', '32', 'M', '55000'),
    #  ('006', 'Abby', '001', '25', 'F', '40000'),
    #  ('007', 'Trump', '003', '80', 'M', '90000'),
    #  ('008', 'Eric', '001', '26', 'M', '85000')]
    # '''

    # 加入timestamp
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    values = [tuple(lst_data[d].values()) + (t,) for d in lst_data]

    # 將 SQL 批量執行
    print('新增資料筆數:', cursor.executemany(sql_insert, values))

    # Commit 並檢查資料是否存入資料庫
    conn.commit()

    # 關閉連線
    cursor.close()
    conn.close()


def select_data(DB_NAME):
    conn = connect_to_db(DB_NAME)  # 連接資料庫
    cursor = conn.cursor()

    # 試著將資料表中的資料取出
    # 先寫好 SQL 語法
    sql = """
    SELECT * FROM Staff;
    """

    # 將指令放進 cursor 物件，並執行
    cursor.execute(sql)

    # 將查詢結果取出
    data = cursor.fetchall()

    # 從資料庫將 datetime 形態的資料取出後，在 Python 中會變成 datetime 物件
    # 將 datetime 物件轉為字串
    data[1][6].strftime('%Y-%m-%d %H:%M:%S')

    # 關閉連線
    cursor.close()
    conn.close()

    return data





def main():
    CACHE_FNAME = '104_jobs.json'  # store the cache files
    cache_dict = open_json_file(CACHE_FNAME)

    db = 'TESTDB'

    pass


if __name__ == '__main__':
    db = 'testdb'
    connect_to_db(DB_NAME = db)
