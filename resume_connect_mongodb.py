import pymongo
from pymongo import MongoClient
import json
import os


# 開啟json檔
def open_json_file(CACHE_FNAME):


    try:
        cache_file = open('C:/Users/Big data/Desktop/resume/'+CACHE_FNAME, 'r',encoding='utf-8')
        cache_contents = cache_file.read()
        # print(type(cache_contents))
        CACHE_DICTION = json.loads(cache_contents)
        cache_file.close()
        # CACHE_DICTION才是dict格式
        return CACHE_DICTION

    except:
        CACHE_DICTION = {}
        return CACHE_DICTION

# 跟mongodb建立連線
def mongo_connect_build():
    global mycol
    client = MongoClient("mongodb://root:123456@192.168.1.158/")
    # client = pymongo.MongoClient("mongodb://192.168.1.158:27017/")
    # client = pymongo.MongoClient(host="192.168.1.158", port=27017)

    # 選擇使用的db,不存在則會在資料輸入時自動建立
    db = client['cakeresume']

    # 選擇collection,不存在則會在資料輸入時自動建立
    mycol = db["Resume"]




# 輸入資料
def data_insert(CACHE_DICTION):

    # 輸入json轉成字典的變數名稱
    # 若字典裡values為空值,則跳過
    # if len(list(CACHE_DICTION.values())[0]) == 0:
    #     pass
    # else:
    data = mycol.insert_many(CACHE_DICTION)
    print(data.inserted_ids)


if __name__ == "__main__":

    mongo_connect_build() # 連線mongodb


    # 班代設計程式爬下來之後,都會在data資料夾裡面,將各個檔案輸入到mongodb
    for CACHE_FNAME in os.listdir('C:/Users/Big data/Desktop/resume/'):

        CACHE_DICTION = open_json_file(CACHE_FNAME)

        data_insert(CACHE_DICTION)
