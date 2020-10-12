from hdfs import *
import pymongo
client = pymongo.MongoClient("mongodb://192.168.1.154:27017/")
db = client['Topic_104']
mycol = db["Jobs"]

# def hadoop_connect():
had_client = InsecureClient("http://192.168.1.148:50070/",user = 'abc' )
had_client.makedirs('/user/104test')

for data in mycol.find({}, {"_id": 0}).limit(20):
    data_ID = list(data.keys())[0]
    had_client.write(f'/104test/{data_ID}', data=data, encoding='utf-8')
    print(f"{data_ID}成功導入")
# def writeinhadoop():


