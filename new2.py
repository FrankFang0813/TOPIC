# coding=gbk
from datetime import date
from multiprocessing import Pool
import crowler104
import os
import json
import time




def getlist():
    if not os.path.exists('./data'):
        os.mkdir('./data')
    else:
        pass
    list = catch()['index']
    print("please wait...")
    for i in os.listdir('./data/'):
        if i.strip('.json') in list:
            del list[list.index(i.strip('.json'))]
    print("got catching list!!",list)
    return list

def catch():

    cachefilename = crowler104.cachefilename().name  #檔案名稱為當天日期
    # cachefilename = './2020-08-26.json'               #固定檔案名稱
    cachedict = crowler104.open_json_file(cachefilename)
    return cachedict

def cach_to_json(index):
    if len(index) <10:
        url = "https://www.104.com.tw/job/ajax/content/"+index
        try:
            j = crowler104.extract(index,crowler104.crowl(url))
            crowler104.write_json('./data/{}.json'.format(index), j)


        except Exception as e:
            print("error:",e)
            time.sleep(2)
    else:
        pass




if __name__ == '__main__':

    list = getlist()

    p = Pool(8)
    for i in list:
            p.apply_async(cach_to_json(i))  # 異部執行
            print(i)
        # cach_to_json(d, i)
        # print(i)
    p.close()
    p.join()
    #crowler104.write_json('./final.json',d)
