import pymongo


def mongo_connect_build(db_name, col_name):
    global mycol
    client = pymongo.MongoClient("mongodb://192.168.1.25:27017/")  # 跟mongodb建立連

    db = client[db_name]  # 選擇使用的db,不存在則會在資料輸入時自動建立
    mycol = db[col_name]  # 選擇collection,不存在則會在資料輸入時自動建立

def data_find():
    # 尋找多筆資料
    return mycol.find()

#     for find_manydata in mycol.find()[0:10]:
#         print(find_manydata)

# filter out job title data from JSON file

def extract_job_title(job_data):
    lst_jobTile = []

    for i in job_data:
        job_URL = list(i.keys())[1]  # get the "URL shortcut" of each job
        job_content = i[job_URL]
        job_name = job_content['jobName']
        try:
            jobCategory = job_content['jobCategory'][0]['description']
        except:
            jobCategory = None

        dict_job = {'jobURL': job_URL, 'jobName': job_name, 'jobCategory': jobCategory}
        lst_jobTile.append(dict_job)

    return lst_jobTile


mongo_connect_build('Topic_104', 'Jobs') # connect to mongoDB to get job data

# https://www.104.com.tw/job/1006w?jobsource=company_job

job_data = data_find() # store the data
lst_jobTitle = extract_job_title(job_data) # extract job title

for i in lst_jobTitle[0:10]:
    print(i)


import re

# 移除所有括號內數據
for i in lst_jobTitle:
    i['jobName'] = re.sub(u"\\.|k|,|\+|\\(.*?\\)|\\{.*?}|\\[.*?]|\\【.*?】|\\（.*?）|\\(.*?）|\\<.*?>|\d|", "", i['jobName'])

for i in lst_jobTitle[0:10]:
    print(i)

# 定義資料清洗用函數

def isEnglish(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True

def sort_dict_by_value(x):
    return {k: v for k, v in sorted(x.items(), key=lambda item: item[1], reverse=True)}


def count_job_CN(lst_jobTitle): # count the frequency of words in job title
    jobTitle_count = {}

    for i in lst_jobTitle:
        # 處理非英文職缺
        if isEnglish(i['jobName'])== False :
            JobName = re.sub(r'[^\w\s]',' ',i['jobName'])  # remove all punctuations
            # print(JobName)
            for word in JobName.split(' '):
                word = word.strip()
                if word not in jobTitle_count.keys():
                    jobTitle_count[word] = 1
                else:
                    jobTitle_count[word] += 1


    result = sort_dict_by_value(jobTitle_count)
    return result


def find_jobtitle(JOB):  # 尋找人工職缺關鍵字
    jobTitle_count = {}

    for i in lst_jobTitle:
        jobName = i['jobName']
        # print(jobName)
        if re.search(rf"\s*{JOB}\s*", jobName, re.IGNORECASE):
            if jobName not in jobTitle_count.keys():
                jobTitle_count[jobName] = 1
            else:
                jobTitle_count[jobName] += 1

    jobTitle_count = sort_dict_by_value(jobTitle_count)

    for k, v in jobTitle_count.items():
        if v > 1:
            # print (k, " : ", v)
            print(k)

# find_jobtitle("老師")

# 人工輸入職缺, 開啟'Jobtitle_HandMade.txt'檔案

with open(file='Jobtitle_HandMade.txt', mode='r', encoding="UTF-8") as file:
    # 依照換行字元 \n 進行切割，切完為 list
    handmade_words = file.read().split('\n')

# stopword 排除字元表單 ,開啟'Jobtitle_stopwords.txt'檔案

with open(file='Jobtitle_stopwords.txt', mode='r', encoding="UTF-8") as file:
    # 依照換行字元 \n 進行切割，切完為 list
    stop_words = file.read().split('\n')


# 定義創建職業名稱函數

def create_jobTitle(dict_jobCount, handmade_words):
    # 創建職業名稱list
    job_lst_clean = []

    for k, v in dict_jobCount.items():
        # 如果詞彙出現頻率<18, 詞彙長度<2, 或是詞彙屬於stopword, 排除詞彙
        if v < 17 or len(k) < 2 or k in stop_words:
            continue

        else:
            job_lst_clean.append(k)

    # 加入人工輸入職缺
    for i in handmade_words:
        if i.strip() not in job_lst_clean:
            job_lst_clean.append(i)

    # 更改中英職缺排序，中文職缺優先
    job_lst_clean_CN = []
    job_lst_clean_EN = []

    for job in job_lst_clean:
        if isEnglish(job) == True:
            job_lst_clean_EN.append(job)
        else:
            job_lst_clean_CN.append(job)

    # 根據職缺長度排序

    job_lst_clean_CN = sorted(job_lst_clean_CN, key=len, reverse=True)
    job_lst_clean_EN = sorted(job_lst_clean_EN, key=len, reverse=True)

    job_lst_clean_CN = job_lst_clean_CN + job_lst_clean_EN

    return job_lst_clean_CN

result = count_job_CN(lst_jobTitle)  # 計算職位出現頻率
job_lst_clean = create_jobTitle(result, handmade_words)  # 創建職業名稱list

print("總共產生職稱數量: " ,len(job_lst_clean))
# print(job_lst_clean)

# test for match result

def add_jobtitle(lst_jobTitle, job_lst_clean):
    count = 0
    for i in lst_jobTitle:
        jobName = i['jobName']
        match_title = None
        count += 1

        # match jobtitle with jobName
        for JOB in job_lst_clean:
            if re.search(rf"\s*{JOB}\s*", jobName, re.IGNORECASE):
                match_title = JOB
                break

        # check whether match is successful. If not, use origianl job name.
        if match_title == None:
            match_title = jobName

        i['jobName_clean'] = match_title
        print(count, ' : ')
        # print(i)
        # print('='*20)

        print('successfully add jobtitle')
add_jobtitle(lst_jobTitle, job_lst_clean)


import json
import pandas as pd


def open_json_file(CACHE_FNAME):
    try:
        cache_file = open(CACHE_FNAME, 'r', encoding='utf-8-sig')
        cache_contents = cache_file.read()
        CACHE_DICTION = json.loads(cache_contents, encoding='utf-8-sig')
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
    print('successfully write down the file: ', file_name)

lst_cat = open_json_file('JobCat.json')

dict_job_category = {}

for i in lst_cat:
    # layer one
    layer_1 = i['des']
    dict_job_category[layer_1] = (layer_1, 1)

    # layer two

    for j in i['n']:
        layer_2 = j['des']
        dict_job_category[layer_2] = (layer_1, 2)

        # layer three
        for j in j['n']:
            layer_3 = j['des']
            dict_job_category[layer_3] = (layer_1, 3)

for k, v in dict_job_category.items():
    print(k, v)

# add missing category
dict_job_category['飯店或餐廳高階主管'] = ('餐飲╱旅遊 ╱美容美髮類', 3)
dict_job_category['硬體工程研發高階主管'] = ('資訊軟體系統類', 3)
dict_job_category['專案高階主管'] = ('行銷╱企劃╱專案管理類', 3)
dict_job_category['人力資源高階主管'] = ('經營╱人資類', 3)
dict_job_category['國外業務高階主管'] = ('客服╱門市╱業務╱貿易類', 3)
dict_job_category['醫院行政高階主管'] = ('醫療╱保健服務類', 3)
dict_job_category['國內業務高階主管'] = ('客服╱門市╱業務╱貿易類', 3)
dict_job_category['經營管理高階主管'] = ('經營╱人資類', 3)
dict_job_category['行政總務高階主管'] = ('行政╱總務╱法務類', 3)
dict_job_category['品管或品保高階主管'] = ('生產製造╱品管╱環衛類', 3)
dict_job_category['財務或會計高階主管'] = ('財會╱金融專業類', 3)
dict_job_category['專案業務高階主管'] = ('客服╱門市╱業務╱貿易類', 3)
dict_job_category['產品企劃高階主管'] = ('行銷╱企劃╱專案管理類', 3)
dict_job_category['營建不動產業高階主管'] = ('營建╱製圖類', 3)
dict_job_category['總經理/副總'] = ('經營╱人資類', 3)
dict_job_category['行銷企劃高階主管'] = ('行銷╱企劃╱專案管理類', 3)
dict_job_category['特別助理'] = ('經營╱人資類', 3)
dict_job_category['客服部門高階主管'] = ('客服╱門市╱業務╱貿易類', 3)
dict_job_category['軟體工程研發高階主管'] = ('研發相關類', 3)
dict_job_category['其他工程研發高階主管'] = ('研發相關類', 3)
dict_job_category['廣告企劃高階主管'] = ('行銷╱企劃╱專案管理類', 3)
dict_job_category['媒體或出版高階主管'] = ('傳播藝術╱設計類', 3)
dict_job_category['電子商務技術高階主管'] = ('資訊軟體系統類', 3)
dict_job_category['資材高階主管'] = ('資材╱物流╱運輸類', 3)
dict_job_category['採購高階主管'] = ('資材╱物流╱運輸類', 3)
dict_job_category['生產管理高階主管'] = ('生產製造╱品管╱環衛類', 3)
dict_job_category['MIS高階主管'] = ('資訊軟體系統類', 3)
dict_job_category['品牌宣傳及媒體公關高階主管'] = ('行銷╱企劃╱專案管理類', 3)
dict_job_category['法務部門高階主管'] = ('行政╱總務╱法務類', 3)
dict_job_category['光電工程研發高階主管'] = ('研發相關類', 3)
dict_job_category['保險高階主管'] = ('財會╱金融專業類', 3)
dict_job_category['投資經理人'] = ('財會╱金融專業類', 3)
dict_job_category['PMO高階主管'] = ('行銷╱企劃╱專案管理類', 3)
dict_job_category['通訊工程研發高階主管'] = ('研發相關類', 3)
dict_job_category['金融高階主管'] = ('財會╱金融專業類', 3)


def add_jobCategory(lst_jobTitle, dict_job_category):
    count = 0

    for i in lst_jobTitle:
        jobCategory = i['jobCategory']

        if jobCategory != None:
            jobCategory = jobCategory.replace("／", "╱")
            jobCat_main = dict_job_category[jobCategory][0]
            JobCat_layer = dict_job_category[jobCategory][1]
            i['jobCat_main'] = jobCat_main
            i['JobCat_layer'] = JobCat_layer

        else:
            i['jobCat_main'] = None
            i['JobCat_layer'] = None

        count += 1
        print(count, ' : ')
        print('successfully add category')

add_jobCategory(lst_jobTitle, dict_job_category)

dump_json_file(lst_jobTitle, 'test_jobClean.json')

test = open_json_file('test_jobClean.json')

for i in test:
    print(i)