# coding=gbk
import re, time, requests
import json
import csv
import pprint
from lxml import etree
from multiprocessing import Pool
from datetime import date
import os


class cachefilename():
    def __init__(self,):
        self.name = './{}.json'.format(date.today().strftime("%Y-%m-%d"))

def make_params(sceng,area,min,max):
    list = []
    my_params = {'scneg': sceng,  # 搜尋參數
                 'area': area,  # 指定區域
                 'scmin': min,
                 'scmax': max
                 }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
        'Referer': 'https://www.104.com.tw/job/'}
    searchpageurl = 'https://www.104.com.tw/jobs/search/?'
    ss = requests.session()

    res = ss.get(url=searchpageurl, headers=headers, params=my_params)
    html = etree.HTML(res.text)


    try:
        totalpage = int(html.xpath('//script[contains(text(),"totalPage")]/text()')[0].split('totalPage":')[-1].split(
            ',"totalCount"')[0])  # 取得js中的total page
        print("totalPage:", totalpage)
    except Exception as e:
        print(e)
        totalpage = 1
    for page in range(1,totalpage+1):
        my_params = {'scneg': sceng,  # 搜尋參數
                     'area': area,  # 指定區域
                     'scmin': min,
                     'scmax': max,
                     'page': page
                     }
        list.append(my_params)
    return list

#帶入myparams參數取得後五碼的list
def index(my_params):
    joburl = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
        'Referer': 'https://www.104.com.tw/job/'}
    searchpageurl = 'https://www.104.com.tw/jobs/search/?'
    ss = requests.session()
    try:
        res = ss.get(url=searchpageurl, headers=headers, params=my_params)#allow_redirects=False
    except Exception as e:
        print(e,"continuing...")
    try:
        if res.status_code == 200:
            html = etree.HTML(res.text)
            for ajax_content in html.xpath('//a[contains(@class,"js-job-link")]/@href'):
                joburl.append(ajax_content.split('?')[0].split('/')[-1])
            return joburl  #list
        else :
            print("錯誤代碼:",res.status_code)
    except Exception as exc:
        print(exc)
#把抓下來的後五碼存進json檔
def dump_json_file(query_dict):
    dumped_json_cache = json.dumps(query_dict)
    filename = date.today().strftime("%Y-%m-%d")
    fw = open('./{}.json'.format(filename), "w")
    fw.write(dumped_json_cache)
    fw.close()
    print('dump the data successfully')

def crowl(url):
    tmpdict = {}
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
    'Referer': 'https://www.104.com.tw/job/'}

    ss=requests.session()
    res = ss.get(url= url , headers = headers)
    if res.status_code == 200:
        try:
            return res.json()
        except json.decoder.JSONDecodeError:
            print(res.json)

    else:
        print('請求網頁json錯誤, 錯誤狀態碼：', res.status_code)
#資料清洗、重新排版
def extract(joburl,data_dict):
    job_dict = {joburl: {}}
    print('PID:{} , task:{}'.format(os.getpid(),joburl))
    try:
        data_dict = data_dict['data']

        # job title
        job_dict[joburl]['jobName'] = data_dict['header']['jobName']
        job_dict[joburl]['appearDate'] = data_dict['header']['appearDate']

        # company detail
        job_dict[joburl]['companyName'] = data_dict['header']['custName']
        job_dict[joburl]['companyUrl'] = data_dict['header']['custUrl']
        job_dict[joburl]['industry'] = data_dict['industry']
        job_dict[joburl]['addressRegion'] = data_dict['jobDetail']['addressRegion']
        job_dict[joburl]['longitude'] = data_dict['jobDetail']['longitude']
        job_dict[joburl]['latitude'] = data_dict['jobDetail']['latitude']

        # condition
        job_dict[joburl]['acceptRole'] = data_dict['condition']['acceptRole']
        job_dict[joburl]['workExp'] = data_dict['condition']['workExp']
        job_dict[joburl]['edu'] = data_dict['condition']['edu']
        job_dict[joburl]['major'] = data_dict['condition']['major']
        job_dict[joburl]['language'] = data_dict['condition']['language']
        job_dict[joburl]['skill'] = data_dict['condition']['skill']
        job_dict[joburl]['certificate'] = data_dict['condition']['certificate']
        job_dict[joburl]['other'] = data_dict['condition']['other']

        # job Detail
        job_dict[joburl]['jobDescription'] = data_dict['jobDetail']['jobDescription']
        job_dict[joburl]['jobCategory'] = data_dict['jobDetail']['jobCategory']
        job_dict[joburl]['jobType'] = data_dict['jobDetail']['jobType']
        job_dict[joburl]['manageResp'] = data_dict['jobDetail']['manageResp']
        job_dict[joburl]['businessTrip'] = data_dict['jobDetail']['businessTrip']
        job_dict[joburl]['workPeriod'] = data_dict['jobDetail']['workPeriod']
        job_dict[joburl]['vacationPolicy'] = data_dict['jobDetail']['vacationPolicy']
        job_dict[joburl]['startWorkingDay'] = data_dict['jobDetail']['startWorkingDay']
        job_dict[joburl]['needEmp'] = data_dict['jobDetail']['needEmp']

        # salary
        job_dict[joburl]['salary'] = data_dict['jobDetail']['salary']
        job_dict[joburl]['salaryMin'] = data_dict['jobDetail']['salaryMin']
        job_dict[joburl]['salaryMax'] = data_dict['jobDetail']['salaryMax']
        job_dict[joburl]['salaryType'] = data_dict['jobDetail']['salaryType']
        job_dict[joburl]['welfare'] = data_dict['welfare']['welfare']

        return job_dict
    except Exception as e:
        print(e)
        print(joburl)
        return job_dict

def write_json(filename,tmpdict):
    #pprint.pprint(tmpdict)
    with open('./'+filename,'w') as f:
        f.write(json.dumps(tmpdict))
    print("write{} successfully".format(filename))

def open_json_file(CACHE_FNAME):
    try:
        cache_file = open(CACHE_FNAME, 'r')
        cache_contents = cache_file.read()
        CACHE_DICTION = json.loads(cache_contents)
        cache_file.close()
        return CACHE_DICTION

    except:
        print("no any cache")
        CACHE_DICTION = {}
        return CACHE_DICTION

if __name__ == '__main__':
    arealist = ['6001001001']  #測試用

    # import os
    #
    # cpus = os.cpu_count()
    # print(cpus)