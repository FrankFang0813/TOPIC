import jieba
from sql_select import connect_to_mysql
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import os
conn = connect_to_mysql()

select_sql = '''
    select * from job_104
    where Job_Name like "%資料科學家%"
    '''
a = pd.read_sql(select_sql,con = conn)
b = list(a["Job_Discription"])
word = ""
for i in b:
    word = word + "," + i
# seg_words_list = jieba.lcut(word)
with open(file='./stop_words.txt', mode='r', encoding='utf-8') as file:
    stop_words = file.read().split('\n')
seg_stop_words_list = []
seg_words_list = jieba.lcut(word)
for term in seg_words_list:
    if term not in stop_words:
        seg_stop_words_list.append(term)

seg_df = pd.DataFrame(seg_stop_words_list, columns=['seg_word'])
# print(matplotlib.get_configdir())
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus']=False
seg_words = ' '.join(seg_stop_words_list)
cloud = WordCloud(font_path='C:/Windows/Fonts/simsun.ttc').generate(seg_words)
cloud.to_file('output.png')