import pymongo
import json
import time
from gensim.models import Word2Vec
import pandas as pd

def mongo_connect_build(db_name, col_name):
    global mycol
    client = pymongo.MongoClient("mongodb://192.168.1.25:27017/")  # 跟mongodb建立連

    db = client[db_name]  # 選擇使用的db,不存在則會在資料輸入時自動建立
    mycol = db[col_name]  # 選擇collection,不存在則會在資料輸入時自動建立


def data_find():
    # 尋找多筆資料
    return mycol.find({}, {"_id": 0})

def create_w2v_list(job_data):
    w2v_list = []
    for i in job_data:
        content_list = []
        content_list.append(i['jobURL'])
        for j in i['jobDescription_concate_jieba']:
            for k in j:
                content_list.append(k)
            w2v_list.append(content_list)
    return w2v_list

# train word2vec model
def word2vec_model_train(train_list):
    start = time.time()
    num_features = 100
    # Word vector dimensionality - The size of the dense vector to represent each token or word
    #(i.e. the context or neighboring words). If you have limited data, then size should be a much smaller value
    #since you would only have so many unique neighbors for a given word

    min_word_count = 1
    # Minimium frequency count of words. The model would ignore words that do not satisfy the min_count.

    num_workers = 10       # How many threads to use behind the scenes?

    context = 25
    # The maximum distance between the target word and its neighboring word.

    downsampling = 1e-2
    # threshold for configuring which higher-frequency words are randomly downsampled

    # Initialize and train the model
    model = Word2Vec(train_list, workers=num_workers, \
                size=num_features, min_count = min_word_count, \
                window = context,sample = downsampling, iter=10,sg=1)

    # If you don't plan to train the model any further, calling
    # init_sims will make the model much more memory-efficient.
    model.init_sims(replace=True)

    end = time.time()
    return model
    print(f'Training completed. It cost : {round(end-start,2)} sec')


mongo_connect_build('jieba_clean', 'job_clean')
job_data = data_find()
w2v_list = create_w2v_list(job_data)
print(w2v_list[0:10])
model = word2vec_model_train(w2v_list)

model.save('w2v_test_1026.model')
# model2.wv.vocab
# model2.wv.similarity('C++','C')
# model.wv.similar_by_word('python',10)
# model.wv.get_vector('python')
# model.wv.most_similar(positive=['python'],negative=[])