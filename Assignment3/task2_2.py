import pandas as pd

import math

import time
import numpy as np
import xgboost as xgb
from sklearn import preprocessing
from sklearn.preprocessing import LabelEncoder

from pyspark import SparkConf, SparkContext
import sys
import json


start = time.time()

folder_path=sys.argv[1]
testing_file_path=sys.argv[2]
output_file_path=sys.argv[3]


userjson=pd.read_json(folder_path+"/user.json",lines=True,chunksize=100000)
for i in userjson:
    uchunk=i
    break

uchunk1=uchunk.copy()


for i in userjson:
    uchunk1=uchunk1.append(i)

businessjson=pd.read_json(folder_path+"/business.json",lines=True,chunksize=100000)
for i in businessjson:
    bchunk=i
    break

bchunk1=bchunk.copy()


for i in businessjson:
    bchunk1=bchunk.append(i)


train_data = pd.read_csv(folder_path+"/yelp_train.csv")
userd=uchunk1[['user_id','average_stars','review_count','useful','fans']]
businessd=bchunk1[['business_id','stars','review_count']]
businessd=businessd.rename(columns={'stars':'business_avg_stars','review_count':'business_rev_count'})


trainjoin=pd.merge(train_data,userd,on='user_id',how='inner')
trainjoin=pd.merge(trainjoin,businessd,on='business_id',how='inner')

d1={}
d2={}
d3={}
d4={}
d5={}

for i, content in trainjoin.iterrows():
    if(content['user_id'] not in d1):
        d2[content['user_id']]=1
        d3[content['user_id']]=(content['stars']-content['average_stars'])*(content['stars']-content['average_stars'])*1+0
        d4[content['user_id']]=content['stars']
        d5[content['user_id']] = content['stars']

    else:

        d2[content['user_id']] = d2[content['user_id']]+1
        d3[content['user_id']] = 3+d3[content['user_id']] + ((content['stars'] - content['average_stars'])*(content['stars'] - content['average_stars'])*1+0)-3
        if(content['stars']>d4[content['user_id']]):
            d4[content['user_id']] = content['stars']
        if (content['stars'] < d5[content['user_id']]):
            d5[content['user_id']] = content['stars']


stat=pd.DataFrame(columns=["user_id","user_variance","user_max","user_min"])

for i in d2:
    if(d3[i] == 0 or d2[i] == 0):
        var = d3[i]/d2[i]
    else:
        var = (1/(d2[i]/d3[i]))*1+0
    stat=stat.append({'user_id': i,'user_variance':var,'user_max':d4[i],'user_min':d5[i]},ignore_index=True)


trainjoin=pd.merge(trainjoin,stat,on='user_id',how='left')


pjtr=trainjoin.copy()

for cn in pjtr.columns:
    if pjtr[cn].dtype=='object':
        le=preprocessing.LabelEncoder()
        le.fit(list(pjtr[cn].values))
        pjtr[cn]=le.transform(list(pjtr[cn].values))


rate=pjtr.stars.values
tdi=pjtr.drop(['stars'],axis=1)
tdi=tdi.drop(['user_id'],axis=1)
tdi=tdi.drop(['business_id'],axis=1)
tdi=tdi.values


ratemodel=xgb.XGBRegressor()
ratemodel.fit(tdi,rate)


test_data = pd.read_csv(testing_file_path)

testjoin=pd.merge(test_data,userd,on='user_id',how='left')
testjoin=pd.merge(testjoin,businessd,on='business_id',how='left')
testjoin=pd.merge(testjoin,stat,on='user_id',how='left')


usertest=testjoin['user_id'].values
businesstest=testjoin['business_id'].values



pjt=testjoin.copy()

for cn in pjt.columns:
    if pjt[cn].dtype=='object':
        label_encoder=preprocessing.LabelEncoder()
        label_encoder.fit(list(pjt[cn].values))
        pjt[cn]=label_encoder.transform(list(pjt[cn].values))

pjt.fillna((-999),inplace=True)

tdi=pjt.drop(['user_id'],axis=1)
tdi=tdi.drop(['business_id'],axis=1)
tdi=tdi.drop(['stars'],axis=1)
tdi=tdi.values


pred=rate.predict(data=tdi)

final=pd.DataFrame()
final['user_id']=usertest
final['business_id']=businesstest
final['prediction']=pred

final.to_csv(output_file_path,sep=',',encoding='utf-8',index=False)



print("Duration:",(time.time() - start))

reference_file=open(testing_file_path,"r")
output_file=open(output_file_path,"r")

reference_file=open(testing_file_path,"r")
output_file=open(output_file_path,"r")
n=0
rmse=0
while(True):
    l1=output_file.readline()
    l2=reference_file.readline()
    if "user_id" in l2:
        continue
    if l2=="":
        break
    n+=1
    #if(n==1):
        #print(float(l1.split(",")[2][:-1]),float(l2.split(",")[2][:-1]))
    #print(float(l1.split(",")[2][:-1])-float(l2.split(",")[2][:-1]),float(l1.split(",")[2][:-1]), float(l2.split(",")[2][:-1]))
    rmse+=math.pow(float(l1.split(",")[2][:-1])-float(l2.split(",")[2][:-1]),2)
    #print(rmse)
    if not l1 and not l2:
        break
rmse=math.sqrt(rmse/n)
print(rmse)




reference_file=open(testing_file_path,"r")
output_file=open(output_file_path,"r")
n=0
rmse=0
test_dict={}
output_dict={}
while(True):
    l1=output_file.readline()
    l2=reference_file.readline()
    if "user_id" in l2:
        continue
    if l2=="":
        break
    if not l1 and not l2:
        break
    #print(float(l1.split(",")[2][:-1])-float(l2.split(",")[2][:-1]),float(l1.split(",")[2][:-1]), float(l2.split(",")[2][:-1]))
    test_dict[(l2.split(",")[0],l2.split(",")[1])] = float(l2.split(",")[2][:-1])
    output_dict[(l1.split(",")[0], l1.split(",")[1])] = float(l1.split(",")[2][:-1])
for k in output_dict:
    n+=1
    rmse += math.pow(output_dict[k] - test_dict[k], 2)
rmse=math.sqrt(rmse/n)
print(rmse)

