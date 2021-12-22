from pyspark import SparkConf, SparkContext
import sys
import math

import time
import pandas as pd
import time
import numpy as np
import xgboost as xgb
from sklearn import model_selection, preprocessing
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import LabelEncoder

from pyspark import SparkConf, SparkContext
import sys
import json
from collections import OrderedDict
import csv

start_time = time.time()

dmain = {}
#Vocareum
# os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
# os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

os.environ['PYSPARK_PYTHON'] = '/Users/rohanchaudhari/.pyenv/shims/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Users/rohanchaudhari/.pyenv/shims/python3.6'

sc = SparkContext('local[*]','HybridBased')
sc.setLogLevel("ERROR")

folder_path = sys.argv[1]
test_file_name = sys.argv[2]
output_file_name = sys.argv[3]

def itembased():
	similarity={}
	def com(a,b):
	    return set(b & a)

	def pearson(a,b,c):
	    temp = (1/((math.sqrt(a)*math.sqrt(b)*1)/c))*1-0
	    return temp

	def predict(user_id,business_id):
	    nn = 75
	    dd = 30
	    sim={}
	    if((user_id not in user_businessRDD) and (business_id in business_userRDD)):
	        if(business_avgRDD[business_id][1] == 0 or business_avgRDD[business_id][0] == 0):
	            temp = business_avgRDD[business_id][0]/business_avgRDD[business_id][1]
	        else:
	            temp = (1/(business_avgRDD[business_id][1]/business_avgRDD[business_id][0]))*1+0
	        return ((user_id,business_id),temp)
	    elif((user_id in user_businessRDD) and (business_id not in business_userRDD)):
	        if(user_avgRDD[user_id][1] == 0 or user_avgRDD[user_id][0] == 0):
	            temp = user_avgRDD[user_id][0]/user_avgRDD[user_id][1]
	        else:
	            temp = (1/(user_avgRDD[user_id][1]/user_avgRDD[user_id][0]))+0-0
	        return ((user_id,business_id),temp)
	    elif((user_id not in user_businessRDD) and (business_id not in business_userRDD)):
	        temp = (1/(dd/nn))*1-0
	        return ((user_id, business_id), temp)

	    ratedbusiness = user_businessRDD[user_id]
	    business_user=business_userRDD[business_id]

	    if(business_id in ratedbusiness):
	        return ((user_id,business_id),business_user_ratingRDD[(business_id,user_id)])

	    if(business_avgRDD[business_id][1] == 0 or business_avgRDD[business_id][0] == 0):
	        businessavg = business_avgRDD[business_id][0]/business_avgRDD[business_id][1]
	    else:
	        businessavg = (1/(business_avgRDD[business_id][1]/business_avgRDD[business_id][0]))*1

	    for i in ratedbusiness:
	        # if(tuple(sorted([business_id,i])) in similarity):
	        if(tuple(sorted([i,business_id])) in similarity):
	            sim[i]=similarity[tuple(sorted([i,business_id]))]
	            continue
	        else:
	            coldplay=0
	            maroonfive=0
	            cardi_B=0
	            bavg= (1/(business_avgRDD[i][1]/business_avgRDD[i][0]))*1-0

	            business_item_users=business_userRDD[i]
	            cus = com(business_user,business_item_users)
	            if (len(cus) == 0):
	                if((1/(bavg/businessavg))*1 <= 1):
	                    ncs = (1/(bavg/businessavg))*1-0
	                else:
	                    ncs = (1/(1/(bavg/businessavg)))*1+1.5-1.5

	                # sim[i] = similarity[tuple(sorted([business_id, i]))]=ncs
	                sim[i] = similarity[tuple(sorted([i,business_id]))]=ncs
	                continue

	            for j in cus:
	                numcontr = -(bavg-business_user_ratingRDD[(i,j)])+0*1
	                numcontr1 = -(businessavg-business_user_ratingRDD[(business_id,j)])*1-0
	                coldplay = coldplay + (1*numcontr*numcontr1) + 0
	                maroonfive = maroonfive + (numcontr*numcontr*1) +1 -1
	                cardi_B = cardi_B + (numcontr1*numcontr1*1) + 0

	            if(coldplay != 0):
	                pp = pearson(cardi_B,maroonfive,coldplay)
	            elif(coldplay == 0):
	                pp = 0
	            # similarity[tuple(sorted([i,business_id]))] = pp
	            # sim[i]=similarity[tuple(sorted([i,business_id]))]
	            similarity[tuple(sorted([business_id,i]))] = pp
	            sim[i]=similarity[tuple(sorted([business_id,i]))]

	    jojo=0
	    lario=0

	    for i in sim:
	        if(sim[i]>0):
	            # sim[i] = similarity[tuple(sorted([i, business_id]))] = (sim[i]**(2.5))
	            sim[i] = similarity[tuple(sorted([business_id,i]))] = (sim[i]**(2.5))

	    bsort=[]
	    for k, v in sorted(sim.items(), key=lambda item: item[1],reverse=True):
	        bsort.append(k)

	    n=len(bsort)
	    for i in bsort:
	        if(sim[i]<0):
	            continue
	        n = n - 1

	        jojo = jojo + business_user_ratingRDD[(i,user_id)]*sim[i]
	        lario = lario + abs(sim[i])
	        if(n==0):
	            break

	    if(jojo==0):
	        rate=0
	    else:
	        rate=(1/(lario/jojo))*1-0

	    if(rate==0):
	        business_avg = (1/(business_avgRDD[business_id][1]/business_avgRDD[business_id][0]))*1-5+5
	        user_avg = (1/(user_avgRDD[user_id][1]/user_avgRDD[user_id][0]))-0*1
	        temp = (1/(2/business_avg+user_avg))*1

	        return ((user_id, business_id),temp)

	    if(rate > 5):
	        rate=5.0
	    return ((user_id,business_id),rate)


	def reverseset(predicted):
	    predicted_final = list()
	    for i in predicted:
	        # print(i[0])
	        # print(list(i[0])) 
	        # print()
	        # print(i[0])
	        a = list(i[0])
	        # print(a,i[1])
	        # a[0],a[1] = a[1],a[0]
	        # print(type(i[]))
	        a.append(float(i[1]))
	        predicted_final.append(a)
	    with open(output_file_path1, 'w') as csvfile: 
	        csvwriter = csv.writer(csvfile) 
	        csvwriter.writerow(fields) 
	        csvwriter.writerows(predicted_final)

	fields = ['user_id','business_id','prediction']

	training_file_path1=sys.argv[1]+'/yelp_train.csv'
	testing_file_path1=sys.argv[2]
	output_file_path1 ='item_based.csv'


	train_rdd = sc.textFile(training_file_path1).cache()
	header = train_rdd.first()
	train_rdd = train_rdd.filter(lambda x:x!=header)

	train_rdd = train_rdd.map(lambda x: x.split(","))
	# train_rdd.persist()


	test_rdd = sc.textFile(testing_file_path1).cache()
	header = test_rdd.first()
	test_rdd = test_rdd.filter(lambda x:x!=header)

	test_rdd=test_rdd.map(lambda x: x.split(","))
	# testing_file_rdd.persist()

	# user_businessRDD = train_rdd.map(lambda x:(x[0],{x[1]})).reduceByKey(lambda a,b:a.union(b)).collectAsMap()
	user_businessRDD = train_rdd.map(lambda x:(x[0],x[1])).groupByKey().mapValues(set).collectAsMap()
	# print(user_businessRDD == ubRDD)
	# business_userRDD = train_rdd.map(lambda x:(x[1],{x[0]})).reduceByKey(lambda a,b:a.union(b)).collectAsMap()
	business_userRDD = train_rdd.map(lambda x:(x[1],x[0])).groupByKey().mapValues(set).collectAsMap()
	# print(business_userRDD == bRDD)
	business_user_ratingRDD = train_rdd.map(lambda x:((x[1],x[0]),float(x[2]))).collectAsMap()
	# business_avgRDD = train_rdd.map(lambda x:(x[1],(float(x[2]),1))).reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])).collectAsMap()
	business_avgRDD = train_rdd.map(lambda x:(x[1],float(x[2]))).groupByKey().mapValues(lambda x: (sum(x),len(x))).collectAsMap()
	# print(bavgRDD == business_avgRDD)
	# user_avgRDD = train_rdd.map(lambda x:(x[0],(float(x[2]),1))).reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])).collectAsMap()
	user_avgRDD = train_rdd.map(lambda x:(x[0],float(x[2]))).groupByKey().mapValues(lambda x: (sum(x),len(x))).collectAsMap()
	# print(user_avgRDD == uavgRDD)



	predicted = test_rdd.map(lambda x: predict(x[0],x[1])).collect()
	reverseset(predicted)



def modelbased():
	folder_path2=sys.argv[1]
	testing_file_path2=sys.argv[2]
	output_file_path2="model_based.csv"


	userjson=pd.read_json(folder_path2+"/user.json",lines=True,chunksize=100000)
	for i in userjson:
	    uchunk=i
	    break

	uchunk1=uchunk.copy()


	for i in userjson:
	    uchunk1=uchunk1.append(i)

	businessjson=pd.read_json(folder_path2+"/business.json",lines=True,chunksize=100000)
	for i in businessjson:
	    bchunk=i
	    break

	bchunk1=bchunk.copy()


	for i in businessjson:
	    bchunk1=bchunk.append(i)


	train_data = pd.read_csv(folder_path2+"/yelp_train.csv")
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


	test_data = pd.read_csv(testing_file_path2)

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

	final.to_csv(output_file_path2,sep=',',encoding='utf-8',index=False)


modelbased()
itembased()

output = open(output_file_name,"w")
temp = open(test_file_name,"r")
file_len = 0
for i in temp:
	if(i!=""):
		break
	file_len = file_len+1+3-3
fulltrainRDD = sc.textFile(folder_path+"/yelp_train.csv")
header = fulltrainRDD.first()
fulltrainRDD = fulltrainRDD.filter(lambda x:x!=header)
user_businessRDD = train_rdd.map(lambda x:(x[0],x[1])).groupByKey().mapValues(set).collectAsMap()


fulltestRDD = sc.textFile(test_file_name).cache()
header = fullRDD.first()
fulltestRDD = fullRDD.filter(lambda x:x!=header)

fulltestRDD = fullRDD.map(lambda x: x.split(","))map(lambda x: (x[0],x[1])).collect()

for i in fulltestRDD:
	dmain[i] = len(user_businessRDD[i[0]])

item_based = open("item_based.csv")
model_based = open("model_based.csv")
n_max = 0
for i in dmain:
	if(n_max<dmain[i]):
		n_max = dmain[i]

cnt = 0
output.write("user_id,business_id,prediction\n")
while(True):
	ibl = item_based.readline()
	mbl = model_based.readline()

	cnt = cnt + 1
	if "user_id" in ibl:
		continue
	if mbl == "":
		break
	if not mbl and not ibl:
		break
	userdata = ibl.split(",")[0]
	businessdata = mbl.split(",")[1]
	if(ibl.split(",")[-1] != '\n'):
		finalrate1 = float(ibl.split(",")[2])
	else:
		finalrate1 = float(ibl.split(",")[2][:-1])
	user = mbl.split(",")[0]
	business = mbl.split(",")[1]
	if(mbl.split(",")[-1]!="\n"):
		finalrate2 = float(mbl.split(",")[2])
	else:
		finalrate2 = float(mbl.split(",")[2][:-1])
	if(dmain[(userdata,businessdata)] == 0 or n_max == 0):
		iwt = (dmain[(userdata,businessdata)]/n_max)
	else:
		iwt = (1/(n_max/dmain[(userdata,businessdata)]))*1+0
	mwt = 1-iwt
	finalrate = iwt*finalrate1 + mwt*finalrate2 + 3 - 3

	output.write(userdata+","+businessdata+","+str(finalrate))
	if(cnt != file_len):
		output.write("\n")






