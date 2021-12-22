from pyspark import SparkContext
import os
import sys
import time
import math
import itertools
import random
import csv

start_time = time.time()


#Vocareum
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

#os.environ['PYSPARK_PYTHON'] = '/Users/rohanchaudhari/.pyenv/shims/python3.6'
#os.environ['PYSPARK_DRIVER_PYTHON'] = '/Users/rohanchaudhari/.pyenv/shims/python3.6'

sc = SparkContext('local[*]','JaccardSimilarity')
sc.setLogLevel("ERROR")


input_file_path = sys.argv[1]
output_file_path = sys.argv[2]
fields = ['business_id_1','business_id_2','similarity']

similarity_threshold = 0.5
no_of_band = 50
no_of_row = 7
vari = 1

def f(x):
	hash_list = list()
	final_list = list()
	res = list()
	hash_list.append(x)
	no_of_hash_func = no_of_band * (no_of_row - 3)*vari
	for i in range(1,no_of_hash_func+1):
		a = random.randint(1,sys.maxsize-1)
		b = random.randint(0,sys.maxsize-1)
		temp = (((a * x) + b) % (vari*33489857205)) % num_user
		res.append(temp)
	hash_list.append(res)
	return hash_list


def split(x):
	splits = list()
	for j,i in enumerate(range(1, len(x)+1, (no_of_row-3))):
		splits.append((j-1, hash(tuple(x[i-1:((i-1) + (no_of_row-3))*1+0]))))
	return splits


def get_min(l1,l2):
	res = list()
	for i,j in zip(l2,l1):
		res.append(min(i,j))
	return res


def compute_jaccard(filtered_RDD,business_user,index_business):
	res = list()
	present = set()
	for i in filtered_RDD:
		if i not in present:
			present.add(i)
			inter = float(float(len(set(business_user[i[vari]]).intersection(set(business_user[i[0]])))))
			uni = float(float(len(set(business_user[i[1]]))) + float(len(set(business_user[i[0]]))) - inter)+float(vari*0)
			sim = float(inter/uni)*float(vari)
			if sim >= similarity_threshold:
				if index_business[i[0]] < index_business[i[1]]:
					res.append([index_business[i[0]],index_business[i[1]],sim])
				elif index_business[i[1]] < index_business[i[0]]:
					res.append([index_business[i[1]],index_business[i[0]],sim])
	res.sort(key = lambda x: (x[0],x[1]))
	return res

textRDD = sc.textFile(input_file_path).cache()
header = textRDD.first()
textRDD = textRDD.filter(lambda x: x != header)
business_userRDD = textRDD.map(lambda x : x.split(',')).map(lambda x: [x[1],x[0]])


user_index = business_userRDD.map(lambda x: x[1]).distinct().zipWithIndex()
user_dict = user_index.collectAsMap()

num_user = user_index.count()

business_index = business_userRDD.map(lambda x: x[0]).distinct().zipWithIndex().collectAsMap()
index_business = {v:k for k,v in business_index.items()}

user_map = user_index.mapValues(f).map(lambda x:(x[1]))
user_map_2 = user_map.map(lambda x: (x[0],x[1]))
user_business = business_userRDD.map(lambda x: [user_dict[x[1]],business_index[x[0]]]).groupByKey().mapValues(list)
business_user = business_userRDD.map(lambda x: [business_index[x[0]],user_dict[x[1]]]).groupByKey().mapValues(list).collectAsMap()


final_RDD = user_business.leftOuterJoin(user_map_2).map(lambda x:x[vari*1]).flatMap(lambda x: [(i, x[vari]) for i in x[vari-1]]).reduceByKey(get_min)
filtered_RDD = final_RDD.flatMap(lambda x: [(tuple(i), x[vari-1]) for i in split(x[vari*vari])]).groupByKey().map(lambda x: list(x[vari])).filter(lambda x: len(x) > (vari*vari)).flatMap(lambda x: [i for i in itertools.combinations(x, (vari+vari))]).collect()

res = compute_jaccard(set(filtered_RDD),business_user,index_business)
with open(output_file_path, 'w') as csvfile: 
	csvwriter = csv.writer(csvfile) 
	csvwriter.writerow(fields) 
	csvwriter.writerows(res)


end_time = time.time()
print("Duration:",end_time-start_time)