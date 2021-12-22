from pyspark import SparkContext
import os
import json
import sys
import time
start = time.time()
#Local System
os.environ['PYSPARK_PYTHON'] = '/Users/rohanchaudhari/.pyenv/shims/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Users/rohanchaudhari/.pyenv/shims/python3.6'

#Vocareum
# os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
# os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

sc = SparkContext('local[*]','DataExploration')
# sc.setLogLevels("ERROR")
input_file_path = '/usr/local/Cellar/apache-spark@2.4.6/2.4.6/bin/yelp_academic_dataset_review.json'
# input_file_path = '/usr/local/Cellar/apache-spark@2.4.6/2.4.6/bin/test_review.json'
output_file_path = './task1output1.json'

# input_file_path = sys.argv[1]
# output_file_path = sys.argv[2]
# print(input_file_path)
# print(output_file_path)
textRDD = sc.textFile(input_file_path).map(lambda x: json.loads(x)).cache()

final = {}
# Task 1 : A
n_reviews = textRDD.map(lambda x:x['review_id']).count()
# print(n_reviews)
final["n_review"] = n_reviews

# # Task 1 : B
n_reviews_20181 = textRDD.map(lambda x:x['date'].split("-"))
n_reviews_2018 = n_reviews_20181.filter(lambda x:"2018" in x).count()
# print(n_reviews_2018)
final["n_review_2018"] = n_reviews_2018

# # Task 1 : C
n_user1 = textRDD.map(lambda x:x['user_id'])
n_user = n_user1.distinct().count()
# print(n_user)
final["n_user"] = n_user

# #Task 1 : D
top10_user = textRDD.map(lambda x: (x['user_id'],1)).groupByKey().mapValues(len).takeOrdered(10, key = lambda x: [-x[1],x[0]])
top10_user = list(map(list, top10_user))
# print(top10_user)
final["top10_user"] = top10_user

# #Task 1 : E
n_business1 = textRDD.map(lambda x:x['business_id'])
n_business = n_business1.distinct().count()
# print(n_business)
final["n_business"] = n_business

# # #Task 1 : F
top10_business = textRDD.map(lambda x: (x['business_id'],1)).groupByKey().mapValues(len).takeOrdered(10, key = lambda x: [-x[1],x[0]])
top10_business = list(map(list, top10_business))
# print(top10_business)
final["top10_business"] = top10_business

with open(output_file_path, 'w') as f:
	json.dump(final, f, indent = 4)
end = time.time()
print(end - start)