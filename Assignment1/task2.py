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
# input_file_path = '/usr/local/Cellar/apache-spark@2.4.6/2.4.6/bin/test_review.json'
input_file_path = '/usr/local/Cellar/apache-spark@2.4.6/2.4.6/bin/yelp_academic_dataset_review.json'
output_file_path = './task2output.txt'


# input_file_path = sys.argv[1]
# output_file_path = sys.argv[2]
# n_partitions = sys.argv[3]

final = {}
default = {}
customised = {} 
n_partitions = 3






start_time_def = time.time()
textRDD1 = sc.textFile(input_file_path).map(lambda x: json.loads(x)).map(lambda item: (item["business_id"],1)).cache()
# textRDD1 = textRDD1.partitionBy(int(n_partitions))
n_partitions1 = textRDD1.getNumPartitions()
# print(n_partitions1)
default["n_partition"] = int(n_partitions1)
n_items1 = textRDD1.glom().map(lambda x: len(x)).collect()
# print(n_items1)
default["n_items"] = n_items1

top10_business1 = textRDD1.map(lambda x: (x[0],1)).groupByKey().mapValues(len).sortByKey(True,1).takeOrdered(10, key = lambda x: -x[1])
top10_business1 = list(map(list, top10_business1))
end_time_def = time.time()
diff = end_time_def - start_time_def
default["exe_time"] = diff




def partitionFunc(x):
	return ord(x[0])

start_time_cus = time.time()
textRDD = sc.textFile(input_file_path).map(lambda x: json.loads(x)).map(lambda x: (x["business_id"],1)).cache()
textRDD = textRDD.partitionBy(int(n_partitions), partitionFunc)
n_partitions2 = textRDD.getNumPartitions()
customised["n_partition"] = int(n_partitions2)

n_items = textRDD.glom().map(lambda x: len(x)).collect()
customised["n_items"] = n_items
# print(n_items)


top10_business = textRDD.map(lambda x: (x[0],1)).groupByKey().mapValues(len).sortByKey(True,1).takeOrdered(10, key = lambda x: -x[1])
top10_business = list(map(list, top10_business))
end_time_cus = time.time()
diff = end_time_cus - start_time_cus
customised["exe_time"] = diff

final["default"] = default
final["customized"] = customised
print(final)
end = time.time()
print(end - start)

with open(output_file_path, 'w') as f:
	json.dump(final, f, indent = 4)