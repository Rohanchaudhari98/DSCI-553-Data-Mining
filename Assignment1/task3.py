from pyspark import SparkContext
import os
import json
import sys
from operator import add,itemgetter
import time
from collections import OrderedDict,Counter, defaultdict
import functools
start = time.time()
#Local System
os.environ['PYSPARK_PYTHON'] = '/Users/rohanchaudhari/.pyenv/shims/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Users/rohanchaudhari/.pyenv/shims/python3.6'

#Vocareum
# os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
# os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

input_file_path1 = '/usr/local/Cellar/apache-spark@2.4.6/2.4.6/bin/yelp_academic_dataset_review.json'
input_file_path2 = '/usr/local/Cellar/apache-spark@2.4.6/2.4.6/bin/yelp_academic_dataset_business.json'
output_file_path = '/usr/local/Cellar/apache-spark@2.4.6/2.4.6/bin/task3output.txt'
output_file_pathjson = '/usr/local/Cellar/apache-spark@2.4.6/2.4.6/bin/task3output.json'
sc = SparkContext('local[*]','DataExploration')
# sc.setLogLevel("ERROR")
s = "city,stars\n"
file1 = open(output_file_path, 'r+')
file1.write(s)
final = {}

start_time = time.time()
textRDD1 = sc.textFile(input_file_path1).map(lambda x: json.loads(x))
textRDD2 = sc.textFile(input_file_path2).map(lambda x: json.loads(x))
city_business = textRDD2.map(lambda x: (x['business_id'],x['city']))
business_stars = textRDD1.map(lambda x: (x["business_id"],x["stars"]))
test = city_business.join(business_stars)
test1 = test.map(lambda x: x[1])
test2 = test1.reduceByKey(add)
test3 = test1.groupByKey().mapValues(len)
test4 = test2.join(test3)
test6 = test4.map(lambda x: (x[0], x[1][0]/x[1][1]))
end_time = time.time()
diff = end_time - start_time


tests6 = test6.collect()
for i in tests6:
	file1.write("%s,%s\n" %(i[0],i[1]))



#Python Version
start_time_python = time.time()
testpy = test6.collect()
lst = sorted(testpy, key= lambda x: (-x[1],x[0]))
first_ten = lst[:10]
end_time_python = time.time()
diff_python = end_time_python - start_time_python
final["m1"] = diff+diff_python



#Spark Version
start_time_spark = time.time()
# test7 = test4.map(lambda x: (x[0], x[1][0])).sortBy(lambda x:[-x[1], x[0]]).take(10)
# tests6 = test4.map(lambda x: (x[0], x[1][0]/x[1][1]))
# test8 = test6.sortBy(lambda x: [-x[1], x[0]]).take(10)
# print(test7)
test8 = test6.takeOrdered(10, key = lambda x: [-x[1],x[0]])
end_time_spark = time.time()
diff_spark = end_time_spark - start_time_spark
print("Spark",(diff+diff_spark))
final["m2"] = diff+diff_spark
final["reason"] = "Spark here is faster for the above mentioned query than Python since, Spark here sorts the data in parallel which drastically reduces the runtime."

with open(output_file_pathjson, 'w') as f:
	json.dump(final, f, indent = 4)


# tests6 = test4.map(lambda x: (x[0], x[1][0]/x[1][1])).collect()
# test8 = tests6.sortBy(lambda x: [-x[1], x[0]]).collect()

print("Python",(diff +diff_python))
print(first_ten)
print(test8)
