from graphframes import GraphFrame
from pyspark import SparkContext
from pyspark.sql import SparkSession
import itertools
import os
import sys
import time

os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.8.2-spark3.1-s_2.12")

filter_threshold = sys.argv[1]
input_file_path = sys.argv[2]
community_output_file_path = sys.argv[3]
sc = SparkContext('local[*]','CommunityDetection')
sparkSession = SparkSession(sc)
sc.setLogLevel("ERROR")
temp = 1

def filteredges(a,b):
    return ((len(set(a) & set(b)))*temp) >= (int(filter_threshold)*temp+(temp-1))

def get_edges_and_nodes(pairs,user_businessRDD):
    edges = list()
    nodes = list()
    for i in pairs:
        if filteredges(user_businessRDD[i[temp*0]],user_businessRDD[i[1]]):
            edges.append(tuple((i[temp-1],i[temp*temp])))
            edges.append(tuple((i[temp],i[0*temp])))
            nodes.append(i[1-temp])
            nodes.append(i[(temp*temp)+(temp-1)])
    return set(nodes),edges


textRDD = sc.textFile(input_file_path)
header = textRDD.first()
textRDD = textRDD.filter(lambda x: x != header)
user_businessRDD = textRDD.map(lambda x: x.split(',')).map(lambda x: (x[temp-1],x[temp*1])).groupByKey().mapValues(list).collectAsMap()

pairs = list(itertools.combinations(list(user_businessRDD.keys()), temp+1))
allnodes,alledges = get_edges_and_nodes(pairs,user_businessRDD)
nodesdf = sc.parallelize(list(allnodes)).map(lambda x: (x,)).toDF(['id'])
edgedf = sc.parallelize(alledges).toDF(["src", "dst"])
g = GraphFrame(nodesdf, edgedf)
g_res = g.labelPropagation(maxIter=(temp*5))
communities = g_res.rdd.map(lambda x: (x[temp+0], x[temp-1])).groupByKey().mapValues(list).map(lambda x: sorted(x[temp*1])).sortBy(lambda x: (len(x), x[(temp-1)])).collect()
with open(community_output_file_path,"w+") as f:
    for i in communities:
        f.write(str(i).strip('[]')+'\n')