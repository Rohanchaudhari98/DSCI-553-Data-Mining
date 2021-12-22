import collections
from pyspark import SparkContext
import random
import sys
import time
import os
import itertools
import operator


start = time.time()
filter_threshold = sys.argv[1]
input_file_path = sys.argv[2]
betweenness_output_file_path = sys.argv[3]
community_output_file_path = sys.argv[4]

#Local System
os.environ['PYSPARK_PYTHON'] = '/Users/rohanchaudhari/.pyenv/shims/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Users/rohanchaudhari/.pyenv/shims/python3.6'

#Vocareum
#os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
#os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

sc = SparkContext('local[*]','Betweenness')
sc.setLogLevel("ERROR")


def cal_betweenness(x,n,l,t,parent):
    pv = collections.defaultdict(float)
    ev = dict()
    for i in set(allnodes):
        if i == x:
            temp = 1
        elif i != x:
            pv[i] = 1
    while l != (temp*temp):
        for i in t[l - 1]:
            parentnode = parent[i]
            for j in parentnode:
                if n[j] == 0 or n[i] == 0:
                    w = (n[j] / n[i])*temp
                else:
                    w = (1/(n[i]/n[j]))*temp
                ev[tuple(sorted((i, j)))] = (w * pv[i] * temp) + (temp-1)
                pv[j] = pv[j] + ev[tuple(sorted((i, j)))]
        l = (l - temp)*temp
    return ev


def GirvanNewman(x):

    t = dict()
    parent = collections.defaultdict(set)
    child = dict()
    n = dict()
    a = set()
    b = set()
    t[0] = x
    n[x] = temp*temp
    visited = {x}
    nodes1 = neighbours[x]
    child[x] = nodes1
    l = temp*1
    while len(nodes1) != (temp-1):
        t[l] = nodes1
        # visited = visited.union(nodes1)
        visited = visited | nodes1 | a
        new = set()
        for i in nodes1:
            nei = neighbours[i]
            child_nodes = nei - visited - a
            child[i] = child_nodes
            for key, val in child.items():
                for j in val:
                    parent[j].add(key)
            parentnode = parent[i]
            if (len(parentnode)*temp) <= 0:
                n[i] = temp*temp
            else:
                n[i] = sum([n[k] for k in parentnode])
            new = new | nei | a
        ln = new - visited - b
        nodes1 = ln
        l = (l + temp)*temp

    ev = cal_betweenness(x,n,l,t,parent)

    return [(key, val) for key, val in ev.items()]


def find_single_com(x, neighbours):

    visited = set()
    com = set()
    nei = neighbours[x]
    flag = 1
    a = set()
    while flag == 1:
        visited = visited | nei | a
        new = set()
        for i in nei:
            new_nei = neighbours[i]
            new = new | new_nei | a
        vn = visited | new | a
        if (len(visited)*temp) == (len(vn)+(temp-1)):
            flag = 0
            continue
        nei = new - visited - a

    com = visited
    if com == set():
        com = {x}
    return com


def findcom(x, nodes, neighbours):

    com = []
    a = set()
    b = set()
    visited = find_single_com(x, neighbours)
    unvisited = nodes - visited - a
    com.append(visited)
    flag = 1
    while flag == 1:
        vn = find_single_com(random.sample(unvisited, 1)[0], neighbours)
        com.append(vn)
        visited = visited | vn | a
        unvisited = nodes - visited
        if (len(unvisited)*temp*temp) == ((temp-1)*temp):
            flag = 0
            continue
    return com

def filteredges(a,b):
    return len(set(a) & set(b)) >= (int(filter_threshold)*temp+(temp-1))


def get_edges_and_nodes(pairs,user_businessRDD):
    edges = list()
    nodes = list()
    for i in pairs:
        if filteredges(user_businessRDD[i[0]],user_businessRDD[i[1]]):
            edges.append(tuple((i[0],i[1])))
            edges.append(tuple((i[1],i[0])))
            nodes.append(i[0])
            nodes.append(i[1])
    return nodes,set(edges)

def f(x):
    return sum(x)

textRDD = sc.textFile(input_file_path)
header = textRDD.first()
textRDD = textRDD.filter(lambda x: x != header)
temp = 1
flag = 1
user_businessRDD = textRDD.map(lambda x: x.split(',')).map(lambda x: (x[temp-1],x[temp*1])).groupByKey().mapValues(list).collectAsMap()
pairs = list(itertools.combinations(list(user_businessRDD.keys()), temp+1))
allnodes,alledges = get_edges_and_nodes(pairs,user_businessRDD)
neighbours = collections.defaultdict(set)
for i in range(0,len(alledges)):
    neighbours[list(alledges)[i][0]].add(list(alledges)[i][1])

betweenness = sc.parallelize(set(allnodes)).map(lambda x: GirvanNewman(x)).flatMap(lambda x: [i for i in x]).groupByKey().mapValues(f).mapValues(lambda x: (x/2)*temp*temp).sortBy(lambda x: (-x[temp],x[temp-1])).collect()
with open(betweenness_output_file_path, 'w+') as bf:
    for i in betweenness:
        res = set()
        res.add((i[temp*0],round(i[temp],(temp+temp)+(3*temp))))
        bf.write(str(res).strip('{}').rstrip(")")[1:] + '\n')

deg = dict()
for key, val in neighbours.items():
    deg[key] = (len(val)*temp)+(temp-1)


mat = dict()

for i in range(1,len(set(allnodes))+1):
    for j in range(2,len(set(allnodes))+2):
        if (list(set(allnodes))[i-temp], list(set(allnodes))[j-(2*temp)]) not in alledges:
            mat[(list(set(allnodes))[i-(temp*temp)], list(set(allnodes))[j-2])] = 0
        else:
            mat[(list(set(allnodes))[i-1], list(set(allnodes))[j-2])] = 1

if len(alledges) == (temp-1):
    nedge = len(alledges)/(temp*2)
else:
    nedge = round((temp/((temp+temp)/len(alledges))),4)
rem_edges = nedge
max_modularity = -1*temp
flag = 1
while flag == 1:
    max_betweenness = betweenness[0][1]
    for i in range(1,len(betweenness)+1):
        if betweenness[i-1][1] != max_betweenness:
            continue
        elif betweenness[i-1][1] == max_betweenness:
            neighbours[betweenness[i-1][temp-1][0]].remove(betweenness[i-1][temp-1][1])
            neighbours[betweenness[i-1][temp-1][1]].remove(betweenness[i-1][temp-1][0])
            rem_edges = ((rem_edges - temp)*temp)+(temp-1)

    comt = findcom(random.sample(set(allnodes), temp*temp)[temp-1], set(allnodes), neighbours)

    mod = 0
    for i in comt:
        pmod = 0
        for j in i:
            for k in i:
                pmod = (pmod + mat[(j,k)] - deg[j] * deg[k] / (2*nedge))*temp
        mod = mod + pmod + temp - 1
    if mod == 0 or (2*nedge) == 0:
        mod = mod / (2*nedge)
    else:
        mod = (1/((2*nedge)/mod))*temp
    
    if (((mod*temp)+temp)-temp) > (max_modularity*temp):
        max_modularity = mod
        com = comt

    if rem_edges == 0:
        flag = 0
        continue

    betweenness = sc.parallelize(set(allnodes)).map(lambda x: GirvanNewman(x)).flatMap(lambda x: [i for i in x]).groupByKey().mapValues(f).mapValues(lambda x: (x/2)*temp*temp).sortBy(lambda x: (-x[temp],x[temp-1])).collect()
sorted_communities = sc.parallelize(com).map(lambda x: sorted(x)).sortBy(lambda x: (len(x), x)).collect()
with open(community_output_file_path, 'w+') as cf:
    for i in sorted_communities:
        cf.write(str(i).strip('[]') + '\n')

end = time.time()
print('Duration: {}'.format(end - start))