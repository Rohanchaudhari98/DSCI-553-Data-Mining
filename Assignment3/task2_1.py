from pyspark import SparkConf, SparkContext
import sys
import math

import time
import csv


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
    with open(output_file_path, 'w') as csvfile: 
        csvwriter = csv.writer(csvfile) 
        csvwriter.writerow(fields) 
        csvwriter.writerows(predicted_final)

start = time.time()
fields = ['user_id','business_id','prediction']
sc = SparkContext(conf=SparkConf().setAppName("task1").setMaster("local[*]"))
sc.setLogLevel("OFF")


training_file_path=sys.argv[1]
testing_file_path=sys.argv[2]
output_file_path=sys.argv[3]


train_rdd = sc.textFile(training_file_path).cache()
header = train_rdd.first()
train_rdd = train_rdd.filter(lambda x:x!=header)
train_rdd = train_rdd.map(lambda x: x.split(","))


test_rdd = sc.textFile(testing_file_path).cache()
header = test_rdd.first()
test_rdd = test_rdd.filter(lambda x:x!=header)
test_rdd=test_rdd.map(lambda x: x.split(","))


user_businessRDD = train_rdd.map(lambda x:(x[0],x[1])).groupByKey().mapValues(set).collectAsMap()
business_userRDD = train_rdd.map(lambda x:(x[1],x[0])).groupByKey().mapValues(set).collectAsMap()
business_user_ratingRDD = train_rdd.map(lambda x:((x[1],x[0]),float(x[2]))).collectAsMap()
business_avgRDD = train_rdd.map(lambda x:(x[1],float(x[2]))).groupByKey().mapValues(lambda x: (sum(x),len(x))).collectAsMap()
user_avgRDD = train_rdd.map(lambda x:(x[0],float(x[2]))).groupByKey().mapValues(lambda x: (sum(x),len(x))).collectAsMap()



predicted = test_rdd.map(lambda x: predict(x[0],x[1])).collect()
reverseset(predicted)

end = time.time()
print("Duration: ",(end-start))

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
    rmse+=math.pow(float(l1.split(",")[2][:-1])-float(l2.split(",")[2][:-1]),2)
    if not l1 and not l2:
        break

rmse=math.sqrt(rmse/n)

print(rmse)
