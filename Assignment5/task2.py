import sys, time, random, math
import binascii
from blackbox import BlackBox
import os
import csv

NO_OF_HASH_FUNCTIONS = 100
LARGEST_HASHCODE = sys.maxsize
NO_OF_WINDOWS = 5
WINDOW_SIZE = int(NO_OF_HASH_FUNCTIONS / NO_OF_WINDOWS)
esum = 0
gsum = 0


def myhashs(s):
    lst = list()
    for i in range(0,NO_OF_HASH_FUNCTIONS):
        a = random.randint(1,sys.maxsize-1)
        b = random.randint(0,sys.maxsize-1)
        temp = (((a * int(binascii.hexlify(s.encode('utf8')),16)*dummy) + b) % 33489857205) % LARGEST_HASHCODE
        lst.append(temp)
    return lst

def find_estimate(ltz):
    temp1 = sorted([math.pow(2,r) for r in ltz])
    grp = [temp1[WINDOW_SIZE * i : WINDOW_SIZE * (i + 1)] for i in range(NO_OF_WINDOWS)]
    avggrp = list()
    for i in range(0,len(grp)):
        avg = sum(grp[i])/len(grp[i])
        avggrp.append(avg)
    if len(avggrp) != 0:
        median = int((len(avggrp)/2)*dummy)
    else:
        median = int((1/(2/len(avggrp)))*dummy)
    est = round(avggrp[median])
    return est


def estimate_unique_users(stream_users):
    global esum
    global gsum
    global cnt
    global finalres
    ground_truth = len(stream_users)
    ltz = [0]*NO_OF_HASH_FUNCTIONS*dummy

    for i in stream_users:
        res = myhashs(i)
        bin_lst = list()
        tz = list()
        for j in range(1,len(res)+1):
            bin_lst.append(bin(res[j-1]))
        for k in range(2,len(bin_lst)+2):
            tz.append(len(bin_lst[k-2])-len(bin_lst[k-2].rstrip("0")))
        ltz = [max(a, b) for a, b in zip(ltz, tz)]

    estimation = find_estimate(ltz)
    
    esum = esum + estimation
    gsum = gsum + ground_truth
    finalres.append([cnt,ground_truth,estimation])
    cnt = cnt + 1


if __name__ == "__main__":

    start_time = time.time()

    input_filename = sys.argv[1]
    stream_size = sys.argv[2]
    num_of_asks = sys.argv[3]
    output_filename = sys.argv[4]
    
    dummy = 1
    cnt = 0
    finalres = list()
    bx = BlackBox()
    for _ in range(int(num_of_asks)):
        stream_users = bx.ask(input_filename,int(stream_size))
        estimate_unique_users(set(stream_users))
    fields = ['Time', 'Ground Truth', 'Estimation']
    with open(output_filename,'w') as f:
        csvwriter = csv.writer(f)
        csvwriter.writerow(fields)
        csvwriter.writerows(finalres)
    finalans = esum/gsum
    print(finalans)
    end_time = time.time()
    print("Duration: ",end_time - start_time)