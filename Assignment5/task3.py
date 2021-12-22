import sys, time, random, math
import binascii
from blackbox import BlackBox
import os
import csv


def fixed_size_sampling(stream_users):
    global sequence_number
    global flag
    global counter
    global finalres
    global previous
    lst = list()
    lst1 = list()
    stream_users = stream_users[0:100]
    if flag == 0:
        flag = 1
        previous = stream_users
        lst = previous
    elif flag == 1:
        for i in stream_users:
            counter = counter+1
            probability = float(100.0/counter)
            if random.random() < probability:
                pos = random.randint(0,len(previous)-1)
                deleted_user = previous.pop(pos)
                previous.insert(pos,i)
                lst = previous
            else:
                lst = previous
    lst1.append(counter)
    lst1.extend([lst[0],lst[20],lst[40],lst[60],lst[80]])
    finalres.append(lst1)



if __name__ == "__main__":

    start_time = time.time()
    random.seed(553)
    sequence_number = 100
    flag = 0
    counter = 100
    finalres = list()
    previous = list()
    input_filename = sys.argv[1]
    stream_size = sys.argv[2]
    num_of_asks = sys.argv[3]
    output_filename = sys.argv[4]

    bx = BlackBox()
    for _ in range(int(num_of_asks)):
        stream_users = bx.ask(input_filename,int(stream_size))
        fixed_size_sampling(stream_users)
    fields = ['seqnum','0_id','20_id','40_id','60_id','80_id']

    with open(output_filename,'w') as f:
        csvwriter = csv.writer(f)
        csvwriter.writerow(fields)
        csvwriter.writerows(finalres)

    
    end_time = time.time()
    print("Duration: ",end_time - start_time)