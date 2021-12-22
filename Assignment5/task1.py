from blackbox import BlackBox
import binascii
import sys
import math
import random
import time
import os
import csv


start_time = time.time()
input_filename = sys.argv[1]
stream_size = sys.argv[2]
num_of_asks = sys.argv[3]
output_filename = sys.argv[4]
GLOBAL_FILTER_BIT_ARRAY_LENGTH = 69997
NO_OF_HASH_FUNCTIONS = 8
a = [7, 3, 11, 17, 2, 13, 19, 23]
b = [34, 7717, 5837, 8147, 874, 457, 3529, 15]
c = [82163,82171,82183,82189,82193,82421,82457,82463,82469,82471,82483,82487,82493,82499,82507,82529,82531,82549,82559,82561,82567,82571,82591,82601,82609,82613,82619,82633,82651,82657,82699,82721,82723,82727,82729,82757,82759,82763,82781,82787,82793,82799,82811,82813,82837,82847]
bit_array = [0] * GLOBAL_FILTER_BIT_ARRAY_LENGTH
counter = 0
finalres = list()
user_set = set()
def myhashs(s):
	lst = []
	for i in range(0,NO_OF_HASH_FUNCTIONS):
		temp = (((a[i] * int(binascii.hexlify(s.encode('utf8')),16)) + b[i]) % random.choice(c)) % GLOBAL_FILTER_BIT_ARRAY_LENGTH
		lst.append(temp)
	return lst

def bloomfilter(x):
	global counter
	global finalres
	global user_set
	global bit_array
	flag = 0
	tpcnt = 0
	fpcnt = 0
	total = 0
	for i in x:
		res = myhashs(i)
		for j in res:
			if bit_array[j] == 1:
				flag = 1
			else:
				flag = 0
				break
		if flag == 1:
			if i in user_set:
				tpcnt = tpcnt + 1
			else:
				fpcnt = fpcnt + 1
		for j in res:
			bit_array[j] = 1
		user_set.add(i)
	if fpcnt == 0 and tpcnt == 0:
		fpr = float(0.0)
	else:
		fpr = float(fpcnt/(tpcnt+fpcnt))
	finalres.append([counter,fpr])
	counter = counter+1

bx = BlackBox()
for _ in range(int(num_of_asks)):
	stream_users = bx.ask(input_filename,int(stream_size))
	bloomfilter(stream_users)
fields = ['Time','FPR']
with open(output_filename,'w') as f:
	csvwriter = csv.writer(f)
	csvwriter.writerow(fields)
	csvwriter.writerows(finalres)

end_time = time.time()
print("Duration: ", (end_time-start_time))