#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sys
import os 
import pyspark
import csv 
import json


conf=pyspark.SparkConf()
sc=pyspark.SparkContext()
review_path=sys.argv[1]
business_path=sys.argv[2]

review=sc.textFile(review_path)
business=sc.textFile(business_path)
b_temp=business.map(lambda x: json.loads(x))
r_temp=review.map(lambda x: json.loads(x))

def func(x,b):
    if x[0] in b:
        return True
    else:
        return False
    
    
b=b_temp.filter(lambda x: x['state']=='NV').map(lambda x: x['business_id']).collect()
ans=r_temp.map(lambda x:(x['business_id'],x['user_id'])).filter(lambda x: func(x,b)).collect()





master=[]
temp={}

for item in ans:
    temp['user_id']=item[1]
    temp['business_id']=item[0]
    master.append(temp)
    temp={}
csv_columns=['user_id','business_id']    
filename='file.csv'


with open(filename,'w') as file:
    w=csv.DictWriter(file, fieldnames=csv_columns)
    w.writeheader()
    for data in master:
        w.writerow(data)
    
    

