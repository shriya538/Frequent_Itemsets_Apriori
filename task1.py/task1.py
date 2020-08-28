#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import os 
import time
import pyspark
from operator import add
from itertools import combinations
from collections import defaultdict
from pprint import pprint


conf=pyspark.SparkConf()
sc=pyspark.SparkContext()


# In[15]:



start= time.time()
case_number=int(sys.argv[1])
support=int(sys.argv[2])
input_path=sys.argv[3]
output_path=sys.argv[4]

def frequency_checker(dictionary,support):
  
    ans=[]
    for key in dictionary:
        if dictionary[key]>=support:
            ans.append(key)
    
    return ans
  

def next_level(frequentitems,support,n,allbasket,size):
    ns=support//n
    candidatelist=set()
    pcl=combinations(frequentitems,2)
    for c in pcl:
        c=c[0].union(c[1])
        if len(c)==size:
            candidatelist.add(frozenset(c))
    freq={}
    freq = defaultdict(lambda:0,freq)
  
    for candidate in candidatelist:
        for basket in range(len(allbasket)):
            if set(candidate).issubset(allbasket[basket]):
                freq[candidate]+=1
              
    
    final=frequency_checker(freq,ns)
    
    return final
                
                
        
def apriori_algorithm(file_chunk,support,n):
    ns=support//n
    chunk=list(file_chunk)
    count={}
    count=defaultdict(lambda:0,count)
    allbasket=[]
    frequentitems=[]
    for basket in chunk:
        allbasket.append(basket)
        for item in basket:
            count[item]+=1
            if count[item]==ns:
                frequentitems.append(set([item]))
                
            
            
    totalfrequent=[]
    
    size=2
    totalfrequent+=frequentitems
    for i in  range(1,len(frequentitems)):
        frequentitems=next_level(frequentitems,support,n,allbasket,size)
        totalfrequent+=frequentitems
        size+=1
    return totalfrequent


def mapper_function(out1,file_chunk):
    #chunk=list(file_chunk)
    #freq={}
    #freq=defaultdict(lambda:0,freq)
    #for i in range(len(out1)):
    #    for basket in range(len(chunk)):
    #        if (set(out1[i])).issubset(set(chunk[basket])):
    #                freq[out1[i]]+=1
           
    #ans=[]
    #for k in freq:
    #    ans.append((k,freq[k]))
    freq={}
    freq=defaultdict(lambda:0,freq)
    chunk=list(file_chunk)
    for candidate in out1: ###(list of frequent candidates from phase1 candidate is a tuple, item is a set (1,2,3,4) {1,2,3,4})
        for basket in chunk:
            if (set(candidate)).issubset(set(basket)):
                    freq[candidate]+=1
            
    ans=[]
    for k in freq:
        ans.append((k,freq[k]))
    
    return ans

def writeToFile(out_data_path,outstring1,outstring2):
    
    with open(out_data_path, 'w') as outfile:
        outfile.write('Candidates:' + '\n')
        outfile.write(outstring1)
        
        outfile.write('\n\n')
        outfile.write('Frequent Itemsets:' + '\n')
        outfile.write(outstring2)
    
    
    

def changeformat(data):
    outstring = ''
    size = 1

    for item in data:

        if len(item) == 1:
            outstring+=str("('" + item[0] + "'),")
            
        elif len(item) != size:
            outstring= outstring[:-1] +'\n\n' + (str(item) +',')
            size=len(item)
            
        else:
            outstring+=(str(item)+',')

    
    return outstring[:-1]


        
        



if case_number==1: 
    temp=sc.textFile(input_path)
    head=temp.first()
    rdd=temp.filter(lambda x: x!=head).map(lambda x: x.split(',')).map(lambda x:(str(x[0]),str(x[1]))).groupByKey().mapValues(lambda x: set(x)).map(lambda x: x[1])
    p=rdd.getNumPartitions()
    ##SON PHASE 1
    map1=rdd.mapPartitions(lambda x:apriori_algorithm(x,support,p)).map(lambda x:sorted(x)).map(lambda x:(tuple(x),1))
    reduce1=map1.groupByKey().map(lambda x: x[0]).collect() 
    output1=sorted(reduce1,key=(lambda x:(len(x),x)))
    ##SON PHASE 2
    map2=rdd.mapPartitions(lambda x: mapper_function(output1,x))
    reduce2=map2.reduceByKey(lambda a,b:a+b).filter(lambda x: x[1]>=support).map(lambda x:x[0]).collect()
    output2=sorted(reduce2,key=(lambda x:(len(x),x)))
    
    outstring1=changeformat(output1)
    outstring2=changeformat(output2)
    writeToFile(output_path,outstring1,outstring2)


elif case_number == 2:
    temp=sc.textFile(input_path)
    head=temp.first()
    rdd=temp.filter(lambda x: x!=head).map(lambda x: x.split(',')).map(lambda x:(str(x[1]),str(x[0]))).groupByKey().mapValues(lambda x: set(x)).map(lambda x: x[1])
    p=rdd.getNumPartitions()
    ##SON PHASE 1
    map1=rdd.mapPartitions(lambda x:apriori_algorithm(x,support,p)).map(lambda x:sorted(x)).map(lambda x:(tuple(x),1))
    reduce1=map1.groupByKey().map(lambda x: x[0]).collect() 
    output1=sorted(reduce1,key=(lambda x:(len(x),x)))
    ##SON PHASE 2
    map2=rdd.mapPartitions(lambda x: mapper_function(output1,x))
    reduce2=map2.reduceByKey(lambda a,b:a+b).filter(lambda x: x[1]>=support).map(lambda x:x[0]).collect()
    output2=sorted(reduce2,key=(lambda x:(len(x),x)))
    
    outstring1=changeformat(output1)
    outstring2=changeformat(output2)
    writeToFile(output_path,outstring1,outstring2)
  
    


end=time.time()
n=str(end-start)
print('Duration: '+ n)


# In[ ]:





# In[ ]:




