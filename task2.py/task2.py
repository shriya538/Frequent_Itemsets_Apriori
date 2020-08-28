#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import os 
import pyspark
import time
from operator import add
from itertools import combinations
from collections import defaultdict
from pprint import pprint

conf=pyspark.SparkConf()
sc=pyspark.SparkContext()


# In[47]:


start=time.time()
filter_threshold =int(sys.argv[1])
support= int(sys.argv[2])
input_data_path=sys.argv[3]
out_data_path=sys.argv[4]

def next_level(previous,support,level,allbasket):

    combinations_list=combinations(previous,2)
    next_level_candidates=set()
    for c in combinations_list:
        c=c[0].union(c[1])
        if len(c)==level:
            next_level_candidates.add(frozenset(c))
   
    
    freq={}
    freq=defaultdict(lambda:0,freq)
    ans=[]
    
    for candidate in next_level_candidates:
        for basket in allbasket:
            if set(candidate).issubset(basket):
                freq[candidate]+=1
                if freq[candidate]==support:
                    ans.append(candidate)
                    break
    return ans
                
    
       
            
    
def apriori_algorithm(file_chunk,ns):
    chunk=list(file_chunk)
    dictionary={}
    dictionary=defaultdict(lambda:0,dictionary)
    ktons=[]
    ans=[]
    allbasket=[]
    for basket in chunk:
        allbasket.append(basket)
        for item in basket:
            dictionary[item]+=1
    
    for key in dictionary:
        if dictionary[key] >= ns:
            ktons.append(set([key]))
    
    ans+=ktons
    levels=2
    
    for i in  range(1,len(ktons)):
        ktons=next_level(ktons,ns,levels,allbasket)
        levels+=1
        ans+=ktons
    return ans

def mapper_function(candidatekeys,file_chunk):
    chunk=list(file_chunk)
    freq={}
    freq=defaultdict(lambda:0,freq)
    for candidate in candidatekeys:
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

temp=sc.textFile(input_data_path)
head=temp.first()

temp1=temp.filter(lambda x: x!=head)
rdd=temp1.map(lambda x:x.split(',')).map(lambda y:(str(y[0]),str(y[1]))).groupByKey().map(lambda x: list(x[1])).filter(lambda x: len(x)>filter_threshold).map(set)



# In[48]:


num_Partitions=rdd.getNumPartitions()
ns=support//num_Partitions
##SON PHASE 1
map1=rdd.mapPartitions(lambda x:apriori_algorithm(x,ns)).map(lambda x:sorted(x)).map(lambda x:(tuple(x),1))
reduce1=map1.groupByKey().map(lambda x: x[0]).collect() 
output1=sorted(reduce1,key=(lambda x:(len(x),x)))
##SON PHASE 2
map2=rdd.mapPartitions(lambda x: mapper_function(output1,x))
reduce2=map2.reduceByKey(lambda a,b:a+b).filter(lambda x: x[1]>=support).map(lambda x:x[0]).collect()
output2=sorted(reduce2,key=(lambda x:(len(x),x)))
    
outstring1=changeformat(output1)
outstring2=changeformat(output2)
writeToFile(out_data_path,outstring1,outstring2)

end=time.time()
n=str(end-start)
print('Duration: '+ n)




# In[52]:





# In[ ]:





# In[ ]:




