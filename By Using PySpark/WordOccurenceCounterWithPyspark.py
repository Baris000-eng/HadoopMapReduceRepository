#!/usr/bin/env python
# coding: utf-8

# In[18]:


#necessary imports for creating a spark session
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


# In[19]:


mySparkSession=SparkSession.builder    .master("local[*]")    .appName("WordCount")    .getOrCreate()


# In[20]:


mySparkContext=mySparkSession.sparkContext #creating spark context from spark session
file_with_random_data = "randomText.txt"
my_dataset = mySparkContext.textFile(file_with_random_data) #reading the txt file
my_dataset.take(3)


# In[21]:


my_dataset=my_dataset.flatMap(lambda row: row.split(" "))  #splitting the txt file with blank.
print(my_dataset)


# In[22]:


my_dataset.take(25)


# In[23]:


my_dataset = my_dataset.filter(lambda myWord: myWord!='') #removing the words which are empty space.
print(my_dataset)


# In[24]:


my_dataset.take(25)


# In[25]:


print(type(my_dataset.take(25)))
print(type(my_dataset))


# In[26]:


my_dataset=my_dataset.map(lambda w:(w,1)) #mapping (assigning the count of 1 to. each word)
my_dataset.take(10)


# In[27]:


#my_dataset=my_dataset.map(lambda w:(w,1)) #mapping 
#my_dataset.take(10)


# In[28]:


word_counts = my_dataset.reduceByKey(lambda v1,v2:(v1+v2)) 
#reducing the counts of words by summing the word counts one by one.


# In[29]:


word_counts.take(40)
unique_word_num = word_counts.count()
print("Total unique word number: " + str(unique_word_num))
print(word_counts)
print(word_counts.take(unique_word_num)[0][0])

print()
print()
print()
max_occurence_num = -1 * (9 * int(pow(10,5)))
max_occuring_word = "Germany"

min_occurence_num = 9 * int(pow(10,5))
min_occuring_word = "Denmark"


print("******************* Some Statistics***********************")
for j in range(0,unique_word_num):
    word = word_counts.take(unique_word_num)[j][0] 
    word_count = word_counts.take(unique_word_num)[j][1] 
    if(word_count >= max_occurence_num): #finding max occurence num for a word and the word which has the maximum number of occurence.
        max_occurence_num = word_count
        max_occuring_word = word
    elif(word_count <= min_occurence_num):  #finding min occurence num for a word and the word which has the minimum number of occurence.
        min_occurence_num = word_count
        min_occuring_word = word
    word = str(word) #convert to string
    word_count = str(word_count) #convert to string
    print(" "+word+" appears => "+word_count+" times in the randomText.txt file")


print()    
min_occurence_num = str(min_occurence_num)
print("The word "+min_occuring_word+" has the minimum occurence of "+min_occurence_num+"")
max_occurence_num = str(max_occurence_num)
print("The word "+max_occuring_word+" has the maximum occurence of "+max_occurence_num+"")
print("******************* Some Statistics ***********************")
print()
print()
print()




print(word_counts.take(10))
print(word_counts.take(1)[0][0])  
print(word_counts.take(1)[0][1])


# In[30]:


word_counts_sorted =my_dataset.reduceByKey(lambda c1,c2:(c1+c2)).sortByKey()


# In[31]:


word_counts_sorted.take(30)


# In[ ]:





# In[ ]:





# In[ ]:




