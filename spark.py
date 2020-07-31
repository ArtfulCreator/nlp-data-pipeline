#!/usr/bin/env python
# coding: utf-8

# In[20]:


import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkFiles
import pyspark.sql
import pyspark.sql.functions as F
from pyspark.sql.functions import explode,monotonically_increasing_id
from pyspark.sql.window import Window
import sys
import os

print('###### LIBRARIES IMPORTED ######')
# In[15]:


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell'


# In[4]:


sc = pyspark.SparkContext()
spark = SparkSession(sc)

#SparkFiles.get('transcribe_1568382278.json')

print('------------- GOT THE FILES --------------')


# In[5]:


#sc.stop()


# In[6]:


df = spark.read.json('s3a://transcribe-output-bucket/transcribe_1150-Simple-No-1_mixdown.wav1569007017.json')
print('$$$$$$$$$$ CREATED THE DATAFRAME $$$$$$$$$$$$$')

channelDF = (
    df.select('results.channel_labels','results.items')
    .select('channel_labels')
    .select('channel_labels.channels')
    .select(explode('channels'))
    .select('col.channel_label','col.items.start_time')
    .select('channel_label',explode('start_time').alias('start_time'))
   #.show()
)

print('-------- CREATED THE CHANNELDF DATAFRAME ---------')

# In[7]:


contentDF = (
    df.select(explode('results.items'))
    .select(F.col('col.start_time').alias('start_time'),F.col('col.type').alias('type'),explode('col.alternatives'))
    .select('start_time','type','col.content')
  #  .show()
)


# In[8]:


joinedDF = contentDF.join(channelDF,on='start_time',how='left')
df_index = joinedDF.select("*").withColumn("id", monotonically_increasing_id())
jobdf = df.select('jobName')
df_all = df_index.crossJoin(jobdf)
df_lag = df_all.withColumn('prev_channel',
                        F.lag(df_all['channel_label'])
                                 .over(Window.orderBy('id')))
df2 = (
    df_lag.select('id',
                  F.when(df_lag['channel_label'].isNull(), df_lag['prev_channel'])
                  .otherwise(df_lag['channel_label']).alias('channel_label'),
                  'start_time',
                  'content',
                  'type',
                  'jobName'
                 )
)



# In[22]:



df2.show(30)


# In[75]:


df2.write.json("s3a://transcribe-output-bucket/")

# In[692]:


sc.stop()


# In[ ]:
