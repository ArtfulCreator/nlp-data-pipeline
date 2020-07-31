import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkFiles
import pyspark.sql
import pyspark.sql.functions as F
from pyspark.sql.functions import explode,monotonically_increasing_id
from pyspark.sql.window import Window
import sys
import os

inputBucketLocation = sys.argv[1]
outputFileLocation = sys.argv[2]

print('----------- Imported Libraries -------------')

sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession(sc)

print('----------- Reading Into DataFrame -------------')
# df = spark.read.json('s3a://swill-test-output/')
df = spark.read.json(inputBucketLocation)
print('----------- Creating ChannelDF -------------')
channelDF = (df.select('jobName','results.channel_labels')
          .select('jobName','channel_labels')
          .select('jobName','channel_labels.channels')
          .select('jobName',explode('channels').alias('channels'))
          .select('jobName','channels.channel_label','channels.items.start_time')
          .select('jobName','channel_label',explode('start_time').alias('start_time'))
         ).na.fill('-1')

print('----------- Creating ContentDF -------------')
contentDF = (df.select('jobName',explode('results.items'))
             .select('jobName',F.col('col.start_time').alias('start_time'),F.col('col.type').alias('type'),explode('col.alternatives'))
             .select('jobName','start_time','type','col.content')
             .select("*").withColumn("id", monotonically_increasing_id())
            ).na.fill('-2')

print('----------- Joining Dataframes -------------')
joinedDF = contentDF.join(channelDF, on = ['jobName','start_time'], how = 'left')

print('----------- Creating df_lag -------------')

df_lag = (joinedDF.withColumn('prev_channel',
                              F.lag(joinedDF['channel_label']).over(Window.orderBy('id').partitionBy('jobName'))
                             )
          .withColumn('sentence_start',
                     F.coalesce(F.lag(joinedDF['type']).over(Window.orderBy('id').partitionBy('jobName')),F.lit('punctuation'))
                     )
          .withColumn('next_punctuation',
                     F.coalesce(F.lag(joinedDF['content']).over(Window.orderBy('id').partitionBy('jobName')),F.lit('.')))
         )


print('----------- Reassigning DF -------------')
df = (
    df_lag.select('id',
                  F.when(df_lag['channel_label'].isNull(), df_lag['prev_channel'])
                  .otherwise(df_lag['channel_label']).alias('channel_label'),
                  F.when((df_lag['sentence_start'] == 'punctuation') & (df_lag['next_punctuation'].isin({'.','?','!'})),1).otherwise(0).alias('sentence_start'),
                  'start_time',
                  'content',
                  'type',
                  'jobName'
                 )
)

print('----------- Creating Sentence Starters -------------')
sentenceStartersDF = (df.select('id',F.col('start_time').alias('sentence_start_time'),'jobName','channel_label','content')
                      .where('sentence_start == 1')
                     )

print('----------- Creating WordsDF  -------------')
wordsDF = df.join(sentenceStartersDF,on=['id','jobName','channel_label','content'],how='left')

print('----------- Assigning words to sentences -------------')
sentenceAssignedDF = (wordsDF.select('id','jobName','start_time','channel_label','content','sentence_start','type',
                         F.last('sentence_start_time',True)
                         .over(Window.orderBy('id').partitionBy('jobName')).alias('sentence_start_time'))
           )

print('----------- Creating FinalDF -------------')
finalDF = (sentenceAssignedDF.withColumn("new",F.concat_ws(" ","content"))
       .select("id","channel_label","jobName","sentence_start_time","new")
       .groupBy("sentence_start_time","jobName","channel_label")
       .agg(F.concat_ws(" ",F.collect_list("new"))
            .alias('sentence'))
      )

print('----------- WRITING PARQUET FILE -------------')
#finalDF.write.parquet("s3a://sparkoutputs/parquet",mode='append')
finalDF.write.parquet(outputFileLocation,mode='append')

sc.stop()
