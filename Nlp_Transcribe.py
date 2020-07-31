#!/usr/bin/env python
# coding: utf-8

# In[1]:


import time
import boto3
import json
import urllib.request
import os


def transcribe_handler (event, context):

	region = os.environ['AWS_REGION']
	transcribe = boto3.client('transcribe',
							  region_name= region
							 )

	inputBucket = os.environ['InputS3Bucket']
	outputBucket = os.environ['OutputS3Bucket']

	# Get first event on Records list - TODO: Check for validity of event - type of event if necessary in the future
	info = event.get('Records', [0])[0].get('s3', {})
	file_key = info.get('object', {}).get('key')
	job_uri = 'https://'+inputBucket+'.s3.'+region+'.amazonaws.com/' + file_key

	print(job_uri)
	job_name = "transcribe_"+file_key+ str(round(time.time()))
	transcribe.start_transcription_job(
										TranscriptionJobName=job_name,
										Media={'MediaFileUri':job_uri},
										MediaFormat='wav',
										LanguageCode='en-US',
										OutputBucketName = outputBucket,
										Settings = {
															  'ChannelIdentification': True,
															  'VocabularyName':'GJay'
														  }
										)
