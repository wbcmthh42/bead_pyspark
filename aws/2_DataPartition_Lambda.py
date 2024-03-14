
from datetime import datetime
import pandas as pd
import praw
import boto3
import json
import os
import pyarrow as pa
import pyarrow.parquet as pq


def lambda_handler(event, context):
    # TODO implement
      
    reddit = praw.Reddit(
    client_id= event['client_id'],
    client_secret= event['client_secret'],
    user_agent=event['user_agent'],
    #check_for_async=False,
    username=event['username'],
    password=event['password'],
    )
    
    dfComment= pd.DataFrame(columns =['id','timestamp','author','body'])
    index_loop=0
    
    submission_id="isfwqm"
    submission = reddit.submission(submission_id)
    submission.comments.replace_more(limit=None)
    
    for comment in submission.comments.list():
        dfComment.loc[index_loop,'id'] =comment.id
        dfComment.loc[index_loop,"timestamp"] = datetime.utcfromtimestamp(comment.created_utc)
        dfComment.loc[index_loop,'author'] = comment.author
        dfComment.loc[index_loop,'body'] = comment.body
        index_loop+=1
    
    dfComment['dt'] = pd.to_datetime(dfComment['timestamp']).dt.strftime('%Y-%m-%d')
    dfComment['author'] = dfComment['author'].apply(lambda x: str(x))
    
    print(dfComment)
    tmp_dir='/tmp/'
    bucket_name='bead-reddit-sg'
    ls_dt=sorted(list(dfComment['dt'].unique()))
    s3 = boto3.resource('s3', aws_access_key_id = event['aws_id'],aws_secret_access_key= event['aws_key'] )
    
    for dt in ls_dt:
        
        file_name='%s_%s.%s' %(submission_id,dt,'parquet')
        file_name_s3="%s/dt=%s/%s" %(submission_id,dt,file_name)
        dfComment_daily = dfComment[dfComment['dt'] == dt]
    
        arrow_table=pa.Table.from_pandas(dfComment_daily)
        parquet_bytes=pa.BufferOutputStream()
        pq.write_table(arrow_table,parquet_bytes)
        
        print(tmp_dir+file_name,bucket_name,file_name_s3)
        s3.Bucket(bucket_name).put_object(Key= file_name_s3, Body=parquet_bytes.getvalue().to_pybytes())
        
        try:
            os.remove(tmp+file_name)
        except :
            pass
            
    return None
        
