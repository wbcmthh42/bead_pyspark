
# 0 1 2,7,12,17 4 * /usr/bin/python3 /home/ubuntu/python_script/Data_ingestion(EC2).py >> /home/ubuntu/data_ingest_output1.log 2>&1   Apr
# 0 1 18,23,28 3 * /usr/bin/python3 /home/ubuntu/python_script/Data_ingestion(EC2).py >> /home/ubuntu/data_ingest_output1.log 2>&1   Mar


import boto3
import pandas as pd
import praw
from praw.models import MoreComments
from dotenv import load_dotenv
import os
import configparser
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, timedelta, datetime
from prawcore.exceptions import RequestException
import time

time1 = datetime.now()
print("Start time:",time1)

env_path ='/home/ubuntu/python_script/.env'
load_dotenv(dotenv_path=env_path)
env_openai_key = os.getenv('openai_key')
env_client_id= os.getenv("client_id")
env_client_secret= os.getenv("client_secret")
env_user_agent=os.getenv("user_agent")
env_username=os.getenv("user_name")
env_password=os.getenv("password")

s3 = boto3.resource('s3', aws_access_key_id = os.getenv('aws_id'),aws_secret_access_key= os.getenv('aws_key') )

reddit = praw.Reddit(
    client_id= env_client_id,
    client_secret= env_client_secret,
    user_agent=env_user_agent,
    #check_for_async=False,
    username=env_username,
    password=env_password
)


# Empty dataframe
dfComment= pd.DataFrame(columns =[
                'author',
                'body',
                'body_html',
                'created_utc',
                'distinguished',
                'edited',
                'id',
                'is_submitter',
                'link_id',
                'parent_id',
                'permalink',
                'replies',
                'saved',
                'score',
                'stickied',
                'submission',
                'subreddit',
                'subreddit_id'
                ])
dfComment_daily=dfComment


# Function to ingest data
def process_submission(submission, local_dir, sub_reddit,start_date,end_date,dfComment,dfComment_daily):

    bucket_name='bead-reddit-all'

    try:
        submission = reddit.submission(submission.id)
        submission.comments.replace_more(limit=None)
        cm_loop = 0
        for comment in submission.comments.list():
            dfComment.loc[cm_loop,'author']=str(comment.author)
            dfComment.loc[cm_loop,'body']=str(comment.body)
            dfComment.loc[cm_loop,'created_utc']=datetime.utcfromtimestamp(comment.created_utc)
            dfComment.loc[cm_loop,'distinguished']=str(comment.distinguished)
            dfComment.loc[cm_loop,'id']=str(comment.id)
            dfComment.loc[cm_loop,'is_submitter']=comment.is_submitter
            dfComment.loc[cm_loop,'link_id']=str(comment.link_id)
            dfComment.loc[cm_loop,'score']=comment.score
            dfComment.loc[cm_loop,'stickied']=comment.stickied
            dfComment.loc[cm_loop,'submission']=str(comment.submission)
            dfComment.loc[cm_loop,'subreddit']=str(comment.subreddit)
            dfComment.loc[cm_loop,'subreddit_id']=str(comment.subreddit_id)
            cm_loop+=1

        # Info from submission
        dfComment['sub_author'] =str(submission.author)
        dfComment['sub_created_utc'] =datetime.utcfromtimestamp(submission.created_utc)
        dfComment['sub_id'] =str(submission.id)
        dfComment['sub_link_flair_text'] =str(submission.link_flair_text)
        dfComment['sub_num_comments'] =submission.num_comments
        dfComment['sub_score'] =submission.score
        dfComment['sub_title'] =str(submission.title)
        dfComment['sub_upvote_ratio'] =submission.upvote_ratio
        dfComment['sub_url'] =str(submission.url)

        # For partition
        dfComment['sr'] = dfComment['subreddit_id']
        dfComment['sm'] = dfComment['submission']
        dfComment['dt'] = pd.to_datetime(dfComment['created_utc']).dt.strftime('%Y-%m-%d') 

        # Filter out new data in last 3 days.
        dfComment = dfComment[(pd.to_datetime(dfComment['dt']).dt.date >= start_date) & (pd.to_datetime(dfComment['dt']).dt.date <= end_date)]

        if len(dfComment) > 0:
            ls_dt=sorted(list(dfComment['dt'].unique()))
            for dt in ls_dt:    
                file_name = '%s_%s.%s' % (submission.id, dt, 'parquet') 

                #Path in Ubuntu
                folder_path_ub = '%s/sr=%s/sm=%s/dt=%s' % (local_dir, sub_reddit, submission.id, dt)

                #Path in S3
                folder_path_s3 = '%s/sr=%s/sm=%s/dt=%s/%s' % ('raw_sr_sm_dt',sub_reddit, submission.id, dt,file_name)

                if not os.path.exists(folder_path_ub):
                    os.makedirs(folder_path_ub)
                dfComment_daily = dfComment[dfComment['dt'] == dt]
                dfComment_daily = dfComment_daily[dfComment_daily['sm'] == submission.id]

                # Save in server
                file_path= os.path.join(folder_path_ub, file_name)
                dfComment_daily.to_parquet(file_path, engine='pyarrow')
                print(file_path)

                # Upload into S3
                arrow_table=pa.Table.from_pandas(dfComment_daily)
                parquet_bytes=pa.BufferOutputStream()
                pq.write_table(arrow_table,parquet_bytes)
                s3.Bucket(bucket_name).put_object(Key= folder_path_s3, Body=parquet_bytes.getvalue().to_pybytes())
   
        dfComment = dfComment.iloc[0:0]
        dfComment_daily = dfComment_daily.iloc[0:0]
        ls_dt=[]
        file_name=''
        folder_path_s3=''
        folder_path_ub=''
        file_path=''
        return True
    
    except RequestException as e:
        print(f"Waiting for 300 seconds, due to submission error. {e}")
        time.sleep(300)
        return False

    except Exception as e:
        print(f"Unknown error in submission, {e}")
        return False
    
    
# Cut-off time
end_date = date.today()- timedelta(days=1)
start_date = date.today() - timedelta(days=5)
daily_folder=(date.today()- timedelta(days=0)).strftime('%Y-%m-%d')

root_local_dir ='/home/ubuntu/bead-reddit-all'
local_dir = os.path.join(root_local_dir, daily_folder)
listSubReddit=['Singapore','SGExams','askSingapore','NationalServiceSG','SingaporeRaw','SingaporeEats','singaporehappenings']

# Loop all subreddits
for sub_reddit in listSubReddit:
    print(sub_reddit)
    sm_loop=0
    retry_sr = True
    while retry_sr:
        try:
            for submission in reddit.subreddit(sub_reddit).new(limit=500):
                processed = False
                while not processed:
                    processed = process_submission(submission, local_dir, sub_reddit,start_date,end_date,dfComment,dfComment_daily)
            retry_sr = False

        except RequestException as e:
            print(f"Waiting for 300 seconds, due to subreddit error. {e}")
            time.sleep(300)

        except Exception as e:
            print(f"Unknown error in subreddit, {e}")


#Alter partition on existing data tatable
athena = boto3.client(service_name = 'athena', aws_access_key_id = os.getenv('aws_id'),aws_secret_access_key= os.getenv('aws_key'),  region_name='ap-southeast-1')
output_athena = 's3://athena-output-qs/'

sql = 'MSCK REPAIR TABLE reddit_sr_sm_dt.raw'
response =athena.start_query_execution(QueryString = sql, ResultConfiguration={'OutputLocation': output_athena},WorkGroup= "primary")
    
print(response)
time2=datetime.now()
time_diff = time2 - time1
print("End time:",time2," It takes ",time_diff)