import json
import boto3
import time
import pandas
import os
from io import StringIO


def lambda_handler(event, context):
    # TODO implement
    def df_Athena(sql_script,wait_time):
        output_athena = 's3://athena-output-qs/'
        df = pandas.DataFrame()
        athena = boto3.client(service_name = 'athena', aws_access_key_id = event['aws_id'],aws_secret_access_key= event['aws_key'],  region_name='ap-southeast-1')
        s3 = boto3.resource(service_name = 's3', aws_access_key_id = event['aws_id'],aws_secret_access_key= event['aws_key'],  region_name='ap-southeast-1')
        response = athena.start_query_execution(QueryString=sql_script,
                                                ResultConfiguration={'OutputLocation': output_athena},WorkGroup= "primary")
        file_name = response["QueryExecutionId"]+".csv"
        try_time = 1
        try_ok = False
        print(file_name)
        fullname = (output_athena + file_name).replace('s3://','')
        bucket_name = fullname.split('/')[0]
        key_name = fullname.replace(bucket_name + '/','')
    
        while try_time <= wait_time and try_ok == False:
            try:
                s3_object = s3.Object(bucket_name, key_name)
                df = pandas.read_csv(s3_object.get()["Body"])
                try_ok = True
                print("Jump out",str(try_time))
            except:
                time.sleep(1)
                print("Error comes out: ",str(try_time))
                try_time = try_time + 1
        return df
        

    SQL_qs='''
    
        select 
            sub_id as submission_id,	
            id as comment_id,	
            created_utc as timestamp,	
            author,	
            body,	
            sub_title as submission, 
            sr as sub_reddit,
            sub_score as upvotes,		
            sub_upvote_ratio as upvote_ratio,	
            dt as date,
            lower(regexp_replace(body, 'http\\S+|\\n', '')) AS body_cleaned,
            lower(regexp_replace(sub_title, 'http\\S+|\\n', '')) AS submission_cleaned  
        from reddit_sr_sm_dt.raw where dt between date('2024-03-15') and date('2024-04-16') 
    '''
    df_output = df_Athena(SQL_qs,800)
    
    print(df_output)
    csv_buffer = StringIO()
    df_output.to_csv(csv_buffer, index=False)
    s3 = boto3.resource(service_name = 's3', aws_access_key_id = event['aws_id'],aws_secret_access_key= event['aws_key'],  region_name='ap-southeast-1')
    s3.Bucket('aws-emr-studio-533267180383-ap-southeast-1').put_object(Key= 'input/df_raw_clean.csv', Body=csv_buffer.getvalue())
    
    return None