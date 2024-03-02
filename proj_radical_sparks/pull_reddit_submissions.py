from datetime import datetime
import pandas as pd
import praw
import json
import pyarrow as pa
import pyarrow.parquet as pq

from dotenv import load_dotenv
import os


class reddit_submission():
    def __init__(self):

        # Load environment variables from .env file
        load_dotenv('.env')

        # Access the environment variables
        self.db_host = os.getenv("DB_HOST")
        self.db_user = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")
        self.db_database = os.getenv("DB_DATABASE")
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("SECRET_KEY")
        self.user_agent = os.getenv("REDDIT_GRANT_TYPE")
        self.username = os.getenv("REDDIT_USERNAME")
        self.password = os.getenv("REDDIT_PASSWORD")

        # self.reddit = praw.Reddit(
        #     self.client_id,
        #     self.client_secret,
        #     self.user_agent,
        #     self.username,
        #     self.password
        #     )
        self.reddit = praw.Reddit(
            client_id=os.getenv("CLIENT_ID"),
            client_secret=os.getenv("SECRET_KEY"),
            user_agent=os.getenv("REDDIT_GRANT_TYPE"),
            username=os.getenv("REDDIT_USERNAME"),
            password=os.getenv("REDDIT_PASSWORD")
        )
    

    def get_submission_ids(self, file):

        df = pd.read_csv(file)
        submission_ids = df['submission_id'].tolist()
        submission_titles = df['submission_title'].tolist()
        
        return submission_ids, submission_titles
    

    def process_reddit_data(self, file):

        index_loop = 0

        submission_ids_list, submission_titles = self.get_submission_ids(file)

        for submission_id, submission_title in zip(submission_ids_list, submission_titles):
            dfComment = pd.DataFrame(columns=['id', 'timestamp', 'author', 'body', 'title'])
            submission = self.reddit.submission(submission_id)
            submission.comments.replace_more(limit=None)

            for comment in submission.comments.list():
                dfComment.loc[index_loop, 'id'] = comment.id
                dfComment.loc[index_loop, "timestamp"] = datetime.utcfromtimestamp(comment.created_utc)
                dfComment.loc[index_loop, 'author'] = str(comment.author)
                dfComment.loc[index_loop, 'body'] = comment.body
                dfComment.loc[index_loop, 'title'] = submission_title
                index_loop += 1

            dfComment['dt'] = pd.to_datetime(dfComment['timestamp']).dt.strftime('%Y-%m-%d')
            dfComment['author'] = dfComment['author'].apply(lambda x: str(x))

            data_folder = f'./proj_radical_sparks/reddit_data_folder/{submission_id}/'

            if not os.path.exists(data_folder):
                os.makedirs(data_folder)

            ls_dt = sorted(list(dfComment['dt'].unique()))

            for dt in ls_dt:
                file_name = f'{submission_id}_{dt}.parquet'
                dfComment_daily = dfComment[dfComment['dt'] == dt]

                arrow_table = pa.Table.from_pandas(dfComment_daily)
                file_path = os.path.join(data_folder, file_name)
                pq.write_table(arrow_table, file_path)
            
            del dfComment

        return None


if __name__ == "__main__":
    get_reddit_data = reddit_submission()
    get_reddit_data.process_reddit_data('./proj_radical_sparks/new_100_submission.csv')
