from datetime import datetime
import pandas as pd
import praw
# import json
import pyarrow as pa
import pyarrow.parquet as pq

from dotenv import load_dotenv
import os


class reddit_submission():
    def __init__(self, env_file_path):
        """
        Initialize the class with environment variables loaded from the .env file.
        Set instance variables for database connection details, client ID, client secret, user agent, Reddit username, and password.
        Initialize a Reddit instance using the provided credentials.
        """

        # Load environment variables from .env file
        load_dotenv(env_file_path)
        print(os.getenv('CLIENT_ID'))

        self.reddit = praw.Reddit(
            client_id=os.getenv('CLIENT_ID'),
            client_secret=os.getenv('SECRET_KEY'),
            user_agent=os.getenv('REDDIT_GRANT_TYPE'),
            username=os.getenv('REDDIT_USERNAME'),
            password=os.getenv('REDDIT_PASSWORD')
        )
    
    def retrieve_list_of_submission_id(self, subreddit_name_list, limit, file_path):
        """
        Retrieves a list of submission IDs from the specified subreddits and saves them to a CSV file.

        :param subreddit_name_list: List of subreddit names to retrieve submissions from
        :param limit: Maximum number of submissions to retrieve per subreddit
        :param file_path: Path to the CSV file to save the submission IDs
        :return: List of submission IDs
        """
        submissions = []

        for subreddit_name in subreddit_name_list:
            for submission in self.reddit.subreddit(subreddit_name).new(limit=limit):
                submissions.append(submission.id)
                pd.DataFrame(submissions).to_csv(file_path)
            
        return submissions
    
    
    def get_submission_ids(self, file):
        """
        Retrieves the submission ids and titles from the given CSV file.

        Parameters:
            file (str): The path to the CSV file.

        Returns:
            tuple: A tuple containing two lists - submission_ids and submission_titles.
        """

        df = pd.read_csv(file)
        submission_ids = df['submission_id'].tolist()
        submission_titles = df['submission_title'].tolist()
        
        return submission_ids, submission_titles
    

    def process_reddit_data(self, file):
        """
        Process Reddit data from a given file and store it in a structured format for analysis.
        
        Args:
            self: The class instance.
            file: The file containing the Reddit data to be processed.
        
        Returns:
            None
        """

        index_loop = 0

        submission_ids_list, submission_titles = self.get_submission_ids(file)

        for submission_id, submission_title in zip(submission_ids_list, submission_titles):
            dfComment = pd.DataFrame(columns=['submission_id', 'comment_id', 'timestamp', 'author', 'body', 'submission', 'sub_reddit', 'upvotes', 'upvote_ratio'])
            submission = self.reddit.submission(submission_id)
            submission.comments.replace_more(limit=None)

            for comment in submission.comments.list():
                dfComment.loc[index_loop, 'submission_id'] = submission_id
                dfComment.loc[index_loop, 'comment_id'] = comment.id
                dfComment.loc[index_loop, "timestamp"] = datetime.utcfromtimestamp(comment.created_utc)
                dfComment.loc[index_loop, 'author'] = str(comment.author)
                dfComment.loc[index_loop, 'body'] = comment.body
                dfComment.loc[index_loop, 'submission'] = submission_title
                dfComment.loc[index_loop, 'sub_reddit'] = submission.subreddit.display_name
                dfComment.loc[index_loop, 'upvotes'] = submission.score
                dfComment.loc[index_loop, 'upvote_ratio'] = submission.upvote_ratio

                index_loop += 1

            dfComment['dt'] = pd.to_datetime(dfComment['timestamp']).dt.strftime('%Y-%m-%d')
            dfComment['author'] = dfComment['author'].apply(lambda x: str(x))

            data_folder = f'./reddit_33_id_data_folder/{submission_id}/'

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
    get_reddit_data = reddit_submission('.env')
    # get_reddit_data.retrieve_list_of_submission_id(['Singapore'], 100, './proj_radical_sparks/new_25_submission.csv')
    get_reddit_data.process_reddit_data('new_33_submission.csv')
