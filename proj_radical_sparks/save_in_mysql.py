import glob
import pyarrow.parquet as pq
import os
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from dotenv import load_dotenv


class load_to_mysql():
    def __init__(self, env_file_path):
        """
        Constructor for the class.
        """
        self.env_file_path = env_file_path
        pass

    def parquet_to_df(self, parquet_folder):
        """
        Read Parquet files from the specified folder and its subfolders, and concatenate the data into a single DataFrame.
        
        Parameters:
            parquet_folder (str): The path to the folder containing Parquet files.
        
        Returns:
            pandas.DataFrame: A DataFrame containing the combined data from all the Parquet files.
        """

        # Get a list of all Parquet files in the folder and its subfolders
        parquet_files = glob.glob(os.path.join(parquet_folder, '**/*.parquet'), recursive=True)

        # Read each Parquet file and append the data to a list
        data = []
        for file in parquet_files:
            table = pq.read_table(file)
            df = table.to_pandas()
            data.append(df)

        # Concatenate all the data into a single DataFrame
        self.df_combined = pd.concat(data, ignore_index=True)

        return self.df_combined

    def save_to_mysql(self):
        """
        Save data to a MySQL database using environment variables from a .env file. 
        """

        # Load environment variables from .env file
        load_dotenv(self.env_file_path)

        # Access the environment variables
        db_host = os.getenv("DB_HOST")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_database = os.getenv("DB_DATABASE")

        mydb = mysql.connector.connect(
            host=db_host,
            user=db_user,
            passwd=db_password,
            database=db_database
            )

        mycursor = mydb.cursor()

        engine = create_engine('mysql+pymysql://user:passwd@host/database')
        # Create the table
        mycursor.execute(
            "CREATE TABLE IF NOT EXISTS proj_radical_sparks (submission_id VARCHAR(255), comment_id VARCHAR(255), timestamp TIMESTAMP, author VARCHAR(255), body TEXT, submission TEXT, upvotes VARCHAR(255), upvote_ratio VARCHAR(255), date DATE)"
        )

        sqlFormula = "INSERT INTO proj_radical_sparks (submission_id, comment_id, timestamp, author, body, submission, upvotes, upvote_ratio, date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE submission_id=VALUES(submission_id), comment_id=VALUES(comment_id), timestamp=VALUES(timestamp), author=VALUES(author), body=VALUES(body), submission=VALUES(submission), upvotes=VALUES(upvotes), upvote_ratio=VALUES(upvote_ratio), date=VALUES(date);"

        # Insert DataFrame data into the MySQL table
        mycursor.executemany(sqlFormula, self.df_combined.values.tolist())

        mydb.commit()


if __name__ == "__main__":
    tomysql = load_to_mysql('.env')
    tomysql.parquet_to_df('./reddit_35_id_data_folder/')
    tomysql.save_to_mysql()