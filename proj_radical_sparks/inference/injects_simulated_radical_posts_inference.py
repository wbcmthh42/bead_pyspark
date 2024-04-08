import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv


class load_to_mysql():
    def __init__(self, env_file_path):
        """
        Constructor for the class.
        """
        self.env_file_path = env_file_path
        pass

    def csv_to_df(self, csv_file):
        """
        Read Parquet files from the specified folder and its subfolders, and concatenate the data into a single DataFrame.

        Parameters:
            parquet_folder (str): The path to the folder containing Parquet files.

        Returns:
            pandas.DataFrame: A DataFrame containing the combined data from all the Parquet files.
        """
            
        self.simulated_df = pd.read_csv(csv_file, encoding='utf-8')
        self.simulated_df['timestamp'] = pd.to_datetime(self.simulated_df['timestamp'], format='%d/%m/%y %H:%M')
        self.simulated_df['date'] = pd.to_datetime(self.simulated_df['date'], format='%d/%m/%y')

        return self.simulated_df


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

        mycursor.execute("CREATE TABLE IF NOT EXISTS posts_for_inference (submission_id VARCHAR(255), comment_id VARCHAR(255), timestamp TIMESTAMP, author VARCHAR(255), body TEXT, submission TEXT, upvotes VARCHAR(255), upvote_ratio VARCHAR(255), date DATE);")

        sqlFormula = "INSERT INTO posts_for_inference (submission_id, comment_id, timestamp, author, body, submission, upvotes, upvote_ratio, date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE submission_id=VALUES(submission_id), comment_id=VALUES(comment_id), timestamp=VALUES(timestamp), author=VALUES(author), body=VALUES(body), submission=VALUES(submission), upvotes=VALUES(upvotes), upvote_ratio=VALUES(upvote_ratio), date=VALUES(date);"

        # Insert DataFrame data into the MySQL table
        mycursor.executemany(sqlFormula, self.simulated_df.values.tolist())

        mydb.commit()


if __name__ == "__main__":
    csvmysql = load_to_mysql('.env')
    csvmysql.csv_to_df('simulated_radical_data_v4.csv')
    csvmysql.save_to_mysql()