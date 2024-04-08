import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import streamlit as st
import random
import string
from datetime import datetime


class load_to_mysql():
    def __init__(self, env_file_path):
        """
        Constructor for the class.
        """
        self.env_file_path = env_file_path
        pass

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

        # Get streamlit input from user
        st.set_page_config(layout="wide")
        st.title("Simulated post on reddit r/Singapore")
        body = st.text_input("Write your post here:")

        # Only insert data to MySQL table if there is data entered in 'body' column
        if body:

            # Generate random data for the rest of the columns
            submission = str('r/Singapore')
            submission_id = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(10))
            comment_id = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(10))
            timestamp = str(datetime.now())
            author = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            upvotes = str(random.randint(1, 1000))
            upvote_ratio = str(random.uniform(0.1, 1))
            date = str(datetime.now().date())

            mycursor = mydb.cursor()

            mycursor.execute("CREATE TABLE IF NOT EXISTS posts_for_inference (submission_id VARCHAR(255), comment_id VARCHAR(255), timestamp TIMESTAMP, author VARCHAR(255), body TEXT, submission TEXT, upvotes VARCHAR(255), upvote_ratio VARCHAR(255), date DATE);")

            sqlFormula = "INSERT INTO posts_for_inference (submission_id, comment_id, timestamp, author, body, submission, upvotes, upvote_ratio, date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE submission_id=VALUES(submission_id), comment_id=VALUES(comment_id), timestamp=VALUES(timestamp), author=VALUES(author), body=VALUES(body), submission=VALUES(submission), upvotes=VALUES(upvotes), upvote_ratio=VALUES(upvote_ratio), date=VALUES(date);"

            # Insert DataFrame data into the MySQL table
            mycursor.execute(sqlFormula, (submission_id, comment_id, timestamp, author, body, submission, upvotes, upvote_ratio, date))

            mydb.commit()


if __name__ == "__main__":
    csvmysql = load_to_mysql('.env')
    csvmysql.save_to_mysql()
