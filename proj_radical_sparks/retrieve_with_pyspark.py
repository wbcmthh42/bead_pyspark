import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

class data_retrieval():
    def __init__(self):
        """
        Initialize the object.
        """
        pass

    def get_data(self, file_path):
        """
        Method to get data from a MySQL database using SparkSession.

        Args:
            file_path (str): The path to the .env file containing environment variables.

        Returns:
            DataFrame: The data retrieved from the MySQL database.
        """

        # Load environment variables from .env file
        load_dotenv(file_path)

        # Access the environment variables
        db_host = os.getenv("DB_HOST")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_database = os.getenv("DB_DATABASE")

        spark = SparkSession.builder.appName("reddit"). \
            config("spark.jars", "/Users/mysql-connector-j-8.3.0/mysql-connector-j-8.3.0.jar"). \
            getOrCreate()

        df_mysql = spark.read.format("jdbc"). \
            option("url", "jdbc:mysql://localhost:3306/testdb"). \
            option("driver", "com.mysql.jdbc.Driver"). \
            option("user", db_user). \
            option("password", db_password). \
            option("query", "select * from proj_radical_sparks"). \
            load()

        return df_mysql

if __name__ == "__main__":
    dp = data_retrieval()
    dp.get_data('/Users/johnnytay/Library/CloudStorage/OneDrive-Personal/My NUS Mtech EBAC course/Semester 3/Practice Module/bead_pyspark/.env')