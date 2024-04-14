# from retrieve_with_pyspark import data_retrieval
# from data_processing import DataProcessing
import os
import re
import sys
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.ml import PipelineModel
from pyspark.ml.tuning import CrossValidatorModel
from dotenv import load_dotenv
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

class DataProcessing():

    def clean_text(self, text):
        """
        Clean the input text by removing URLs and newline characters, then convert to lowercase.
        Parameters:
            text (str): The input text to be cleaned.
        Returns:
            str: The cleaned text.
        """
        return re.sub(r'http\S+|\n', '', text).lower()

    def clean_body_title(self, df, clean_text_udf):
        """
        Clean the body and title columns of the input dataframe using the provided clean_text_udf function and return the modified dataframe.
        """
        df = df.withColumn('body_cleaned', clean_text_udf(df['body']))
        df = df.withColumn('submission_cleaned', clean_text_udf(df['submission']))
        return df

    def get_clean_table(self, df):
        """
        This function retrieves a clean table by getting data from a specified location, applying a user-defined function to clean the text, and returning the resulting DataFrame.
        """
        clean_text_udf = udf(self.clean_text, StringType())
        df = self.clean_body_title(df, clean_text_udf)
        return df

class Data_inferencing():
    def get_data_from_database(self, file_path):
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
            option("query", "select * from posts_for_inference"). \
            load()

        return df_mysql

    def get_data(self, env_path):
            """
            Retrieve data using the specified environment path.

            Args:
                env_path (str): The path to the environment.

            Returns:
                DataFrame: The retrieved data.
            """
            self.file_path = env_path
            self.df = self.get_data_from_database(self.file_path)
            return self.df

    def get_processed_df(self):
            """
            Method to retrieve a processed DataFrame.
            This method initializes a DataProcessing object and retrieves a clean table.
            """
            dr = DataProcessing()
            self.df_mysql_clean = dr.get_clean_table(self.df)
            return self.df_mysql_clean

    def get_vectorizer(self):
            self.pipelineFit = PipelineModel.load("vectorizer_checkpoint")
            return self.pipelineFit

    def get_model(self):
            self.model = CrossValidatorModel.load("model_checkpoint")
            return self.model

    def inference(self, text):
            transformed_text = self.pipelineFit.transform(text)
            predictions = self.model.transform(transformed_text)
            return predictions  # return the entire DataFrame with the 'prediction' column

    def get_prediction(self):
            predictions = self.inference(self.df_mysql_clean)
            # df = self.df_mysql_clean.join(predictions, ['id'])  # join on the 'id' column
            df = predictions.drop('words', 'filtered', 'rawFeatures', 'features', 'rawPrediction', 'probability')
            return df

    def get_radical_class_df(self, df):
            # Access the environment variables
            db_user = os.getenv("DB_USER")
            db_password = os.getenv("DB_PASSWORD")

            df.write.format('jdbc').options(
                url='jdbc:mysql://localhost:3306/testdb',
                driver='com.mysql.cj.jdbc.Driver',
                dbtable='predicted_labels',
                user=db_user,
                password=db_password
            ).mode('overwrite').save()

            return df

if __name__ == '__main__':
    dl = Data_inferencing()
    df = dl.get_data('.env')
    df = dl.get_processed_df()
    dl.get_vectorizer()
    dl.get_model()
    df = dl.get_prediction()
    df.show()
    dl.get_radical_class_df(df)