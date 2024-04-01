# from retrieve_labelled_data_with_pyspark import labelled_data_retrieval
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re
import os
import sys

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
