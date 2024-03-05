from retrieve_with_pyspark import data_retrieval
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

class DataProcessing():
    def get_data(self, env_path):
        """
        Retrieve data using the specified environment path.

        Args:
            env_path (str): The path to the environment.

        Returns:
            DataFrame: The retrieved data.
        """
        dp = data_retrieval()
        df = dp.get_data(env_path)
        return df

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
        df = df.withColumn('title_cleaned', clean_text_udf(df['title']))
        return df

    def get_clean_table(self):
        """
        This function retrieves a clean table by getting data from a specified location, applying a user-defined function to clean the text, and returning the resulting DataFrame.
        """
        df = self.get_data('/Users/johnnytay/Library/CloudStorage/OneDrive-Personal/My NUS Mtech EBAC course/Semester 3/Practice Module/bead_pyspark/.env')
        clean_text_udf = udf(self.clean_text, StringType())
        df = self.clean_body_title(df, clean_text_udf)
        # df.show()
        return df

if __name__ == '__main__':
    dp = DataProcessing()
    dp.get_clean_table()