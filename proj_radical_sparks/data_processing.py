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
        dp = data_retrieval()
        df = dp.get_data(env_path)
        return df

    def clean_text(self, text):
        return re.sub(r'http\S+|\n', '', text).lower()

    def clean_body_title(self, df, clean_text_udf):
        df = df.withColumn('body_cleaned', clean_text_udf(df['body']))
        df = df.withColumn('title_cleaned', clean_text_udf(df['title']))
        return df

    def get_clean_table(self):
        df = self.get_data('/Users/johnnytay/Library/CloudStorage/OneDrive-Personal/My NUS Mtech EBAC course/Semester 3/Practice Module/bead_pyspark/.env')
        clean_text_udf = udf(self.clean_text, StringType())
        df = self.clean_body_title(df, clean_text_udf)
        # df.show()
        return df

if __name__ == '__main__':
    dp = DataProcessing()
    dp.get_clean_table()