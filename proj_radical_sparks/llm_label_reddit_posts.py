from retrieve_with_pyspark import data_retrieval
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from dotenv import load_dotenv
import re
import os
import sys

import openai
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

class DataLabelling():
    def get_data(self, env_path):
        """
        Retrieve data using the specified environment path.

        Args:
            env_path (str): The path to the environment.

        Returns:
            DataFrame: The retrieved data.
        """
        self.file_path = env_path
        dp = data_retrieval()
        df = dp.get_data(self.file_path)
        return df

    def llm_classification(self, text):
        """
        This function performs LLM classification on the given text using OpenAI's GPT model. 
        It takes 'text' as input and returns the classification output. 
        """
        # Load environment variables from .env file
        load_dotenv(self.file_path)
        openai.api_key = os.getenv('OPENAI_API_KEY')

        def pretrained_llm(messages, model="gpt-3.5-turbo",
                           #  model="gpt-4",
                           temperature=0,
                           max_tokens=500):
            """
            A function to generate completions for a given set of messages using a pre-trained language model.

            Parameters:
                messages (list): A list of messages to provide context for generating completions.
                model (str): The name of the pre-trained language model to use for generating completions. Default is "gpt-3.5-turbo".
                temperature (int): The temperature for sampling the next token. Default is 0.
                max_tokens (int): The maximum number of tokens to generate in the completion. Default is 500.

            Returns:
                str: The generated completion message.
            """
            response = openai.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            return response.choices[0].message.content

        message = [
            {"role": "system", "content": "You help to assess if a text contains radical or extremist statements. Only answer in lowercase 'yes' or 'no'. If you are not sure, just answer 'no'."},
            {"role": "user", "content": f"{text}"},
        ]

        outputs = pretrained_llm(message)
        # print(outputs)

        return outputs

    def radical_classification(self, df, chk_radical_udf):
        """
        This function applies the given UDF to the 'body' and 'submission' columns of the input DataFrame and returns the modified DataFrame.
        """
        df = df.withColumn('label', chk_radical_udf(df['body']))
        # df = df.withColumn('chk_radical_post_submission', chk_radical_udf(df['submission_cleaned']))
        return df

    def get_radical_class_df(self, df):
        """
        This function takes a DataFrame as input and applies radical classification to it using the provided UDF. It then saves the resulting DataFrame to MySQL and returns it.
        """
        # Load environment variables from .env file
        # load_dotenv(self.env_path)

        # Access the environment variables
        db_host = os.getenv("DB_HOST")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_database = os.getenv("DB_DATABASE")
        
        chk_radical_udf = udf(self.llm_classification, StringType())
        df = self.radical_classification(df, chk_radical_udf)
        df.write.format('jdbc').options(
            url='jdbc:mysql://localhost:3306/testdb',
            driver='com.mysql.cj.jdbc.Driver',
            dbtable='reddit_post_with_labels',
            user=db_user,
            password=db_password
        ).mode('overwrite').save()

        return df

if __name__ == '__main__':
    dl = DataLabelling()
    df = dl.get_data('.env')
    dl.get_radical_class_df(df)