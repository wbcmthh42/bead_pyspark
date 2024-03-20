from data_processing import DataProcessing
from dotenv import load_dotenv
import openai
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import os


class LLMClassification():
    def __init__(self, env_file_path):
        """
        Constructor for the class.
        """
        self.env_file_path = env_file_path
        pass

    def get_processed_df(self):
        """
        Method to retrieve a processed DataFrame. 
        This method initializes a DataProcessing object and retrieves a clean table. 
        """
        dr = DataProcessing()
        df = dr.get_clean_table(self.env_file_path)
        return df.limit(50) #limit to 50 rows to test so that it runs faster and its cheaper to run and we dont have to run the entire table through openai api

    def llm_classification(self, text):
        """
        This function performs LLM classification on the given text using OpenAI's GPT model. 
        It takes 'text' as input and returns the classification output. 
        """
        # Load environment variables from .env file
        file_path = self.env_file_path
        load_dotenv(file_path)
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
            {"role": "system", "content": "You help to assess if a text contains radical or extremist statements. Answer in 'yes' or 'no'"},
            {"role": "user", "content": f"{text}"},
        ]

        outputs = pretrained_llm(message)
        # print(outputs)

        return outputs

    def radical_classification(self, df, chk_radical_udf):
        """
        This function applies the given UDF to the 'body_cleaned' and 'title_cleaned' columns of the input DataFrame and returns the modified DataFrame.
        """
        df = df.withColumn('chk_radical_post_body', chk_radical_udf(df['body_cleaned']))
        df = df.withColumn('chk_radical_post_submission', chk_radical_udf(df['submission_cleaned']))
        return df

    def get_radical_class_df(self, df):
        """
        This function takes a DataFrame as input and applies radical classification to it using the provided UDF. It then saves the resulting DataFrame to MySQL and returns it.
        """
        # Load environment variables from .env file
        load_dotenv(self.env_file_path)

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
            dbtable='radical_classification',
            user=db_user,
            password=db_password
        ).mode('overwrite').save()

        return df



if __name__ == '__main__':
    llm = LLMClassification('.env')
    df = llm.get_processed_df()
    llm.get_radical_class_df(df)

