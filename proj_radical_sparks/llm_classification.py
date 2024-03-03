from data_processing import DataProcessing
from dotenv import load_dotenv
import openai
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import os


class LLMClassification():
    def __init__(self):
        """
        Constructor for the class.
        """
        pass

    def get_processed_df(self):
        """
        Method to retrieve a processed DataFrame. 
        This method initializes a DataProcessing object and retrieves a clean table. 
        """
        dr = DataProcessing()
        df = dr.get_clean_table()
        return df.limit(20) #limit to 20 rows to test so that it runs faster and its cheaper to run and we dont have to run the entire table through openai api

    def llm_classification(self, text):
        """
        This function performs LLM classification on the given text using OpenAI's GPT model. 
        It takes 'text' as input and returns the classification output. 
        """
        # Load environment variables from .env file
        file_path = '/Users/johnnytay/Library/CloudStorage/OneDrive-Personal/My NUS Mtech EBAC course/Semester 3/Practice Module/bead_pyspark/.env'
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
        df = df.withColumn('chk_radical_post_(body_cleaned)', chk_radical_udf(df['body_cleaned']))
        df = df.withColumn('chk_radical_post_(title_cleaned)', chk_radical_udf(df['title_cleaned']))
        return df

    def get_radical_class_df(self, df):
        """
        This function takes a DataFrame as input and applies radical classification to it using the provided UDF. It then displays the resulting DataFrame and returns it.
        """
        chk_radical_udf = udf(self.llm_classification, StringType())
        df = self.radical_classification(df, chk_radical_udf)
        df.show()
        return df


if __name__ == '__main__':
    llm = LLMClassification()
    df = llm.get_processed_df()
    llm.get_radical_class_df(df)

