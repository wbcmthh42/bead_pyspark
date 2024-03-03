from data_processing import DataProcessing
from dotenv import load_dotenv
import openai
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import os


class LLMClassification():
    def __init__(self):
        pass

    def get_processed_df(self):
        dr = DataProcessing()
        df = dr.get_clean_table()
        return df.limit(20)

    def llm_classification(self, text):
        # Load environment variables from .env file
        file_path = '/Users/johnnytay/Library/CloudStorage/OneDrive-Personal/My NUS Mtech EBAC course/Semester 3/Practice Module/bead_pyspark/.env'
        load_dotenv(file_path)
        openai.api_key = os.getenv('OPENAI_API_KEY')

        def pretrained_llm(messages, model="gpt-3.5-turbo",
                           #  model="gpt-4",
                           temperature=0,
                           max_tokens=500):
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
        df = df.withColumn('chk_radical_post_(body_cleaned)', chk_radical_udf(df['body_cleaned']))
        df = df.withColumn('chk_radical_post_(title_cleaned)', chk_radical_udf(df['title_cleaned']))
        return df

    def get_radical_class_df(self, df):
        chk_radical_udf = udf(self.llm_classification, StringType())
        df = self.radical_classification(df, chk_radical_udf)
        df.show()
        return df


if __name__ == '__main__':
    llm = LLMClassification()
    df = llm.get_processed_df()
    llm.get_radical_class_df(df)

