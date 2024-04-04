from data_processing import DataProcessing
from dotenv import load_dotenv
import pandas as pd
import openai
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import os
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType


class LLMClassification():
    def __init__(self, env_file_path):

        self.env_file_path = env_file_path

        pass

    def load_test_csv(self):
        """
        Method to load the testing data from a CSV file and convert it to a PySpark DataFrame.
        Args:
            spark (SparkSession): The SparkSession object.
        Returns:
            DataFrame: The testing data as a PySpark DataFrame.
        """
        spark = SparkSession.builder.appName("llm_inference"). \
            config("spark.jars", "/Users/mysql-connector-j-8.3.0/mysql-connector-j-8.3.0.jar"). \
            getOrCreate()

        # Load the CSV file as a pandas DataFrame
        pandas_df = pd.read_csv("testing_data.csv", header=0)

        # Convert the pandas DataFrame to a PySpark DataFrame
        self.testing_data = spark.createDataFrame(pandas_df)

        return self.testing_data


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
            {"role": "system", "content": "You help to assess if a text contains radical or extremist statements. Only answer in lowercase 'yes' or 'no'. If you are not sure, just answer 'no'."},
            {"role": "user", "content": f"{text}"},
        ]

        outputs = pretrained_llm(message)
        # print(outputs)

        return outputs

    def radical_classification(self, df, chk_radical_udf):
        """
        This function applies the given UDF to the 'body_cleaned' and 'title_cleaned' columns of the input DataFrame and returns the modified DataFrame.
        """
        df = df.withColumn('predicted_label', chk_radical_udf(df['body_cleaned']))
        # df = df.withColumn('chk_radical_post_submission', chk_radical_udf(df['submission_cleaned']))
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
        

        def llm_classification_func(body_cleaned):
            # Create an instance of your class
            llm = LLMClassification('.env')  # Replace LLM with the name of your class

            # Call the llm_classification method
            return llm.llm_classification(body_cleaned).lower()
        
        chk_radical_udf = udf(llm_classification_func, StringType())
        df = self.radical_classification(df, chk_radical_udf)
        df = df.withColumn('predicted_label', F.lower(df['predicted_label']))
        df = df.withColumn('predicted_label_int', F.when(F.col('predicted_label') == 'yes', F.lit(1)).otherwise(F.lit(0)))
        df.write.format('jdbc').options(
            url='jdbc:mysql://localhost:3306/testdb',
            driver='com.mysql.cj.jdbc.Driver',
            dbtable='radical_classification',
            user=db_user,
            password=db_password
        ).mode('overwrite').save()

        return df

    
    def evaluate_classification(self, df):
        """
        Function to evaluate the classification results by comparing the predicted_label against the ground_truth_label_int.
        Calculates accuracy, precision, recall, f1 score, and the confusion matrix.

        Args:
            df (DataFrame): The DataFrame containing the predicted_label and ground_truth_label_int columns.

        Returns:
            dict: A dictionary containing the evaluation metrics.
        """
        
        # Cast the 'ground_truth_label_int' and 'predicted_label_int' column are integers
        df = df.withColumn("ground_truth_label_int", col("ground_truth_label_int").cast(DoubleType()))
        df = df.withColumn("predicted_label_int", col("predicted_label_int").cast(DoubleType()))

        # Calculate accuracy
        evaluator = MulticlassClassificationEvaluator(predictionCol="predicted_label_int", labelCol="ground_truth_label_int", metricName="accuracy")
        accuracy = evaluator.evaluate(df)
        

        # Cast tp, fp, tn, and fn to double for calculation
        tp = df.filter((df.predicted_label_int.cast("double") == 1.0) & (df.ground_truth_label_int.cast("double") == 1.0)).count()
        fp = df.filter((df.predicted_label_int.cast("double") == 1.0) & (df.ground_truth_label_int.cast("double") == 0.0)).count()
        tn = df.filter((df.predicted_label_int.cast("double") == 0.0) & (df.ground_truth_label_int.cast("double") == 0.0)).count()
        fn = df.filter((df.predicted_label_int.cast("double") == 0.0) & (df.ground_truth_label_int.cast("double") == 1.0)).count()

        precision = tp / (tp + fp)
        recall = tp / (tp + fn)
        f1 = 2 * (precision * recall) / (precision + recall)

        # Calculate confusion matrix
        predictionAndLabels = df.select("predicted_label_int", "ground_truth_label_int").rdd.map(lambda row: (row.predicted_label_int, row.ground_truth_label_int))
        metrics = MulticlassMetrics(predictionAndLabels)
        confusion_matrix = metrics.confusionMatrix().toArray().tolist()

        evaluation_metrics = {
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "confusion_matrix": confusion_matrix
        }
        print(evaluation_metrics)
        return evaluation_metrics




if __name__ == '__main__':
    llm = LLMClassification('.env')
    df = llm.load_test_csv()
    df = llm.get_radical_class_df(df)
    llm.evaluate_classification(df)

