from retrieve_with_pyspark import data_retrieval
from data_processing import DataProcessing
import os
import sys
from pyspark.ml import PipelineModel
from pyspark.ml.tuning import CrossValidatorModel

class Data_inferencing():
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
        self.df = dp.get_data(self.file_path)
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