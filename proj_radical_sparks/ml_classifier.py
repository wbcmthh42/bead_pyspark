import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer,StopWordsRemover
from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.classification import NaiveBayes, RandomForestClassifier, LogisticRegression, DecisionTreeClassifier, GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.types import FloatType
from pyspark.ml import PipelineModel
import pyspark.sql.functions as F
from joblib import dump

from data_processing import DataProcessing

class data_retrieval():
    def __init__(self):
        """
        Initialize the object.
        """
        pass

    def get_data(self, file_path):
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

        self.df_mysql = spark.read.format("jdbc"). \
            option("url", "jdbc:mysql://localhost:3306/testdb"). \
            option("driver", "com.mysql.jdbc.Driver"). \
            option("user", db_user). \
            option("password", db_password). \
            option("query", "select *, CASE WHEN ground_truth_label = 'yes' THEN 1 ELSE 0 END as ground_truth_label_int from reddit_post_with_labels_human_review"). \
            load()

        self.df_mysql.show()
        return self.df_mysql


    def get_processed_df(self):
        """
        Method to retrieve a processed DataFrame. 
        This method initializes a DataProcessing object and retrieves a clean table. 
        """
        dr = DataProcessing()
        self.df_mysql_clean = dr.get_clean_table(self.df_mysql)
        return self.df_mysql_clean

    def train_test_split(self):
        (self.training_data, self.testing_data) = self.df_mysql_clean.randomSplit([0.8, 0.2], seed=42)
        self.training_data_pandas = self.training_data.toPandas()
        self.testing_data_pandas = self.testing_data.toPandas()
        self.training_data_pandas.to_csv("training_data.csv", index=False)
        self.testing_data_pandas.to_csv("testing_data.csv", index=False)
        return self.training_data, self.testing_data


    def text_processing_for_model(self):
        # regular expression tokenizer
        regexTokenizer = RegexTokenizer(inputCol="body_cleaned", outputCol="words", pattern="\\W")
        # stop words
        stopwordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered")
        stopwords = stopwordsRemover.getStopWords()  # Get the default stop words
        # Add additional stop words if needed
        additional_stopwords = ["i", "we", "he", "she", "is", "like", "and", "the"]
        stopwords += additional_stopwords
        stopwordsRemover.setStopWords(stopwords)
        # TFIDF
        hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=10000)
        idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=5)  # minDocFreq: remove sparse terms

        # pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover,countVectors])
        pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, hashingTF, idf])
        pipelineFit = pipeline.fit(self.training_data)
        self.transformed_training_dataset = pipelineFit.transform(self.training_data)
        self.transformed_testing_dataset = pipelineFit.transform(self.testing_data)
        self.transformed_training_dataset.show()
        self.transformed_testing_dataset.show()

        # Save the pipeline model
        vectorizer_checkpoint = "inference/vectorizer_checkpoint"
        if not os.path.exists("inference"):
            os.makedirs("inference")
        pipelineFit.write().overwrite().save(vectorizer_checkpoint)

        return self.transformed_training_dataset, self.transformed_testing_dataset

    def model_selection(self, model_type):
        # Logistic Regression
        if model_type == "logistic_regression":
            self.model = LogisticRegression(labelCol="ground_truth_label_int", featuresCol="features", maxIter=5, regParam=0.3, elasticNetParam=0)
        elif model_type == "Decision_tree":
            self.model = DecisionTreeClassifier(labelCol="ground_truth_label_int", featuresCol="features", maxDepth=5, minInfoGain=0.001, impurity="entropy")
        elif model_type == "Random_forest":
            self.model = RandomForestClassifier(labelCol="ground_truth_label_int", featuresCol="features", numTrees=100, maxDepth=4, maxBins=32)
        elif model_type == "Gradient_boosting":
            self.model = GBTClassifier(labelCol="ground_truth_label_int", featuresCol="features", maxIter=5)

        # Model = self.model.fit(self.transformed_training_dataset)
        # predictions = Model.transform(self.transformed_testing_dataset)
        # predictions.filter(predictions['prediction'] == 1) \
        #         .select("body", "ground_truth_label_int", "prediction") \
        #         .orderBy("probability", ascending=False) \
        #         .show(n=20, truncate=30)
    

    def model_training_evaluation(self, results):
        evaluator = MulticlassClassificationEvaluator(labelCol="ground_truth_label_int", predictionCol="prediction",
                                                      metricName="accuracy")
        # ParamGrid
        if isinstance(self.model, RandomForestClassifier):
            paramGrid = (ParamGridBuilder()
                         .addGrid(self.model.numTrees, [10, 50, 100])
                         .addGrid(self.model.maxDepth, [2, 5, 10])
                         .build())
        elif isinstance(self.model, DecisionTreeClassifier):
            paramGrid = (ParamGridBuilder()
                         .addGrid(self.model.maxDepth, [2, 5, 10])
                         .build())
        elif isinstance(self.model, LogisticRegression):
            paramGrid = (ParamGridBuilder()
                         .addGrid(self.model.regParam, [0.1, 0.3, 0.5])
                         .build())
        elif isinstance(self.model, GBTClassifier):
            paramGrid = (ParamGridBuilder()
                         .addGrid(self.model.maxIter, [5, 10])
                         .build())
        # n-fold CrossValidator
        cv = CrossValidator(estimator=self.model, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=4)
        cvModel = cv.fit(self.transformed_training_dataset)

        predictions = cvModel.transform(self.transformed_testing_dataset)
        accuracy = evaluator.evaluate(predictions)

        # Precision, Recall, F1
        try:
            evaluator = MulticlassClassificationEvaluator(labelCol="ground_truth_label_int", predictionCol="prediction",
                                                          metricName="weightedPrecision")
            precision = evaluator.evaluate(predictions)
        except Exception as e:
            raise RuntimeError("Error while calculating precision", e) from e

        try:
            evaluator = MulticlassClassificationEvaluator(labelCol="ground_truth_label_int", predictionCol="prediction",
                                                          metricName="weightedRecall")
            recall = evaluator.evaluate(predictions)
        except Exception as e:
            raise RuntimeError("Error while calculating recall", e) from e

        try:
            evaluator = MulticlassClassificationEvaluator(labelCol="ground_truth_label_int", predictionCol="prediction",
                                                          metricName="f1")
            f1 = evaluator.evaluate(predictions)
        except Exception as e:
            raise RuntimeError("Error while calculating f1", e) from e

        # # Confusion Matrix
        # #important: need to cast to float type, and order by prediction, else it won't work
        # try:
        #     preds_and_labels = predictions.select(['prediction','prediction'])\
        #                                   .withColumn('label', F.col('prediction').cast(FloatType())).orderBy('prediction')
        #
        #     #select only prediction and label columns
        #     preds_and_labels = preds_and_labels.select(['prediction','label'])
        #
        #     metrics = MulticlassMetrics(preds_and_labels.rdd.map(tuple))
        #     print("Confusion Matrix:")
        #     print(metrics.confusionMatrix().toArray())
        # except Exception as e:
        #     raise RuntimeError("Error while calculating confusion matrix", e) from e

        with open(results, 'w') as f:
            f.write(f"Test Accuracy: {accuracy:.2f}\n")
            f.write(f"Precision: {precision:.2f}\n")
            f.write(f"Recall: {recall:.2f}\n")
            f.write(f"F1 Score: {f1:.2f}\n")

        print(f"Test Accuracy: {accuracy:.2f}\n")
        print(f"Precision: {precision:.2f}\n")
        print(f"Recall: {recall:.2f}\n")
        print(f"F1 Score: {f1:.2f}\n")
        print(f"Metrics saved in eval_metrics.txt")

        # Save the model checkpoint as a pickle file
        model_checkpoint = "inference/model_checkpoint"
        cvModel.write().overwrite().save(model_checkpoint)
        print(f"Model checkpoint saved as {model_checkpoint}")
        

if __name__ == "__main__":
    dp = data_retrieval()
    dp.get_data('.env')
    dp.get_processed_df()
    dp.train_test_split()
    dp.text_processing_for_model()
    dp.model_selection('logistic_regression')
    dp.model_training_evaluation('eval_metrics_lr.txt')
