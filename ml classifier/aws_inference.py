import sys
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer,StringIndexer, RegexTokenizer,StopWordsRemover
from pyspark.ml.feature import HashingTF, IDF
from pyspark.sql.functions import col, udf,regexp_replace,isnull
from pyspark.sql.types import StringType,IntegerType
from pyspark.ml.classification import NaiveBayes, RandomForestClassifier, LogisticRegression, DecisionTreeClassifier, GBTClassifier, RandomForestClassificationModel
from pyspark.ml.tuning import CrossValidatorModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.functions import rand
from pyspark.ml.pipeline import PipelineModel


if __name__ == "__main__":
    
    spark = SparkSession \
        .builder \
        .appName("PySpark-TextClassifier") \
        .getOrCreate()
    # data = spark.read.csv("s3://tansw-bead2024/reddit_posts_with_labels_after_human_review_manual_v4.csv", header=True, inferSchema=True)
    data = spark.read.csv("s3://tansw-bead2024/testing_data.csv", header=True, inferSchema=True)
    data = data.dropna()
    vec = PipelineModel.load("s3://tansw-bead2024/vectorizer_checkpoint/")
    
    # vec = PipelineModel.load("s3://tansw-bead2024/vec/")
    
    data2  = vec.transform(data)
    # model = RandomForestClassificationModel.load("s3://tansw-bead2024/model/")
    model = CrossValidatorModel.load("s3://tansw-bead2024/model_checkpoint/")


    predictions = model.transform(data2)
    # predictions.show()
    radical = predictions.filter(predictions['prediction'] == 1) \
    .select("submission_id", "comment_id", "timestamp", "author", "body", "label", "prediction") \
    .orderBy("author", ascending=True) 
    radical.show()
    radical.write.mode("overwrite").csv("s3://tansw-bead2024/predicted_radical")