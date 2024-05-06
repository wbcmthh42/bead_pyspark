#import findspark
import sys
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer,StringIndexer, RegexTokenizer,StopWordsRemover
from pyspark.ml.feature import HashingTF, IDF
from pyspark.sql.functions import col, udf,regexp_replace,isnull
from pyspark.sql.types import StringType,IntegerType
from pyspark.ml.classification import NaiveBayes, RandomForestClassifier, LogisticRegression, DecisionTreeClassifier, GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.functions import rand
#import boto3

# for local machine only
def s3_to_pyspark(bucket_name,file_name,aws_key,aws_secret):
    s3 = boto3.resource('s3', aws_access_key_id = aws_key,
                          aws_secret_access_key= aws_secret )
    s3.Bucket(bucket_name).download_file(file_name,file_name)
    findspark.init()
    spark = SparkSession \
        .builder \
        .appName("PySpark-TextClassifier") \
        .getOrCreate()
    data = spark.read.csv(file, header=True, inferSchema=True)
    
    return data

def preprocess_data(data):
    data = data.dropna()
    data = data.withColumn("label", col("label").cast("int"))
    data = data.filter((data['label'] == 0) | (data['label'] == 1))
    
    # regular expression tokenizer
    regexTokenizer = RegexTokenizer(inputCol="body", outputCol="words", pattern="\\W")
    
    # stop words
    stopwordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered")
    stopwords = stopwordsRemover.getStopWords()  # Get the default stop words
    
    # Add additional stop words if needed
    additional_stopwords = ["i", "we", "he", "she", "is", "like", "and", "the"]
    stopwords += additional_stopwords
    stopwordsRemover.setStopWords(stopwords)
    
    # TFIDF
    hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=10000)
    idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=5)
    
    # Train test split
    (training_data, testing_data) = data.randomSplit([0.7, 0.3])
    
    # Define pipeline
    pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, hashingTF, idf])
    
    # Fit pipeline to training data
    pipelineFit = pipeline.fit(training_data)
    training_data = pipelineFit.transform(training_data)
    pipelineFit.write().overwrite().save("s3://tansw-bead2024/vec/")
    
    # Fit pipeline to testing data
    pipelineFit = pipeline.fit(testing_data)
    testing_data = pipelineFit.transform(testing_data)
    
    return training_data, testing_data

def train_classifier(selection, training_data):
    if selection == 'lr':
        model = LogisticRegression(maxIter=5, regParam=0.3, elasticNetParam=0)
    elif selection == 'dt':
        model = DecisionTreeClassifier(labelCol="label", featuresCol="features", maxDepth=3, minInfoGain=0.001, impurity="entropy")
    elif selection == 'rf':
        model = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=20, maxDepth=3, maxBins=32)
    elif selection == 'gbt':
        model = GBTClassifier(labelCol="label", featuresCol="features", maxIter=3)
    else:
        raise ValueError("Invalid selection: {}".format(selection))
    
    # Fit the model to the training data
    trained_model = model.fit(training_data)
    trained_model.write().overwrite().save("s3://tansw-bead2024/model/")
    print("Trained model:", model)
    
    return trained_model

def evaluate_model(predictions):
    # Define evaluators
    evaluator1 = BinaryClassificationEvaluator(rawPredictionCol="prediction", labelCol="label", metricName="areaUnderROC")
    evaluator2 = BinaryClassificationEvaluator(rawPredictionCol="prediction", labelCol="label", metricName="areaUnderPR")
    evaluator3 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    evaluator4 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
    evaluator5 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
    evaluator6 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall")
    
    # Print evaluation results
    print("Area Under ROC:", "{:.4f}".format(evaluator1.evaluate(predictions)))
    print("Area Under PR:", "{:.4f}".format(evaluator2.evaluate(predictions)))
    print("Accuracy:", "{:.4f}".format(evaluator3.evaluate(predictions)))
    print("F1 Score:", "{:.4f}".format(evaluator4.evaluate(predictions)))
    print("Weighted Precision:", "{:.4f}".format(evaluator5.evaluate(predictions)))
    print("Weighted Recall:", "{:.4f}".format(evaluator6.evaluate(predictions)))
    
    return

def predicted_rad_to_s3(predictions,bucket,file_out,aws_key,aws_secret):
    df =predictions.filter(predictions['prediction'] == 1) \
    .select("submission_id", "comment_id", "timestamp", "author", "body", "label", "prediction") \
    .orderBy("author", ascending=True) \
    .toPandas()
    df.to_csv(f'{file_out}.csv')
    s3 = boto3.resource('s3', aws_access_key_id = aws_key,
                          aws_secret_access_key= aws_secret )
    my_bucket = s3.Bucket(bucket)
    with open(f"{file_out}.csv", 'rb') as data:
        s3.Bucket(bucket).put_object(Key= f"{file_out}.csv", Body=data)
    
    return


if __name__ == "__main__":
    aws_key = ''   ######################## input aws key when using local machine only####################
    aws_secret = ''    ################## input aws secret when using local machine only ######################
    bucket = 'tansw-bead2024' ## input bucket ##
    file = 'reddit_posts_with_labels_after_human_review_manual_v4.csv' ##  input file ##
    
    spark = SparkSession \
        .builder \
        .appName("PySpark-TextClassifier") \
        .getOrCreate()
    data = spark.read.csv("s3://tansw-bead2024/reddit_posts_with_labels_after_human_review_manual_v4.csv", header=True, inferSchema=True)

    # data = s3_to_pyspark(bucket,file,aws_key,aws_secret) for local machine only
    train, test = preprocess_data(data)
    model = train_classifier('rf',train)
    model2 = model.load("s3://tansw-bead2024/model/")
   

    predictions = model2.transform(test)
    evaluate_model(predictions)
    # predicted_rad_to_s3(predictions,bucket,'results',aws_key,aws_secret)

    # show detected radical comments
    predictions.filter(predictions['prediction'] == 1) \
    .select("submission_id","comment_id","timestamp","author","body","label","prediction") \
    .orderBy("author", ascending=True) \
    .show(n = 20, truncate = 30)

    ##### show wrong predictions
    predictions.filter(predictions['prediction'] != predictions['label']) \
    .select("submission_id", "comment_id", "timestamp", "author", "body", "label", "prediction") \
    .orderBy("author", ascending=True) \
    .show(n=20, truncate=30)

    ##### show top radical users ###########
    predictions.filter(predictions['prediction'] == 1) \
    .groupBy("author") \
    .agg({'comment_id': 'count'}) \
    .orderBy(col("count(comment_id)").desc()) \
    .show()