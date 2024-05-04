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
from pyspark.ml import PipelineModel
from pyspark.ml.tuning import CrossValidatorModel

spark = SparkSession.builder.appName("QS_10").getOrCreate()

inputFilePath="s3://aws-emr-studio-533267180383-ap-southeast-1/input/df_raw_clean.csv"

data = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(inputFilePath) )

data = data.na.drop()
total_rows = data.count()

# Function to load model
def inference(text, pipeline_fit, model):
    transformed_text = pipeline_fit.transform(text)
    predictions = model.transform(transformed_text)
    return predictions


vectorizer = PipelineModel.load("s3://aws-emr-studio-533267180383-ap-southeast-1/model/inference/vectorizer_checkpoint/")
model = CrossValidatorModel.load("s3://aws-emr-studio-533267180383-ap-southeast-1/model/inference/model_checkpoint/")

predictions = inference(data, vectorizer, model)
df = predictions.drop('words', 'filtered', 'rawFeatures', 'features', 'rawPrediction', 'probability')
df.show(1000)

output_uri="s3://aws-emr-studio-533267180383-ap-southeast-1/output/"
df.repartition(1).write.option("header", "true").mode("overwrite").csv(output_uri)

print("BEAD TEAM IS HERE")