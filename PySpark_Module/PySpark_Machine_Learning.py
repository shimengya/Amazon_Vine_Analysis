import os
# Find the latest version of spark 3.0  from http://www.apache.org/dist/spark/ and enter as the spark version
# For example:
# spark_version = 'spark-3.0.3'
spark_version = 'spark-3.3.1'
os.environ['SPARK_VERSION']=spark_version

# Install Spark and Java
!apt-get update
!apt-get install openjdk-11-jdk-headless -qq > /dev/null
!wget -q http://www.apache.org/dist/spark/$SPARK_VERSION/$SPARK_VERSION-bin-hadoop2.tgz
!tar xf $SPARK_VERSION-bin-hadoop2.tgz
!pip install -q findspark

# Set Environment Variables
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = f"/content/{spark_version}-bin-hadoop2"

# Start a SparkSession
import findspark
findspark.init()


# Start Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Tokens").getOrCreate()


from pyspark.ml.feature import Tokenizer

#Create sample DataFrame
dataframe = spark.createDataFrame([(0,"Spark is great"), (1,"We are learning Spark"), (2,"Spark is better than hadoop no doubt")],["id", "sentence"])
dataframe.show()

# Tokenize sentences
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
tokenizer

# Transform and show DataFrame
tokenized_df = tokenizer.transform(dataframe)
tokenized_df.show(truncate=False)

# Create a function to return the length of a list
def word_list_length(word_list):
    return len(word_list)


from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

# Create a user defined function
count_tokens = udf(word_list_length, IntegerType())

# Create our Tokenizer
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")

# Transform DataFrame
tokenized_df=tokenizer.transform(dataframe)

# Select the needed columns and don't truncate results
tokenized_df.withColumn("tokens", count_tokens(col("words"))).show(truncate=False)