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
spark = SparkSession.builder.appName("StopWords").getOrCreate()


# Create DataFrame
sentenceData = spark.createDataFrame([(0, ["Big", "data", "is", "super", "powerful"]), (1, ["This", "is", "going", "to", "be", "epic"])],["id", "raw"])
sentenceData.show(truncate=False)

# Import stop words library
from pyspark.ml.feature import StopWordsRemover

# Run the Remover
remover = StopWordsRemover(inputCol="raw", outputCol="filtered")

# Transform and show data
remover.transform(sentenceData).show(truncate=False)