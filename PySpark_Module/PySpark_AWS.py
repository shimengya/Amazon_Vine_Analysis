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

!wget https://jdbc.postgresql.org/download/postgresql-42.2.17.jar

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CloudETL").config("spark.driver.extraClassPath","/content/postgresql-42.2.17.jar").getOrCreate()

# Read in data from S3 Buckets
from pyspark import SparkFiles
url ="https://miashi-bucket.s3.amazonaws.com/user_data.csv"
spark.sparkContext.addFile(url)
user_data_df = spark.read.csv(SparkFiles.get("user_data.csv"), sep=",", header=True, inferSchema=True)
# Show DataFrame
user_data_df.show()

url ="https://miashi-bucket.s3.amazonaws.com/user_payment.csv"
spark.sparkContext.addFile(url)
user_payment_df = spark.read.csv(SparkFiles.get("user_payment.csv"), sep=",", header=True, inferSchema=True)

# Show DataFrame
user_payment_df.show()


# Join the two DataFrame
user_payment_df = user_payment_df.join(user_payment_df, on = "usename", how = "inner")
user_payment_df

# Drop null values
dropna_df = joined_df.dropna()
dropna_df.show()

# Load in a sql function to use columns
from pyspark.sql.functions import col

# Filter for only column with active users
cleaned_df = dropna_df.filter(col("active_user") == True)
cleaned_df.show()

# Create user dataframe to match active_suer table
clean_user_df = cleaned_df.select(["id", "first_name", "last_name", "username"])
clean_user_df.show()

# Create user dataframe to match billing_info table
clean_billing_df = cleaned_df.select(["billing_id", "street_address", "state", "username"])
clean_billing_df.show()

# Create user dataframe to match payment_info table
clean_payment_df = cleaned_df.select(["billing_id", "cc_encrypted"])
clean_payment_df.show()

# Store environmental variable
from getpass import getpass
password = getpass('Enter database password')
# Configure settings for RDS
mode = "append"
jdbc_url="jdbc:postgresql://dataviz.cssmblf2ngiv.us-east-1.rds.amazonaws.com:5433/medical"
config = {"user":"postgres",
          "password": XXXX,
          "driver":"org.postgresql.Driver"}
        

# Write DataFrame to active_user table in RDS
clean_user_df.write.jdbc(url=jdbc_url, table='active_user', mode=mode, properties=config)

# Write dataframe to billing_info table in RDS
clean_billing_df.write.jdbc(url=jdbc_url, table='billing_info', mode=mode, properties=config)

# Write dataframe to payment_info table in RDS
clean_payment_df.write.jdbc(url=jdbc_url, table='payment_info', mode=mode, properties=config)