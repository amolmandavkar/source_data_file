print("hello git")

from pyspark.sql import SparkSession
# create a SparkSession
spark = SparkSession.builder.master("local[*]").appName("dataframe").getOrCreate()
# define Oracle database connection properties
j_url = "jdbc:oracle:thin:@//192.168.0.129:1521/xe"
pro = {
    "user": "sys as SYSDBA",
    "password": "xyz",
    "driver": "oracle.jdbc.driver.OracleDriver"
}
# read data from Oracle database into Spark dataframe
df = spark.read.jdbc(url=j_url, table="club_csv", properties=pro)
df.show(10)
from io import StringIO
import boto3
pandas_df = df.toPandas()

csv_buffer = StringIO()
pandas_df.to_csv(csv_buffer, index=False)

s3 = boto3.resource('s3')
s3.Object('abc', 'club_df.csv').put(Body=csv_buffer.getvalue())


