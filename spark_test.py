# https://dimajix.de/running-pyspark-on-anaconda-in-pycharm/?lang=en

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('udfs').getOrCreate()
print(f'Spark version == {spark.version}')

data = [(1, 'Alex', 40000000,90000),(2, 'Nadya', 3000000,9900)]
schema = ['id','name','salary','bonus']

df = spark.createDataFrame(data,schema)
df.show()



