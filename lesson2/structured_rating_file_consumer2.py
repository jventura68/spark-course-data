from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import (
    StructType, StructField, FloatType, StringType, IntegerType
)

from pyspark.sql.streaming import DataStreamReader

import argparse
import os

def consume_records():
    spark_context = SparkContext (appName='RatingConsumer')
    sql_context = SQLContext(spark_context)
    stream_reader = DataStreamReader (sql_context)
    
    fpath = os.path.join (os.environ['SPARK_DATA'], 'structured')
    
    fields = [
        StructField ('userId', IntegerType(), True),
        StructField ('movieId', IntegerType(), True),
        StructField ('rating', FloatType(), True),
        StructField ('timestamp', StringType(), True),
    ]
    
    schema = StructType (fields)
    ratings = stream_reader.load (fpath, schema=schema, format='csv')
    
    ratings.createOrReplaceTempView ('ratingsView')
    
    #user_481 = sql_context.sql ("select userId, rating from ratingsView where userId < 481")
    user_481 = ratings.where ("userId < 481").select("userId","rating")
    
    query = user_481\
        .writeStream\
        .outputMode ('append')\
        .format ('console')\
        .start()
    query.awaitTermination()
    
def main():
    if 'SPARK_DATA' not in os.environ:
        print('Error. Please define SPARK_DATA variable')
        exit(1)
    
    parser = argparse.ArgumentParser()
    args, extra_params = parser.parse_known_args()
    consume_records()
    
if __name__ == '__main__':
    main()