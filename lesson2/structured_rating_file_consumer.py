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
    
    user_counts = ratings.groupBy ('userId').count()
    
    query = user_counts\
        .writeStream\
        .outputMode ('complete')\
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