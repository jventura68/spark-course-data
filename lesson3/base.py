from pyspark import SparkContext
from pyspark.streaming import StreamingContext

interval = 1
spark_context = SparkContext(appName='base.py')
stream_context = StreamingContext(spark_context, interval)

StreamName = 'test'
region_name = 'us-west-2'
endpoint = 'https://kinesis.{}.amazonaws.com/'.format(region_name)


from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

stream = KinesisUtils.createStream(
    stream_context, 'EventLKinesisConsumer', StreamName, endpoint,
    region_name, InitialPositionInStream.LATEST, interval)
