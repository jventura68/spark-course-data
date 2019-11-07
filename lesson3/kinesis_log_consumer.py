from datetime import datetime
from glob import glob
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

import argparse
import boto3
import json
import os
from dstreamShow import show
import re


def parse_log_entry(msg):
    """
    Parse a log entry from the format
    $ip_addr - [$time_local] "$request" $status $bytes_sent $http_user_agent"
    to a dictionary
    """
    data = {}

    # Regular expression that parses a log entry
    search_term = '(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+\-\s+\[(.*)]\s+'\
        '"(\/[/.a-zA-Z0-9-]*)"\s+(\d{3})\s+(\d+)\s+"(.*)"'

    values = re.findall(search_term, msg)

    if values:
        val = values[0]
        data['ip'] = val[0]
        data['date'] = val[1]
        data['path'] = val[2]
        data['status'] = val[3]
        data['bytes_sent'] = val[4]
        data['agent'] = val[5]
    return data


def update_global_event_counts(key_value_pairs):
    def update(new_values, accumulator):
        if accumulator is None:
            accumulator = 0
        return sum(new_values, accumulator)

    return key_value_pairs.updateStateByKey(update)

def aggregate_by_event_type(record, column='agent'):
    """
    Step 1. Maps every entry to a dictionary.
    Step 2. Transform the dataset in a set of
        tuples (event, 1)
    Step 3: Applies a reduction by event type
        to count the number of events by type
        in a given interval of time.
    """
    show (record,"datos de entrada")
    return record\
        .map(lambda x: json.loads(x)['value'])\
        .map(parse_log_entry)\
        .map(lambda record: (record[column], 1))\
        .reduceByKey(lambda a, b: a+b)

def test(record):
    r1=record.map(lambda x: json.loads(x)['value'])
    show (r1,"Datos de entrada")
    
def send_record(rdd, Bucket):
    """
    If rdd size is greater than 0, store the
    data as text in S3
    """
    if rdd.count() > 0:
        client = boto3.client('s3')
        data_dir = os.path.join(
            os.environ['SPARK_DATA'],
            'streams', 'kinesis_{}'.format(datetime.utcnow().timestamp()))
        rdd.saveAsTextFile(data_dir)
        for fname in glob('{}/part-0000*'.format(data_dir)):
            client.upload_file(fname, Bucket, fname)


def consume_records(
        interval=1, StreamName=None, region_name='us-west-2', Bucket=None):
    """
    Create a local StreamingContext with two working
    thread and batch interval
    """
    assert StreamName is not None

    endpoint = 'https://kinesis.{}.amazonaws.com/'.format(region_name)

    sc, stream_context = initialize_context(interval=interval)
    sc.setLogLevel("ERROR")
    print ('create stream')
    stream = KinesisUtils.createStream(
        stream_context, 'LogKinesisConsumer', StreamName, endpoint,
        region_name, InitialPositionInStream.LATEST, interval)
    #LATEST

    # counts number of events
    event_counts = aggregate_by_event_type(stream)
    global_counts = update_global_event_counts(event_counts)
    global_counts.pprint()
    # Sends data to S3
    global_counts.foreachRDD(lambda rdd: send_record(rdd, Bucket))
    #test(stream)
    stream_context.start()
    print ('stream iniciado')
    stream_context.awaitTermination()
    stream_context.stop()
    sc.stop()


def initialize_context(interval=1, checkpointDirectory='/tmp'):
    """
    Creates a SparkContext, and a StreamingContext object.
    Initialize checkpointing
    """
    spark_context = SparkContext(appName='EventKinesisConsumer')
    stream_context = StreamingContext(spark_context, interval)
    stream_context.checkpoint(checkpointDirectory)
    return spark_context, stream_context


def parse_known_args():
    # AWS credentials should be provided as environ variables
    if 'AWS_ACCESS_KEY_ID' not in os.environ:
        print('Error. Please setup AWS_ACCESS_KEY_ID')
        exit(1)
    elif 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        print('Error. Please setup AWS_SECRET_ACCESS_KEY')
        exit(1)

    if 'SPARK_DATA' not in os.environ:
        print('Error. Please define SPARK_DATA variable')
        exit(1)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--interval', required=False, default=1.0,
        help='Interval in seconds', type=float)

    parser.add_argument(
        '--region_name', required=False,
        help='AWS Region', default='us-west-2')

    parser.add_argument('--StreamName', required=True, help='Stream name')

    parser.add_argument(
        '--Bucket', required=True, help='S3 Bucket Name'
    )

    args, extra_params = parser.parse_known_args()

    return args, extra_params


def main():
    args, extra_params = parse_known_args()
    consume_records(
        interval=args.interval, StreamName=args.StreamName,
        Bucket=args.Bucket, region_name=args.region_name)


if __name__ == '__main__':
    main()
