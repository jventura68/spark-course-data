import argparse
import os
import boto3
import json
import userPrompt


def delete_all_streams(region_name='us-west-2'):
    """
    Executes Kinesis.Client.describe_stream_summary function
    """
    client = boto3.client('kinesis', region_name=region_name)
    result = client.list_streams()

    if 'StreamNames' in result:
        if len(result['StreamNames'])==0:
            print ('No se ha encontrado ningún Stream definido')
        else:
            userPrompt.prompt('Desea borrar los {} Streams encontrados?'.\
                               format(len(result['StreamNames'])))
        for st in result['StreamNames']:
             client.delete_stream(StreamName=st)


def parse_known_args():
    # AWS credentials should be provided as environ variables
    if 'AWS_ACCESS_KEY_ID' not in os.environ:
        print('Error. Please setup AWS_ACCESS_KEY_ID')
        exit(1)

    elif 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        print('Error. Please setup AWS_SECRET_ACCESS_KEY')
        exit(1)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--region_name', required=False,
        help='AWS Region', default='us-west-2')
    args, extra_params = parser.parse_known_args()
    return args, extra_params


def main():
    """
    List all AWS Kinesis streams associated to a given AWS account
    using boto3 library
    """
    args, _ = parse_known_args()
    delete_all_streams(
        region_name=args.region_name)
    

if __name__ == '__main__':
    main()
