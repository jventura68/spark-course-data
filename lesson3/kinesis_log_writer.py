from datetime import datetime

import argparse
import os
import boto3
import random
import time
import json
import uuid


HTTP_CODES = (
    200, 201, 300, 301, 302, 400, 401, 404, 500, 502
)


PATHS = (
    '/', '/home', '/login', '/user', '/user/profile',
    '/user/network/friends'
)


AGENTS = (
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',
    'Chrome/41.0.2224.3 Safari/537.36',
    'EsperanzaBot(+http://www.esperanza.to/bot/)',
)


def get_line():
    """
    Similates a log entry for a webserver in the format
    $remote_addr - [$time_local] "$request" $status $body_bytes_sent
    $http_user_agent"
    """
    base = '{ip} - [{time}] "{request}" {status} {bytes} "{agent}"'
    data = {}
    data['ip'] = '{0}.{1}.{2}.{3}'.format(
        random.randint(0, 255), random.randint(0, 255), random.randint(0, 255),
        random.randint(0, 255))

    data['time'] = datetime.utcnow()
    data['request'] = random.choice(PATHS)
    data['status'] = random.choice(HTTP_CODES)
    data['bytes'] = random.randint(1, 2048)
    data['agent'] = random.choice(AGENTS)
    return data['agent'], base.format(**data)


def generate_records(StreamName=None, delay=1, region_name='us-west-2'):
    assert StreamName is not None
    client = boto3.client('kinesis', region_name=region_name)
    for n in range(5):
        agent, line = get_line()
        payload = {
            'value': line,
            'timestamp': str(datetime.utcnow()),
            'id': str(uuid.uuid4())
        }
        time.sleep(delay)
        
        '''
        Utilizamos agent como partition key
        Todas las peticiones con el mismo agent van a la misma
        shard
        '''
        r = client.put_record(
            StreamName=StreamName,
            Data=json.dumps(payload),
            PartitionKey=agent
        )
        print('Record {} stored in shard {}'.format(agent[:8],r['ShardId']))


def main():

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
    parser.add_argument('--StreamName', required=True, help='Stream name')
    parser.add_argument(
        '--delay', required=False, default=1,
        help='Delay in seconds between records', type=int)

    args, extra_params = parser.parse_known_args()
    generate_records(
        StreamName=args.StreamName,
        delay=args.delay, region_name=args.region_name)

if __name__ == '__main__':
    main()
