import os
import boto3
import userPrompt

def delete_all_buckets():
    """
    Executes the function S3.Client.list_buckets to get a list
    a of buckets by account
    """
    client = boto3.client('s3')
    bucket_list = client.list_buckets()
    if 'Buckets' not in bucket_list:
        print ('No se hay definido ningún Bucket')
    if 'Buckets' in bucket_list:
        if len(bucket_list['Buckets']) == 0:
            print ('No se hay definido ningún Bucket')
        else:
            userPrompt.prompt ('Se han encontrado {} Buckets, desea borrarlos?'.\
                          format(len(bucket_list['Buckets'])))
        for b in bucket_list['Buckets']:
            bucket = b['Name']
            data = client.list_objects_v2(Bucket=bucket)
            if 'Contents' in data:
                objects=[]
                for r in data['Contents']:
                    objects.append (
                        {'Key': r['Key']}
                    )
                print ('deleting objects: ',objects)
                client.delete_objects(
                    Bucket=bucket,
                    Delete={'Objects':objects,
                            'Quiet':True
                    }
                )

            print ('deleting Bucket',b['Name'],'...')
            client.delete_bucket(Bucket=b['Name'])


def main():
    """
    List AWS S3 buckets using boto3 library
    """
    # AWS credentials should be provided as environ variables
    if 'AWS_ACCESS_KEY_ID' not in os.environ:
        print('Error. Please setup AWS_ACCESS_KEY_ID')
        exit(1)

    elif 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        print('Error. Please setup AWS_SECRET_ACCESS_KEY')
        exit(1)
    delete_all_buckets()

if __name__ == '__main__':
    main()
