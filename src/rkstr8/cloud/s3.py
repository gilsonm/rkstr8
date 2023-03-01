from . import BotoClientFactory, Service, json_serial
from botocore.client import ClientError
import boto3
import botocore
from pathlib import Path
from urllib.parse import urlparse
import logging
import os
import json

logger = logging.getLogger(__name__)

s3_resource = boto3.resource('s3')


def uri_to_bucket_key(s3_uri):
    parsed_uri = urlparse(s3_uri)
    bucket = parsed_uri.netloc
    key = parsed_uri.path.lstrip('/')
    return bucket, key


def s3_uri_exists(s3_uri):

    print('s3_uri_exists: %s' % s3_uri)

    bucket, key = uri_to_bucket_key(s3_uri)

    print('b: {}, k: {}'.format(bucket, key))

    try:
        print('Testing...')
        s3_resource.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        print('botocore.exceptions.ClientError')
        print(e.response)
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise e
    else:
        return True


class S3Upload(object):

    @staticmethod
    def upload_file(local_path, bucket_name, key_name):
        # check if local template file exists
        if not os.path.isfile(local_path):
            raise ValueError('Could not find file, {}, to upload to S3'.format(local_path))

        s3_client = BotoClientFactory.client_for(Service.S3)

        # check if bucket exists...
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except ClientError as ce:
            logger.error('Could not find bucket, {}, in S3.'.format(bucket_name))
            logger.error('Or bucket exists, but did not have HEAD_OBJECT permissions on it.')
            raise ce

        logger.debug('Attempting to upload {local_file} to s3://{bucket}/{key}..'.format(local_file=local_path,
                                                                                         bucket=bucket_name,
                                                                                         key=key_name))

        response = s3_client.put_object(
            Body=open(local_path, 'rb'),
            Bucket=bucket_name,
            Key=key_name
        )

        logger.debug('Upload response: ')
        logger.debug(json.dumps(response, indent=4, sort_keys=False, default=json_serial))

    @staticmethod
    def object_exists(bucket_name, key_name):
        logger.debug('Attempting to test existence of s3://{bucket}/{key}..'.format(bucket=bucket_name, key=key_name))
        try:
            s3_client = BotoClientFactory.client_for(Service.S3)

            response = s3_client.head_object(
                Bucket=bucket_name,
                Key=key_name
            )
            logger.debug('head_object response: ')
            logger.debug(json.dumps(response, indent=4, sort_keys=False, default=json_serial))
            return ('ContentLength' in response) and (response['ContentLength'] > 0)
        except ClientError as ce:
            https_url = '/'.join(('https://s3.amazonaws.com', bucket_name, key_name))
            logger.error('Could not find object, {}, in S3.'.format(https_url))
            logger.error('Or object exists, but did not have HEAD_OBJECT permissions on it.')
            logger.error(ce)
            return False
