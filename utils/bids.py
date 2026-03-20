import boto3
import botocore
# For anonymous access to the bucket.
from botocore import UNSIGNED
from botocore.client import Config
from botocore.handlers import disable_signing
import pdb
import glob
import os
import re

def s3_client(access_key, host, secret_key):
    '''
    Build boto3 client to access s3 buckets
    '''
    session = boto3.session.Session()
    client = session.client('s3',endpoint_url=host, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    return client

    
def s3_get_bids_subjects(access_key, bucketName, host, prefix, secret_key):
    '''
    Page through BIDS s3 bucket to find subjects to analyze
    '''
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    paginator = client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucketName, Delimiter='/', Prefix=prefix,
                                            EncodingType='url',ContinuationToken='',
                                            FetchOwner=False, StartAfter='')
    get_data = client.list_objects_v2(Bucket=bucketName, Delimiter='/', Prefix=prefix,
                                            EncodingType='url', MaxKeys=1000, ContinuationToken='',
                                            FetchOwner=False, StartAfter='')
    bids_subjects = []
    for page in page_iterator:
        page_bids_subjects = ['sub-'+item['Prefix'].split('sub-')[1].strip('/') for item in page['CommonPrefixes'] if 'sub' in item['Prefix']]
        bids_subjects.extend(page_bids_subjects)
    return bids_subjects

def s3_get_bids_sessions(access_key, bucketName, host, prefix, secret_key):
    '''
    Find sessions for a given subject (included in the prefix variable)
    '''
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    get_data = client.list_objects_v2(Bucket=bucketName, Delimiter='/', EncodingType='url',
                                          MaxKeys=1000, Prefix=prefix, ContinuationToken='',
                                          FetchOwner=False, StartAfter='')
    bids_sessions = []
    for item in get_data['CommonPrefixes']:
        ses = re.findall("ses-[^\/]+",item["Prefix"])[0]
        bids_sessions.append(ses)
    return bids_sessions
        
