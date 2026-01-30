import os
import json
import boto3
import botocore
import glob
# For anonymous access to the bucket.
from botocore import UNSIGNED
from botocore.client import Config
from botocore.handlers import disable_signing
from .bids import s3_client
import pdb      

def fmriprep_exec_sum(output_dir):
    # find executime summary in fmriprep/nibabies output dirs
    suffix = ".html"
    exec_sum = glob.glob(output_dir + "/sub-*/sub-*" + suffix, recursive=True)
    if exec_sum:
        stage_status = "FOUND_EXEC-SUM"
    else:
        stage_status = "NO_EXEC-SUM"
    return stage_status

def s3_fmriprep_exec_sum(access_key, host, secret_key, bucket_name, prefix):
    # prefix is relative path to outputs (NO leading /!!!)
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    suffix = ".html"
    try:
        list_objects = client.list_objects_v2(Bucket=bucket_name, EncodingType='url',MaxKeys=1000,Prefix=prefix,ContinuationToken='',FetchOwner=False,StartAfter='')
    except KeyError:
        s3_status = 'NO_OUTPUTS'
        return s3_status
    try:
        s3_status = ''
        for obj in list_objects["Contents"]:
            key = obj["Key"]
            if key.endswith(suffix):
                s3_status = "FOUND_EXEC-SUM"
            if not s3_status:
                s3_status = "NO_OUTPUTS"
            return s3_status
    except KeyError:
        s3_status = "NO_EXEC-SUM"
        return s3_status

def xcpd_exec_sum(output_dir):
    # find executive summary in xcp-d output dirs 
    suffix = "executive_summary.html"
    exec_sum = glob.glob(output_dir + "/sub-*/sub-*/ses-*/" + suffix, recursive=True)
    if exec_sum:
        stage_status = "FOUND_EXEC-SUM"
    else:
        stage_status = "NO_EXEC-SUM"
    return stage_status

def s3_fmriprep_exec_sum(access_key, host, secret_key, bucket_name, prefix):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    suffix = "executive_summary.html"
    try:
        list_objects = client.list_objects_v2(Bucket=bucket_name, EncodingType='url',MaxKeys=1000,Prefix=prefix,ContinuationToken='',FetchOwner=False,StartAfter='')
    except KeyError:
        s3_status = 'NO_OUTPUTS'
        return s3_status
    try:
        s3_status = ''
        for obj in list_objects["Contents"]:
            key = obj["Key"]
            if key.endswith(suffix):
                s3_status = "FOUND_EXEC-SUM"
            if not s3_status:
                s3_status = "NO_OUTPUTS"
            return s3_status
    except KeyError:
        s3_status = "NO_EXEC-SUM"
        return s3_status

def fmriprep_crash_log(output_dir):
    # find crash logs in fmriprep/nibabies output dirs
    logs = glob.glob(output_dir + "/sub-*/sub-*/ses-*/log/*",recursive=True) ## TODO: Could this be simplified with /**/log/*?
    recent_dir = get_most_recent_dir(logs)
    # determine if there was a crash log created in most recent run
    crashed = ""
    crash_logs = glob.glob(recent_dir + '/crash*')
    if crash_logs:
        crashed = "CRASHED"
    else:
        crashed = "NO_CRASH"
    return crashed

##TODO: Could the crash log functions be combined if the glob can be simplified to /**/log*?

def xcpd_crash_log(output_dir):
    # find crash logs in xcp-d output dirs
    logs = glob.glob(output_dir + "/sub-*/sub-*/log/*",recursive=True) ## TODO: Could this be simplified with /**/log/*?
    recent_dir = get_most_recent_dir(logs)
    # determine if there was a crash log created in most recent run
    crashed = ""
    crash_logs = glob.glob(recent_dir + '/crash*')
    if crash_logs:
        crashed = "CRASHED"
    else:
        crashed = "NO_CRASH"
    return crashed

def get_most_recent_dir(log_dirs):
    # determine which log directory is most recent
    most_recent = 1
    recent_dir = ""
    for folder in log_dirs:
        created = os.path.getctime(folder)
        if created > most_recent:
            most_recent = created
            recent_dir = folder
    return recent_dir     
    
