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

    try:
        list_objects = client.list_objects_v2(Bucket=bucket_name, EncodingType='url',MaxKeys=1000,Prefix=prefix,ContinuationToken='',FetchOwner=False,StartAfter='')
    except KeyError:
        s3_status = 'NO_OUTPUTS'
        return s3_status
    s3_status = ''
    try:
        for obj in list_objects["Contents"]:
            # Loop through Keys (filepaths) to find a file that starts with "sub-" and ends with ".html"
            # Keys are ALL of the contents of a subfolder, so will run into other HTML files that aren't relevant
            # Check that there is only 1 extra value in split of keys (compared to split of prefix) to ensure
            # its only looking at first level files
            key = obj["Key"]
            key_len = len(key.split(r"%2F"))
            prefix = prefix.replace("/",r"%2F") # Key object doesn't have / for subfolder delineation, instead has %2F
            prefix_len = len(prefix.split(r"%2F"))
            # Only check for htmls in the first level dir 
            if key_len == prefix_len:
                file = key.split(prefix)[-1].replace(r"%2F","") # Remove prefix to get filename 
                if file.startswith("sub-") and file.endswith(".html"):
                    s3_status = "FOUND_EXEC-SUM"
                    return s3_status
            else: 
                s3_status = "NO_EXEC-SUM"
        return s3_status
    except KeyError:
        s3_status = 'NO_OUTPUTS'
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

def s3_xcpd_exec_sum(access_key, host, secret_key, bucket_name, prefix):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    
    try:
        list_objects = client.list_objects_v2(Bucket=bucket_name, EncodingType='url',MaxKeys=1000,Prefix=prefix,ContinuationToken='',FetchOwner=False,StartAfter='')
    except KeyError:
        s3_status = 'NO_OUTPUTS'
        return s3_status
    s3_status = ''
    try:
        for obj in list_objects["Contents"]:
            # Loop through Keys (filepaths) to find a file that starts with "sub-" and ends with ".html"
            key = obj["Key"]
            key_len = len(key.split(r"%2F"))
            prefix = prefix.replace("/",r"%2F") # Key object doesn't have / for subfolder delineation, instead has %2F
            prefix_len = len(prefix.split(r"%2F"))
            # Only check for htmls in the first level dir 
            if key_len == prefix_len:
                file = key.split(prefix)[-1].replace(r"%2F","") # Remove prefix to get filename 
                if file.startswith("sub-") and file.endswith("executive_summary.html"):
                    s3_status = "FOUND_EXEC-SUM"
                    return s3_status
            else: 
                s3_status = "NO_EXEC-SUM"
        return s3_status
    except KeyError:
        s3_status = 'NO_OUTPUTS'
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

def s3_xcpd_crash_log(access_key, host, secret_key, bucket_name, prefix):
    client = s3_client(access_key=access_key,host=host,secret_key=secret_key)
    crash_prefix = "crash"
    try:
        list_objects = client.list_objects_v2(Bucket=bucket_name, EncodingType='url',MaxKeys=1000,Prefix=prefix,ContinuationToken='',FetchOwner=False,StartAfter='')
    except KeyError:
        s3_status = 'NO_OUTPUTS'
        return s3_status
    # To determine most recent log dir, list all "dirs" in the log dir and pull their LastModified value (datetime object) from their Key to compare 
    # Alternative is to pull date and time from dir name but this will be harder i think
    # documentation here: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html
    # maybe helpful example here: https://repost.aws/questions/QUFpzxAPCEQa6HqYceZ6bRIA/how-to-list-s3-directories-not-objects-by-date-added
    # When looping through Keys (after determing correct log bucket), determine status with key.startswith(crash_prefix)

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
