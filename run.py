#!/usr/bin/env python3

import argparse
import os
import subprocess
from glob import glob
from utils.bids import s3_get_bids_subjects, s3_get_bids_sessions
# from utils.get_status import s3_fmriprep_exec_sum, s3_xcpd_exec_sum, s3_fmriprep_crash_log, s3_xcpd_crash_log ## TODO: Build out these functions and incorporate
from utils.get_status import fmriprep_exec_sum,xcpd_exec_sum, fmriprep_crash_log, xcpd_crash_log
from utils.html import *
import pandas as pd
import numpy as np

#debugging
import pdb

__version__ = open(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                'version')).read()

parser = argparse.ArgumentParser(description='processing_audit entrypoint script.')
parser.add_argument('bids_dir', help='The directory with the input dataset '
                    'formatted according to the BIDS standard. In the case that the BIDS dataset is within s3 provide the path to the folder along with "s3://BUCKET_NAME/path_to_BIDS_folder".')
parser.add_argument('output_dir', help='The directory where the output files '
                    'are stored. If you are running group level analysis '
                    'this folder should be prepopulated with the results of the'
                    'participant level analysis.In the case that this folderis within s3 provide the path to the folder along with "s3://BUCKET_NAME/path_to_derivatives_folder".')
parser.add_argument('analysis_level', help='Level of the analysis that will be performed. '
                    'Unless checking on status of one participant''s processing, use "group".',
                    choices=['participant', 'group'])
parser.add_argument('--report_output_dir','--report-output-dir',required=True, help='The directory where the CSV and HTML files will be outputted once the report finishes.')
parser.add_argument('--participant_label', '--participant-label',help='The label(s) of the participant(s) that should be analyzed. The label '
                   'corresponds to sub-<participant_label> from the BIDS spec '
                   '(so it does not include "sub-"). If this parameter is not '
                   'provided all subjects should be analyzed. Multiple '
                   'participants can be specified with a space separated list.',
                   nargs="+")
parser.add_argument('--n_cpus',required=False,help='Number of CPUs to use for parallel download.',type=int)
parser.add_argument('--s3_access_key',required=False,type=str,
                        help='Your S3 access key, if data is within S3. If using MSI, this can be found at: https://www.msi.umn.edu/content/s3-credentials')
parser.add_argument('--s3_hostname',required=False,default='https://s3.msi.umn.edu',type=str,
                        help='URL for S3 storage hostname, if data is within S3 bucket. Defaults to s3.msi.umn.edu for MSIs tier 2 CEPH storage.')
parser.add_argument('--s3_secret_key',required=False,type=str,
                        help='Your S3 secret key. If using MSI, this can be found at: https://www.msi.umn.edu/content/s3-credentials')                        
parser.add_argument('--skip_bids_validator', help='Whether or not to perform BIDS dataset validation',
                   action='store_true')
parser.add_argument('--session_label', help='The label(s) of the session(s) that should be analyzed. The label '
                   'corresponds to ses-<session_label> from the BIDS spec '
                   '(so it does not include "sub-"). If this parameter is not '
                   'provided all subjects should be analyzed. Multiple '
                   'participants can be specified with a space separated list.',
                   nargs="+")
# TODO: edit version argument to expand to other pipeline versions, i.e. nibabies, fmriprep, xcp-d, etc. and whatever their current version is/what we've adapted to.
parser.add_argument('-p', '--pipeline', required=True, help="Which processing pipeline you're using. Currently supports Nibabies, fMRIprep, and XCP-D")
# parser.add_argument('-v', '--version', action='version',
#                     version='processing_audit version {}'.format(__version__))

# Parse and gather arguments
args = parser.parse_args()

def get_s3_inputs():
    bids_dir_bucket_name = args.bids_dir.split('s3://')[1].split('/')[0]
    bids_dir_relative_path = args.bids_dir.split('s3://'+bids_dir_bucket_name)[1]
    if bids_dir_relative_path == '/': 
        bids_dir_relative_path = ''
        # only for a subset of subjects at participant level
    if args.participant_label and args.analysis_level == "participant":
        subjects_to_analyze = args.participant_label
    else:
        subjects_to_analyze = s3_get_bids_subjects(bucketName=bids_dir_bucket_name, 
                            prefix=bids_dir_relative_path,
                            access_key=args.s3_access_key, 
                            secret_key=args.s3_secret_key, 
                            host=args.s3_hostname)
    assert len(subjects_to_analyze)>0, args.bids_dir + ' has no subject folders within it. Are you sure this the root to the BIDS folder?'
    if not 'sub-' in subjects_to_analyze[0]:
        subjects_to_analyze[0] = 'sub-'+subjects_to_analyze[0]

    return subjects_to_analyze

def get_local_inputs():
    if args.participant_label and args.analysis_level == "participant":
        subjects_to_analyze = args.participant_label
    else:
        subject_dirs = glob(os.path.join(args.bids_dir, "sub-*"))
        subjects_to_analyze = [os.path.basename(subject_dir) for subject_dir in subject_dirs]
    if not 'sub-' in subjects_to_analyze[0]:
        subjects_to_analyze[0] = 'sub-'+subjects_to_analyze[0]
    
    return subjects_to_analyze    

def analyze_s3_outputs(subjects_to_analyze, bids_dir_bucket_name, bids_dir_relative_path):
    columns=["sub_id","ses_id","crash_log","exec_sum"]
    session_statuses = pd.DataFrame(columns=columns)
    study_ses_count = 0
    for subject in subjects_to_analyze:
        sessions_to_analyze = s3_get_bids_sessions(bucketName=bids_dir_bucket_name, 
                            prefix=bids_dir_relative_path + subject+'/',
                            access_key=args.s3_access_key, 
                            secret_key=args.s3_secret_key, 
                            host=args.s3_hostname) # checking if sessions exist
        for session in sessions_to_analyze:
            study_ses_count = study_ses_count + 1
            session_status = pd.DataFrame(columns=columns,index=range(1))
            session_status.loc[0].sub_id = subject.split('-')[1]
            session_status.loc[0].ses_id = session.split('-')[1]
    ## TODO: follow logic of local outputs function once S3 functions have been built out

def analyze_local_outputs(subjects_to_analyze, pipeline, output_dir):
    columns=["subject","session","status","crash_log","exec_sum"]
    session_statuses = pd.DataFrame(columns=columns)
    study_ses_count = 0
    for subject in subjects_to_analyze:
        sessions_to_analyze = glob(os.path.join(args.bids_dir,subject,'ses-*')) # checking if sessions exist
        for session in sessions_to_analyze:
            study_ses_count = study_ses_count + 1
            session_status = pd.DataFrame(columns=columns,index=range(1))
            session_status.loc[0].subject = subject
            session_status.loc[0].session = os.path.split(session)[-1]
            if pipeline == "xcpd": 
                exec_sum_status = xcpd_exec_sum(output_dir)
                crash_status = xcpd_crash_log(output_dir)
            else:
                exec_sum_status = fmriprep_exec_sum(output_dir)
                crash_status = fmriprep_crash_log(output_dir)
            
            if exec_sum_status == "FOUND_EXEC-SUM" and crash_status == "NO_CRASH":
                status = "success?"
            else:
                status = "failed?"
            session_status.loc[0,'exec_sum'] = exec_sum_status
            session_status.loc[0,'crash_log'] = crash_status
            session_status.loc[0,'status'] = status
            session_statuses = pd.concat([session_statuses,session_status],ignore_index=True)
    return session_statuses

current_path=os.path.dirname(__file__)
def old_code():
    # determine if bids_dir or output_dir are S3 buckets, and their respective names if so.
    if 's3://' in args.bids_dir or 's3://' in args.output_dir:
        # set up s3 connection
        assert args.s3_access_key, print(args.bids_dir + ' or ' +  args.output_dir + ' are S3 buckets but you did not input a S3 access key following argument "--s3_access_key". If using MSI, this can be found at: https://www.msi.umn.edu/content/s3-credentials.')
        assert args.s3_secret_key, print(args.bids_dir + ' or ' +  args.output_dir + ' are S3 buckets but you did not input a S3 secret key following argument "--s3_secret_key". If using MSI, this can be found at: https://www.msi.umn.edu/content/s3-credentials.')
        # Determine bucket names
        if 's3://' in args.bids_dir:
            bids_dir_bucket_name = args.bids_dir.split('s3://')[1].split('/')[0]
            bids_dir_relative_path = args.bids_dir.split('s3://'+bids_dir_bucket_name)[1]
            if bids_dir_relative_path == '/': 
                bids_dir_relative_path = ''
        else:
            bids_dir_bucket_name = ''
        if 's3://' in args.output_dir:
            output_dir_bucket_name = args.output_dir.split('s3://')[1].split('/')[0]
            output_dir_relative_path = args.output_dir.split('s3://'+output_dir_bucket_name)[1]
            if output_dir_relative_path == '/':
                output_dir_relative_path = ''
            elif output_dir_relative_path[0] == '/':
                output_dir_relative_path = output_dir_relative_path[1:]
            if len(output_dir_relative_path) > 0 and not output_dir_relative_path[-1] == '/':
                output_dir_relative_path = output_dir_relative_path+'/'

        else:
            output_dir_bucket_name = ''
    else:
        bids_dir_bucket_name = ''
        output_dir_bucket_name = ''
    # only for a subset of subjects at participant level
    if args.participant_label and args.analysis_level == "participant":
        subjects_to_analyze = args.participant_label
    # running group level for all subject
    elif args.analysis_level == "group":
        if bids_dir_bucket_name:
            
            subjects_to_analyze = s3_get_bids_subjects(bucketName=bids_dir_bucket_name, 
                                prefix=bids_dir_relative_path,
                                access_key=args.s3_access_key, 
                                secret_key=args.s3_secret_key, 
                                host=args.s3_hostname)
        else:
            subject_dirs = glob(os.path.join(args.bids_dir, "sub-*"))
            subjects_to_analyze = [os.path.basename(subject_dir) for subject_dir in subject_dirs]
        assert len(subjects_to_analyze)>0, args.bids_dir + ' has no subject folders within it. Are you sure this the root to the BIDS folder?'
        if output_dir_bucket_name:
            output_dir_subjects_to_analyze = s3_get_bids_subjects(bucketName=output_dir_bucket_name, 
                                prefix=output_dir_relative_path,
                                access_key=args.s3_access_key, 
                                secret_key=args.s3_secret_key, 
                                host=args.s3_hostname)
        else:
            output_dir_subject_dirs = glob(os.path.join(args.output_dir, "sub-*"))
            output_dir_subjects_to_analyze = [subject_dir for subject_dir in output_dir_subject_dirs]
        assert len(output_dir_subjects_to_analyze)>0, args.bids_dir + ' has no subject folders within it. Are you sure this the root to the pipeline derivatives folder?'   
    else:
        raise Exception("You must enter participant --participant_label or group in order to run.")
    # some prelimnaries prior to looping through data
    if not 'sub-' in subjects_to_analyze[0]:
        subjects_to_analyze[0] = 'sub-'+subjects_to_analyze[0]
    if bids_dir_bucket_name:
        sessions_to_analyze = s3_get_bids_sessions(bucketName=bids_dir_bucket_name, 
                            prefix=bids_dir_relative_path + subjects_to_analyze[0]+'/',
                            access_key=args.s3_access_key, 
                            secret_key=args.s3_secret_key, 
                            host=args.s3_hostname) # checking if sessions exist
    else:
        sessions_to_analyze = glob(os.path.join(args.bids_dir,subjects_to_analyze[0],'ses-*')) # checking if sessions exist
    columns=["sub_id","ses_id","crash_log","exec_sum"]
    session_statuses = pd.DataFrame(columns=columns)
    study_ses_count = 0
    for subject in subjects_to_analyze:
        if bids_dir_bucket_name: # if bids dir is a bucket pull sessions from that subject
            sessions_to_analyze = s3_get_bids_sessions(bucketName=bids_dir_bucket_name, 
                            prefix=bids_dir_relative_path + subject+'/',
                            access_key=args.s3_access_key, 
                            secret_key=args.s3_secret_key, 
                            host=args.s3_hostname)
        else:
            session_dirs = glob(os.path.join(args.bids_dir, subject, 'ses-*' ))
            sessions_to_analyze =  [os.path.basename(session_dir) for session_dir in session_dirs]
            
        if sessions_to_analyze:
            for session in sessions_to_analyze:
                study_ses_count = study_ses_count + 1
                session_status = pd.DataFrame(columns=columns,index=range(1))
                session_status.loc[0].subj_id = subject.split('-')[1]
                session_status.loc[0].ses_id = session.split('-')[1]
                if bids_dir_bucket_name:
                    print("just so i can make this not give me a syntax error")
                #     bolds = s3_get_bids_funcs(access_key=args.s3_access_key,
                #         bucketName=bids_dir_bucket_name,
                #         secret_key=args.s3_secret_key, 
                #         host=args.s3_hostname,
                #         prefix=bids_dir_relative_path + subject+ '/' +session)
                #     struct = s3_get_bids_structs(access_key=args.s3_access_key,
                #         bucketName=bids_dir_bucket_name,
                #         secret_key=args.s3_secret_key, 
                #         host=args.s3_hostname,
                #         prefix=bids_dir_relative_path + subject+ '/' +session)
                # else:
                #     bolds = get_bids_funcs(os.path.join(args.bids_dir,subject,session))
                #     struct = get_bids_structs(os.path.join(args.bids_dir,subject,session))
                                            
                # if struct: # if structurals can be found continue, otherwise tag this as "No BIDS" 
                #     # if not output_dir_bucket_name:
                #     #     struct_status = abcd_hcp_struct_status(os.path.join(args.output_dir,subject,session))
                #     # else:
                #     #     # LOOK IN S3 FOR FINAL STRUCTURAL OUTPUT 
                #     #     # TODO: change function or add new one for other pipelines
                #     #     struct_status = s3_abcd_hcp_struct_status(bucketName=output_dir_bucket_name,
                #     #             access_key=args.s3_access_key,
                #     #             secret_key=args.s3_secret_key,
                #     #             host=args.s3_hostname,
                #     #             prefix=output_dir_relative_path +subject+ '/' +session)
                # else:
                #     struct_status = "NO BIDS"
                # session_status.loc[0,'structural'] = struct_status
                # if bolds: # if bolds can be found continue, otherwise tag this as "No BIDS"
                #     #TODO: change these functions or add new ones for other pipelines
                #     if not output_dir_bucket_name:
                #         minimal_func_status = abcd_minimal_func_hcp_status_outputs(os.path.join(args.output_dir,subject,session))
                #         DCANBoldPreProc_func_status = abcd_hcp_DCANBoldPreProc_func_status(os.path.join(args.output_dir,subject,session))
                #     else:
                #         #  LOOK IN S3 FOR FINAL FUNTIONAL OUTPUTS
                #         minimal_func_status = s3_abcd_hcp_minimal_func_status(bucketName=output_dir_bucket_name,
                #                 access_key=args.s3_access_key,
                #                 secret_key=args.s3_secret_key,
                #                 host=args.s3_hostname,
                #                 prefix=output_dir_relative_path +subject+ '/' +session)
                #         DCANBoldPreProc_func_status = s3_abcd_hcp_DCANBoldPreProc_func_status(bucketName=output_dir_bucket_name,
                #                 access_key=args.s3_access_key,
                #                 secret_key=args.s3_secret_key,
                #                 host=args.s3_hostname,
                #                 prefix=output_dir_relative_path +subject+ '/' +session)
                # else:
                #     minimal_func_status = "NO BIDS"
                #     DCANBoldPreProc_func_status = "NO BIDS"
                # # Tag funcs with status
                # print('subject:{}, session:{}'.format(subject,session))
                # #TODO: expand these expected tasks to be pipeline agnostic
                # for task in expected_tasks:
                #     minimal_proc_task = 'Minimal Preprocessing: ' + task
                #     dcan_proc_task = 'DCANBoldPreProc: ' + task
                #     if bolds:
                #         if task in bolds:
                #             session_status.loc[0,minimal_proc_task] = minimal_func_status
                #             session_status.loc[0,dcan_proc_task] = DCANBoldPreProc_func_status
                #         else:
                #             session_status.loc[0,minimal_proc_task] = minimal_func_status
                #             session_status.loc[0,dcan_proc_task] = DCANBoldPreProc_func_status
                #     else:
                #         session_status.loc[0,minimal_proc_task] = minimal_func_status
                #         session_status.loc[0,dcan_proc_task] = DCANBoldPreProc_func_status
                        
                        
                # session_statuses = session_statuses.append(session_status,ignore_index=True)
                    
        else:
            print("BIDS folders without session folders has not be implemented for subject", subject)
            continue

if 's3://' in args.bids_dir:
    subjects_to_analyze = get_s3_inputs()
else:
    subjects_to_analyze = get_local_inputs()
    
if 's3://' in args.output_dir:
    session_statuses = analyze_s3_outputs(subjects_to_analyze)
else:
    session_statuses = analyze_local_outputs(subjects_to_analyze, args.pipeline, args.output_dir)

# save output to CSV
session_statuses = session_statuses.sort_values(by=['subject','session'],ignore_index=True)
session_statuses = session_statuses.replace(np.nan, '', regex=True)
session_statuses.to_csv(os.path.join(args.report_output_dir,'s3_status_report.csv'),index=False)

# generate HTML reporter
# html_report_wf(session_statuses_df=session_statuses,report_output_dir=args.report_output_dir)

print('CSV and HTML status report files have been outputted to ' + args.report_output_dir)
        