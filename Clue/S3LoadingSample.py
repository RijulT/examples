'''
    Example of reading a data file and loading it into S3 bucket
    Files are processed from a "queue" directory and after processing
    moved to a "processed" directory
    Version: 1.6
    Date: 2021-04-22

    Version History:
    Version     Description                     Author
    ---------------------------------------------------
    1.0         Initial Release                 MB
    1.1         Ready for Task Manager          MB
    1.3         Sorted files in mod time asc    MB
    1.4         Added delay between files       MB
    1.5         Changed put method to set ACL   MB
                Changes to List method (using v2) to
                    enable more than 1000 files
    1.6         Adds method of handling various  MB
                time formats see
                to_falkonry_datetime_str
'''

import pandas as pd # used to facilitate reading metadata and file contents
import logging
import logging.handlers
import io
import boto3
import os
import time as tm
import shutil
from enum import Enum
from datetime import *
import pytz

import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)

def configure_logging(logroot,source=__file__,format='%(asctime)s:%(levelname)s:%(message)s',daystokeep=7,level=logging.INFO):
    name=os.path.basename(source)
    logger=logging.getLogger(name)
    logger.setLevel(level)
    logfile=name[:-3]
    formatter=logging.Formatter(format)
    if(not(os.path.exists(logroot))):
        os.makedirs(logroot)
    fileHandler=logging.handlers.TimedRotatingFileHandler("{0}/{1}.log".format(logroot,logfile),when='D',backupCount=10,interval=daystokeep)
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)
    consoleHandler=logging.StreamHandler()
    consoleHandler.setFormatter(formatter)
    logger.addHandler(consoleHandler)
    return logger

# sort files in dir according to modification time
def get_time_sorted_files(dir,extension=None):
    fts_sorted=[]
    for _,_,files in os.walk(dir):
        fts={}
        for file in files:
            if(extension and not(file.endswith(extension))):
                continue
            fts[file]=os.path.getmtime(os.path.join(dir,file))
        fts_sorted=sorted(fts,key=lambda key: fts[key])
        break # only process this directory
    return fts_sorted

class TimeFormats(Enum):
    ISO='iso-8601'
    MICROS='micros'
    NANOS='nanos'
    MILLIS='millis'
    SECS='seconds'

def to_falkonry_datetime_str(dt:datetime,format=TimeFormats.ISO):
    # if not timezone aware raise exception
    if(dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None):
        raise ValueError("dt must be timezone aware")
    # convert to utc
    utc_dt=dt.astimezone(timezone.utc)
    # Falkonry only accepts 6 figures (microsecs)
    if(format==TimeFormats.SECS):
        return str(round(utc_dt.timestamp()))
    elif(format==TimeFormats.MILLIS):
        return str(round(utc_dt.timestamp()*1000))
    elif(format==TimeFormats.MICROS):
        return str(round(utc_dt.timestamp()*1e6))
    elif(format==TimeFormats.NANOS):
        return str(round(utc_dt.timestamp()*1e9))
    else:
        # Falkonry only accepts 6 figures (microsecs)
        return datetime.strftime(utc_dt,"%Y-%m-%dT%H:%M:%S.%fZ")

def remove_nas(df,inplace=True):
    df_ret = df
    if(not(inplace)):
        df_ret=pd.DataFrame(df)
    df_ret = df_ret.replace(r'^\s*$', '', regex=True) # eliminate spaces
    df_ret = df_ret.applymap(lambda s: '' if str(s).lower() in ['na','n/a','nan','none','null'] else s)
    return df_ret

out_bucket="falkonry-customer-uploads"

# TODO:  Data files are expected to be .csv time in wide format.
# The column names are expected to be those in the customer tags defined in the Site Survey
# plus a timestamp column (any name is fine)
#
# Files are placed in a directory and
#  Example file contents:
# timestamp, tag1, tag2, tag3
# 2021-01-01 12:00:01.500 PM, 12.34, STARTING_MODE, 1
# ....
# 2021-05-01 09:00:02.0000 AM, 4.5, STOPPING_MODE, 0

# TODO: ADJUST ACCORDING TO NEEDS.  IF FILES ARE VERY LARGE INCREASE DELAY IN BETWEEN
poll=1 # minutes to check again for arrival of new files to the queue
delay=60 # delay between sending each file.  Allows S3 events to not overlap.

# TODO:  SPECIFY LOCATION OF LOGS, FILES TO PROCESS,
log_root="<PATH TO WHERE TO STORE LOG FILES>" # location to store log files
queue_dir = "<PATH TO QUEUE WERE FILES ARE GOING TO BE PLACED FOR LOADING>"  # Location of files to move
processed_dir = "<PATH TO STORE PROCESSED FILES>"  # Location of files sucessfully processed

logger=configure_logging(log_root)

# TODO: SPECIFY TIME COLUMN FORMAT IN FILES
tscol="<NAME OF COLUMN IN DATA FILES THAT HOLDS TIMESTAMPS>" # Column in the soure file that contains the time stamp
tsformat="<FORMAT STRING OF WHAT IS IN THE FILE>" # Format in which the timestamp string is stored in the file
                                                # see  https://www.cplusplus.com/reference/ctime/strftime/
desired_format=TimeFormats.ISO # desired format to send to Falkonry - MUST match defined in Clue Data Source
tzone= "<TIMEZONE STRING FOR THE TIMESTAMPS IN THE FILE>" # Time zone name in which the date was stored please see this for list of supported names
                                                # see https://gist.github.com/heyalexej/8bf688fd67d7199be4a1682b3eec7568

pytzone=pytz.timezone(tzone)

# TODO: SPECIFY ACCOUNT SPECIFIC INFORMATION
account_id= "<YOUR ACCOUNT ID FROM FALKONRY APP>"
profile_name="<PROFILE NAME IN credentials file>" # this points to a set of credentials stored in .aws credentials file in the host
                            # see https://docs.aws.amazon.com/sdk-for-php/v3/developer-guide/guide_credentials_profiles.html
prefix="<ANY PREFIX TO ISOLATE FILES OR DIFFERENT FORMATS AND HANDLING>" # This is used to differentiate files that need different processing (example, different formats, equipment)

# TODO: MODIFIY ACCORDING TO YOUR SITE SURVEY CUSTOMER TAGS SHEET
site_survey_file= "<PATH TO YOUR SITE SURVEY EXCEL FILE>"
tags_df=pd.read_excel(site_survey_file,sheet_name="4.Customer Tags")

# Falkonry will not ignore additional columns that may exit in the source data files
# This uses the site survey to only process those columns
cols_to_send=tags_df['Signal Tag Name'].unique().tolist()

# TODO: COMMENT NEXT LINE IF RUNNING WITH TASK MANAGER AND OUTDENT TRY,CATCH BLOCKS
# while(True):
try:
    # Open connection to s3
    session = boto3.Session(profile_name=profile_name)
    s3_resource = session.resource('s3')  # low level client
    s3_client = session.client('s3')  # high level client
    logger.info("session opened with profile name {0}".format(profile_name))
    files=get_time_sorted_files(queue_dir)
    if(len(files)==0):
        logger.info("No files to process at this time in the {0} directory".format(queue_dir))
    for in_file in files:
        logger.info("reading file {0}".format(in_file))
        source_file=os.path.join(queue_dir, in_file)
        df=pd.read_csv(source_file,dtype={tscol:str}) # do not parse timestamp to datetime

        # exclude tags not needed
        df=df[cols_to_send]

        # Correct datetime format before sending
        df[tscol]=pd.to_datetime(df[tscol])
        df[tscol]=df[tscol].apply(lambda c: to_falkonry_datetime_str(c.tz_localize(pytzone), desired_format))

        # Correct nans,n/a,none,np.nan with emtpy
        df=remove_nas(df)

        # Send file
        pfx="{0}/{1}".format(account_id,prefix)
        out_file="{0}/{1}".format(pfx,in_file)
        s=io.StringIO()
        logging.info("writing dataframe to text stream")
        df.to_csv(s,index=False)  # write pandas frame to stream
        logger.info("sending file {0} to bucket {1}".format(out_file,out_bucket))
        s3_resource.Object(out_bucket, out_file).put(Body=s.getvalue(),ACL="bucket-owner-full-control")

        # Check if it worked
        size = 0
        saved = False
        items = s3_client.list_objects_v2(Bucket=out_bucket, Prefix=pfx)
        items_list = []
        items_list.extend(items['Contents'])
        while (items['IsTruncated']):
            next_key = items['NextContinuationToken']
            items = s3_client.list_objects_v2(Bucket=out_bucket, Prefix=pfx, ContinuationToken=next_key)
            items_list.extend(items['Contents'])
        for item in items_list:
            if(item['Key']==out_file):
                saved=True
                size=item['Size']
                break
        if(saved):
            if(size>0):
                logger.info("file {1} saved to bucket {0} with size={2} bytes".format(out_bucket,source_file,size))
                shutil.move(source_file,os.path.join(processed_dir,in_file))
            else:
                logger.error("file {1} saved to bucket {0} is empty".format(out_bucket,out_file))
        else:
            logger.error("failed to save file {0} in bucket {1}".format(source_file,out_bucket))
        # sleep between sending each file
        tm.sleep(delay)
except Exception as e:
    logger.exception(e)
    # TODO: COMMENT NEXT TWO LINES IF RUNNING WITH TASK MANAGER
    #logger.info("Sleeping for {0} minutes".format(poll))
    #tm.sleep(60*poll)

