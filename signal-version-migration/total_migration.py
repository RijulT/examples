from input_migration import createInputSignalConnection 
from output_migration import createOutputSignalConnection 
import requests
import json
import logging
from datetime import datetime, time
from pandas.io.pytables import IndexCol
import requests
import pandas as pd
import os
import boto3
from urllib.parse import urlparse
from csv import reader
import numpy as np
import sys
import os
from dotenv import load_dotenv
import logging
import json
from datetime import datetime




logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
rootLogger = logging.getLogger()
logging.basicConfig(level=logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt="%Y-%m-%dT%H:%M:%S%z")
root = logging.getLogger()
root.setLevel(logging.INFO)

for h in root.handlers:
    h.setLevel(logging.INFO)
    h.setFormatter(formatter)
    
def migrate(accountID,datastreamID,urlPrefix, auth):
    headers = {"Authorization": auth}
    
    logging.info("Starting Input Signal Mirgation")
    createInputSignalConnection(accountID, datastreamID,urlPrefix, auth)
    logging.info("Starting Output Signal Mirgation")
    createOutputSignalConnection(accountID, datastreamID,urlPrefix, auth)
    
    timeUpdateLink = urlPrefix+"accounts/"+accountID+"/datastreams/"+datastreamID+"/"
    versionUpdateLink = urlPrefix+"accounts/"+accountID+"/datastreams/"+datastreamID+"/properties/"
    timeUpdateJson = {"baseTimeUnit":"nanos"}
    timeUpdateJson = str(timeUpdateJson)
    versionUpdateJson= """ {"key":"version","value":"1.2"} """
 
    resp = requests.put(timeUpdateLink, headers=headers,data=timeUpdateJson)
    resp = requests.post(versionUpdateLink, headers=headers, data=versionUpdateJson)
    
    
acccountID1 = "849393527846985728"
datastreamID1 = "862065678411948032"
#migrate(acccountID1,datastreamID1,appUrl, auth1)


def migrateInputsFromCSV(csvFile, appUrl, auth):
    logging.warning("START")
    header = {"Authorization": auth}
    with open(csvFile, 'r') as read_obj:
        csv_reader = reader(read_obj)
        headers = next(csv_reader, None)
        try:
            tenIndex= headers.index('Tenant')
            dataIndex = headers.index('Datastream')
            
        except:
            tenIndex = 0
            dataIndex= 1

        for index, row in enumerate(csv_reader):
            accountID = row[tenIndex]
            dataID = row[dataIndex]
            
            # Check if row is valid
            accountValid = 200 == requests.get(appUrl + "accounts/" + accountID, headers=header).status_code
            dataValid = False
            if (accountValid):
                dataValid = 200 == requests.get(
                    appUrl + "accounts/" + accountID + "/datastreams/" + dataID,
                    headers=header).status_code
                
            if (dataValid):
                logging.info("Starting Migration Process")
                migrate(accountID,dataID, appUrl, auth)
            else:
                print("An exception occurred in CSV Line " + str(row))
            logging.warning("END")
load_dotenv()
auth1 = os.environ.get('AUTH')
appUrl = os.environ.get('APP_URL')
file = sys.argv[1]
migrateInputsFromCSV(file,appUrl,auth1)
