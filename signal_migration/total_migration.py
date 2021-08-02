from input_migration import createInputSignalConnection 
from output_migration import createOutputSignalConnection 
import requests
import logging
import requests
import os
from csv import reader
import sys
import os
from dotenv import load_dotenv
import logging




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



def migrateInputsFromCSV(csvFile, appUrl,getAll, auth):
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
            accountValid = ((200 == requests.get(appUrl + "accounts/" + accountID, headers=header).status_code) and (accountID!=""))
            if(not getAll):
                dataValid = False
                if (accountValid and dataID!=""):
                    dataValid = 200 == requests.get(
                        appUrl + "accounts/" + accountID + "/datastreams/" + dataID,
                        headers=header).status_code
                    print(dataID)
                print(accountValid,dataValid )  
                if (dataValid):
                    logging.info("Starting Migration Process"+ str(row))
                    migrate(accountID,dataID, appUrl, auth)
                else:
                    print("An exception occurred in CSV Line " + str(row))
            elif(getAll and accountValid):
                url = appUrl+"accounts/"+accountID+"/datastreams"
                number_of_datastreams = requests.get(url,headers=header).json()[0]['count']
                for i in range(number_of_datastreams):
                    newUrl = url + f"?offset={str(i)}&limit=1"
                    datastreamID = requests.get(newUrl,headers=header).json()[0]['id']
                    migrate(accountID,datastreamID, appUrl, auth)
            else:
                    print("An exception occurred in CSV Line " + str(row))
        logging.warning("END")
        
        
load_dotenv()
auth1 = os.environ.get('AUTH')
appUrl = os.environ.get('APP_URL')
print(appUrl)
file = sys.argv[1]
try:
    getAllDatastreams = sys.argv[2] == 't'
except:
    getAllDatastreams = False
print(getAllDatastreams)
migrateInputsFromCSV(file,appUrl,getAllDatastreams,auth1)
#migrate(acccountID1,datastreamID1,appUrl, auth1)