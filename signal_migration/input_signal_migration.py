import requests
import json
import os
import boto3
import shutil
from urllib.parse import urlparse
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




def createInputSignalConnection(acccountID, datastreamID, urlPrefix,auth):
    try:
        os.mkdir('workDir')
    except:
        shutil.rmtree('workDir')
        os.mkdir('workDir')
    headers = {"Authorization": auth}
    #API Extraction
    logging.info("Fetching Initial data")
    print(type(urlPrefix),type(acccountID),type(datastreamID))
    datastreamLink = urlPrefix+"accounts/"+acccountID+"/datastreams/"+datastreamID

    numberOfSignals = requests.get(datastreamLink+"/signals",headers=headers)
    numberOfSignalsJSON = numberOfSignals.json()
    numberOfSignals = numberOfSignalsJSON[0]['count']
    
    numberOfEntities = requests.get(datastreamLink+"/entities",headers=headers)
    numberOfEntitiesJSON = numberOfEntities.json()
    numberOfEntities = numberOfEntitiesJSON[0]['count']
    
    signalJSON = []
    for i in range(numberOfSignals):
        count = str(i)
        signalList = requests.get(datastreamLink+"/signals?limit=1&offset="+count,headers=headers)
        signalJSON.append(signalList.json())
        
    entityJSON = []
    for i in range(numberOfEntities):
        count = str(i)
        entityList = requests.get(datastreamLink+"/entities?limit=1&offset="+count,headers=headers)
        entityJSON.append(entityList.json())


    currentDatastream = requests.get(datastreamLink,headers=headers)
    currentJSON = currentDatastream.json()

    datastreamName = currentJSON['name']
    timeFormat = currentJSON.get('baseTimeUnit','millis')
    timeFormat = currentJSON.get('oldBaseTimeUnit',timeFormat)
    entityNameList = []
    flowName = f"{datastreamName}/{datastreamID}"
    logging.info("Begin Migrating model "+flowName)

    template = """ [
                {% for key, value in dataMap.items() %}
                    {% set list = contextMap.filePath.split('signal=') %} 
                    {% set list = list[1].split('/') %} 
                    
                    {% if (key =='value') %} """
                    
    dataDict = {
            "name": flowName,
            "description": "desc",
            "flowType": "CLUESOURCE",
            "spec": {
                "connectionConfig": {
                "ruleSpec": {
                    "templateRule":{
                    "timeFormat":timeFormat,
                    "template":template
                    }
                },
                "name": flowName,
                "mqtt": {"topic":"value"}
                },
                "sourceMappings": [],
                "config": {}
            }
            }
    inputDict = {}
    dataJSON = json.dumps(dataDict)
    dataJSON = json.loads(dataJSON)
    for j in range(len(entityJSON)):
        entityNameList.append(entityJSON[j][0]['name'])
        for i in range(len(signalJSON)):

            signalKey = signalJSON[i][0]['key']
            signalName= signalJSON[i][0]['name']
            signalID= signalJSON[i][0]['id']
            entityName= entityJSON[j][0]['name']
            signalValueType = signalJSON[i][0]['valueType']
            entityID= entityJSON[j][0]['id']
            
            
            if(j==0):
                template +="""
                    {% if '"""
                template += signalJSON[i][0]['key']
                template +="""' in contextMap.filePath %}
                """
                template += """    { "sourceName": "{{ dataMap.thing }}_"""
                template += signalJSON[i][0]['name']
                template += """", "time": "{{ (dataMap.time)|int }}",  "value": "{{ value }}"} 
                    {% endif %} """

    
            obj = {"name": entityName+"_"+signalName, "valueType": signalValueType, "description": entityName+"_"+signalName}
            y = json.dumps(obj)
            y = json.loads(y)
            dataJSON['spec']['sourceMappings'].append(y)
            inputDict[entityName+"_"+signalName] = {'eName':entityName,'eID':entityID,'sName':signalName,'sID':signalID}
            
    template += """
                    {% if not loop.last %}
                    ,
                    {% endif %} 
                    
                    {% endif %} 
                {% endfor %}  
                ] """
            
    dataJSON['spec']['connectionConfig']['ruleSpec']['templateRule']['template'] = template         
    dataJSON = str(dataJSON)
    print(template)
    postURL = urlPrefix+"accounts/"+acccountID+"/flows"
    logging.info("Flow Posting "+flowName)
    resp = requests.post(postURL, headers=headers, data=dataJSON)
    logging.info("Flow Posted "+flowName)
    flowJSON = resp.json()
    flowID = flowJSON['id']
    resp = requests.get(postURL+"/"+flowID, headers=headers)
    respJSON = resp.json()
    print("Waiting for completion")
    while (respJSON["status"]!="COMPLETED"):
        resp = requests.get(postURL+"/"+flowID, headers=headers)
        respJSON = resp.json()
    connectionID = (respJSON["outputs"][0]['connection'])  
    print("CONNECTION", connectionID)
    
    url = urlPrefix+"accounts/"+acccountID+"/connectedsources?connection=" + str(connectionID)
    
    resp = requests.get(url, headers=headers)
    connectedSources = resp.json()
    
    url = urlPrefix+"accounts/"+acccountID+"/connections/" + str(connectionID)
    resp = requests.get(url, headers=headers)
    connectionContextJson = resp.json()
    dest = connectionContextJson["connectionConfigs"]["file"]["dataDir"]
    logging.info("DEST "+dest)
    logging.info("Create Job")
    jobJSON = {
        "jobType": "Test",
        "spec": {
            "destinationPath": dest,
            "entities": entityNameList,
            "signals": []
        }
    }
    job = json.dumps(jobJSON)
    job = json.loads(job)
    for i in range(len(signalJSON)):

        obj = {"name":signalJSON[i][0]['name'], "valueType": signalJSON[i][0]['valueType'], "dataPath":"s3://falkonry-dev-backend/tercel/data/"+acccountID+"/"+datastreamID+"/RAWDATA/"+datastreamID+"/signal="+signalJSON[i][0]['key']+"/"}
        y = json.dumps(obj)
        y = json.loads(y)
        job['spec']['signals'].append(y)
    logging.info("Job Created " + dest)

    parquetFileConnector(acccountID,job,connectionID,auth)
    #parquetTransfromUploadInput(job)
    shutil.rmtree('workDir')
    for i in range(connectedSources[0]['count']):
        url = urlPrefix+"accounts/"+str(acccountID)+"/connectedsources?limit=1&offset="+str(i)+"&connection=" + str(connectionID)
        print()
        connectedSource = requests.get(url, headers=headers).json()
        connSourceID = connectedSource[0]['id']
        connContext = connectedSource[0]['context']
        connSourceTenant = connectedSource[0]['tenant']
        sourceName = connectedSource[0]['sourceName']
        currEntityName = inputDict[sourceName]['eName']
        currEntityID = inputDict[sourceName]['eID']
        currSignalName =inputDict[sourceName]['sName']
        currSignalID = inputDict[sourceName]['sID']
        
        payload = {"id" : f"{currEntityID}.{currSignalID}", "context" : connContext, "signalId" : currSignalID,
        "entityId" : currEntityID, "entityName" : currEntityName,
        "model" : "", "datastream" : datastreamID, "signalName" : currSignalName,
        "assessment" : "", "tenant" : connSourceTenant, "job" : "job"
        }
        payload = json.dumps(payload)
        payload = json.loads(payload)
        create_resp = requests.post(urlPrefix+"accounts/"+acccountID+"/signalcontexts", json = payload, headers=headers)
        print(f"Signal Context Payload Resp = {create_resp.status_code}")

            
def parquetFileConnector(accountID,job,connectionId,auth):
    headers = {'Authorization': auth}
    signalList = job['spec']['signals']
    s3 = boto3.resource('s3')
    logging.info("Making Event")
    url = f"https://dev.falkonry.ai:30080/api/1.2/accounts/{accountID}/inputcontexts/"
    for i in range(len(signalList)):
        o = urlparse(signalList[i]['dataPath'], allow_fragments=False)
        bucket = o.netloc
        key = o.path.lstrip('/') 
        my_bucket = s3.Bucket(bucket)
        #Iterate through all files in given datapath
        for obj in my_bucket.objects.filter(Prefix=key):
            
            file = obj.key  

            
            payload = {
                "tenant": accountID,
                "connection": connectionId,
                "inputContextType": "FILE",
                "fileDetails": {
                    "fileType": "PARQUET",
                    "filePath": f"/{bucket}/{file}",
                    "fileSize": 1000
                },
                "status": "CREATED",
                "inputContextStats": {
                    "rowsCount": 0,
                    "pointsCount": 0,
                    "bytesCount": 0,
                    "triggersCount": 0,
                    "badPointsCount": 0,
                    "emptyPointsCount": 0
                },
            }
            
            payload = str(payload)
            response = requests.post(url, headers=headers, data=payload)
            print("RESP",response.json()['id'])
            
            
            
                      
            print("FILE: "+str(file))
            new_event = {
            "file_id":response.json()['id'],
            "bucket": bucket,
            "object_key":file,
            "offset": 0,
            "connection_id": connectionId,
            "tenant_id":accountID
            }
            send_to_sqs(new_event)

def send_to_sqs(event):
    payload = json.dumps(event)
    sqs_client = boto3.client('sqs')
    queue_url = os.environ.get('QUEUE_URL')
    logging.info("Sending Event")
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        DelaySeconds=0,
        MessageBody=payload
    )
    logging.info(f"Event sent")
                 
#*Test              
acccountID1 = "849393527846985728"
datastreamID1 = "849433632037003264"
auth1 = os.environ.get("AUTH")
appUrl = os.environ.get("APP_URL")
createInputSignalConnection(acccountID1,datastreamID1, appUrl, auth1)
print("Finished")


