import requests
import json
import pandas as pd
import os
import boto3
import shutil
from urllib.parse import urlparse
import logging
import time

# logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
# rootLogger = logging.getLogger()
# logging.basicConfig(level=logging.INFO)
# formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt="%Y-%m-%dT%H:%M:%S%z")
# root = logging.getLogger()
# root.setLevel(logging.INFO)

# for h in root.handlers:
#     h.setLevel(logging.INFO)
#     h.setFormatter(formatter)
    
def createOutputSignalConnection(acccountID, datastreamID, urlPrefix,auth):
    try:
        os.mkdir('workDir')
    except:
        shutil.rmtree('workDir')
        os.mkdir('workDir')
    logging.info("Fetching Initial data")
    datastreamLink = urlPrefix+"accounts/"+acccountID+"/datastreams/"+datastreamID
    headers = {"Authorization": auth}
    datastreamName = requests.get(datastreamLink+"/",headers=headers).json()['name']
    assessmentLink = datastreamLink+"/assessments"
    assessmentCount = requests.get(assessmentLink+'/',headers=headers).json()[0]['count']
    assessmentIdList = []
    
    numberOfEntities = requests.get(datastreamLink+"/entities",headers=headers)
    numberOfEntitiesJSON = numberOfEntities.json()
    numberOfEntities = numberOfEntitiesJSON[0]['count']
    allEntityList = []
    entityList = []
    #Get all entities in datastream
    for i in range(numberOfEntities):
        count = str(i)
        entityListJSON = requests.get(datastreamLink+"/entities?limit=1&offset="+count,headers=headers)
        allEntityList.append(entityListJSON.json()[0])
        entityList.append(entityListJSON.json()[0]['name'])
    #Get all assessment in datastream
    for i in range(assessmentCount):
        url = f"{assessmentLink}?limit=1&offset={str(i)}"
        assessmentIdList.append(requests.get(url,headers=headers).json()[0]['id'])
    #Iterate through all assessment 

    for i in range(len(assessmentIdList)):
        logging.info("Assesment Number "+ str(i))

        #Get all models in assesment
        url = f"{assessmentLink}/{str(assessmentIdList[i])}/models"
        numberOfModels = requests.get(url,headers=headers).json()[0]['count']
        modelList = []
        for a in range (numberOfModels):
            url = f"{assessmentLink}/{str(assessmentIdList[i])}/models?limit=1&offset={str(a)}"
            modelList.append(requests.get(url,headers=headers).json())
        #Iterate through all models in assesment
        for a in range(numberOfModels):
            logging.info("Model Number "+ str(a)+" in Assesment-" +str(i))
            #Get job ID from model and get Job object from API
            jobID = modelList[a][0]['job']
            url = f"{urlPrefix}accounts/{acccountID}/jobs/{jobID}"
            job = requests.get(url,headers=headers).json()

            #Signal Object list contains all input signals used in the model as json objects
            
            template = """ [
                    {% for key, value in dataMap.items() %}
                        {% if (key =='value' and 'OUTPUTDATA' in contextMap.filePath) %}
                        {"sourceName": "Prediction_{{dataMap.thing}}", "time": "{{ (dataMap.time)|int }}",  "value": "{{value}}"  }
                        {% endif %}
                        
                        {% if (key =='value' and 'CONFIDENCEDATA' in contextMap.filePath) %}
                        { "sourceName": "Confidence_{{dataMap.thing}}", "time": "{{ (dataMap.time)|int }}",  "value": "{{ value}}"  }
                        {% endif %}
                        
                        {% if (key =='score' and 'EXPLANATIONDATA' in contextMap.filePath) %}
                        { "sourceName": "{{dataMap.thing}}-Explanation-{{dataMap.signalId}}", "time": "{{ (dataMap.time)|int }}",  "value": "{{value}}"  }
                        {% endif %} 
                        
                        
                    {% endfor %}  
                ] """
            
            signalObjList = job['spec']['inputList']
            timeFormat = job.get('baseTimeUnit','millis')
            timeFormat = job.get('oldBaseTimeUnit',timeFormat)
            inputDict = {}
            flowName = f"{datastreamName}/{modelList[a][0]['name']}:{datastreamID}/{modelList[a][0]['id']}"
            logging.info("Begin Migrating model "+flowName)
            #Create Flow
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
            #Create Job for each inputsignal/entity combination
            dataJSON = json.dumps(dataDict)
            dataJSON = json.loads(dataJSON)

            #Create Job using all entities in datastream and ouputsignals in spec
            jobJSON = {
            "jobType": "Test",
            "spec": {
                "destinationPath": "",
                "entities": entityList,
                "signals": []
                }
            }  
            jobN = json.dumps(jobJSON)
            jobN = json.loads(jobN)
            for j in range(len(entityList)):
                entityName = entityList[j]
                
                obj = {"name": "Prediction_"+entityName, "valueType": "Categorical", "description": "Prediction"+entityName}
                y = json.dumps(obj)
                y = json.loads(y)
                inputDict["Prediction_"+entityName] = {'eName':entityName,'eID':allEntityList[j]['id'],'sName':"",'sID':""}
                dataJSON['spec']['sourceMappings'].append(y)
                
                obj = {"name": "Confidence_"+entityName, "valueType": "Numeric", "description": "Confidence"+entityName}
                y = json.dumps(obj)
                y = json.loads(y)
                inputDict["Confidence_"+entityName] = {'eName':entityName,'eID':allEntityList[j]['id'],'sName':"",'sID':""}
                dataJSON['spec']['sourceMappings'].append(y)

                for l in range(len(signalObjList)):
                    signalName = signalObjList[l]['key']
                    signalValueType = signalObjList[l]['valueType']
                
                    obj = {"name": entityName+"-Explanation-"+signalName, "valueType": signalValueType, "description": entityName+"_"+signalName}
                    y = json.dumps(obj)
                    y = json.loads(y)
                    inputDict[entityName+"-Explanation-"+signalName] = {'eName':entityName,'eID':allEntityList[j]['id'],'sName':signalObjList[l]['name'],'sID':signalObjList[l]['id']}
                    dataJSON['spec']['sourceMappings'].append(y)
                    
            obj = {"name":"Prediction", "valueType": "categorical", "dataPath":"s3://falkonry-dev-backend/tercel/data/"+acccountID+"/"+datastreamID+"/"+assessmentIdList[i]+"/OUTPUTDATA/"+modelList[a][0]['id']+'/'}
            y = json.dumps(obj)
            y = json.loads(y)
            jobN['spec']['signals'].append(y)
            obj = {"name": "Confidence", "valueType": "Numeric", "dataPath":"s3://falkonry-dev-backend/tercel/data/"+acccountID+"/"+datastreamID+"/"+assessmentIdList[i]+"/CONFIDENCEDATA/"+modelList[a][0]['id']+'/'}
            y = json.dumps(obj)
            y = json.loads(y)
            jobN['spec']['signals'].append(y)
            obj = {"name":"Explanation", "valueType": signalValueType, "dataPath":"s3://falkonry-dev-backend/tercel/data/"+acccountID+"/"+datastreamID+"/"+assessmentIdList[i]+"/EXPLANATIONDATA/"+modelList[a][0]['id']+'/'}
            y = json.dumps(obj)
            y = json.loads(y)
            jobN['spec']['signals'].append(y)
                   
                    
            #Post Flow 
            dataJSON = str(dataJSON)
            postURL = urlPrefix+"accounts/"+acccountID+"/flows"
            logging.info("Flow Posting "+flowName)
            page = ''
            while page == '':
                try:
                    resp = requests.post(postURL, headers=headers, data=dataJSON)
                    break
                except:
                    logging.info("Connection refused by the server..")
                    time.sleep(5)
                    continue
            logging.info("Flow Posted "+flowName)
            flowJSON = resp.json()
            flowID = flowJSON['id']
            resp = requests.get(postURL+"/"+flowID, headers=headers)
            respJSON = resp.json()
            #Wait for flow completion
            logging.info("Check For Compleetion")
            while (respJSON["status"]!="COMPLETED"):
                resp = requests.get(postURL+"/"+flowID, headers=headers)
                respJSON = resp.json()
            #Using ConnectionID from completed flow, get connection from API to get Destination Path
            connectionID = (respJSON["outputs"][0]['connection'])
            logging.info("Completed")
            
            url = urlPrefix+"accounts/"+acccountID+"/connections/" + str(connectionID)
            resp = requests.get(url, headers=headers)
            connectionContextJson = resp.json()
            dest = connectionContextJson["connectionConfigs"]["file"]["dataDir"]
            logging.info("DEST "+dest)
            logging.info("Create Job")
            
            jobN['spec']['destinationPath'] = dest
            logging.info("Job Created")
            logging.info("MODELID "+ modelList[a][0]['id'])
            #Transform and Upload Parquet files
            #parquetTransfromUploadOutput(jobN)
            parquetFileConnector(acccountID, jobN,connectionID,auth)
            
            
            url = urlPrefix+"accounts/"+acccountID+"/connectedsources?connection=" + str(connectionID)
            resp = requests.get(url, headers=headers)
            connectedSources = resp.json()
            for b in range(connectedSources[0]['count']):
                logging.info("create Payload")
                url = urlPrefix+"accounts/"+str(acccountID)+"/connectedsources?limit=1&offset="+str(i)+"&connection=" + str(connectionID)
                connectedSource = requests.get(url, headers=headers).json()
                connSourceID = connectedSource[0]['id']
                connContext = connectedSource[0]['context']
                connSourceTenant = connectedSource[0]['tenant']
                sourceName = connectedSource[0]['sourceName']
                currEntityName = inputDict[sourceName]['eName']
                currEntityID = inputDict[sourceName]['eID']
                currSignalName = inputDict[sourceName]['sName']
                currSignalID = inputDict[sourceName]['sID']
                
                #add datastream
                payload = {"id" : connSourceID, "context" : connContext, "signalId" : currSignalID,
                "entityId" : currEntityID, "entityName" : currEntityName,
                "model" : "", "datastream" : "", "signalName" : currSignalName,
                "assessment" : "", "tenant" : connSourceTenant, "job" : "job"
                }
                payload = json.dumps(payload)
                payload = json.loads(payload)
                page = ''
                while page == '':
                    try:
                        create_resp = requests.post(urlPrefix+"accounts/"+acccountID+"/signalcontexts", json = payload, headers=headers)
                        break
                    except:
                        print("Connection refused by the server..")
                        time.sleep(5)
                        continue
                logging.info("Created Signal Context")
    shutil.rmtree('workDir')
                
                
def parquetTransfromUploadOutput(job):
    logging.info("Begin transformation/upload of job "+job['spec']['destinationPath'])
    entityList = job['spec']["entities"]
    signalList = job['spec']['signals']
    count=0
    
    for i in range(3):
        name = signalList[i]['name'] 
        o = urlparse(signalList[i]['dataPath'], allow_fragments=False)
        bucket = o.netloc
        key = o.path.lstrip('/') 

        
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket(bucket)
        
        destinationPath = job['spec']['destinationPath']
        print(destinationPath)
        
        #Traverse through all files in the specific datapath
        for obj in my_bucket.objects.filter(Prefix=key):
            file = obj.key
            my_bucket.download_file(file, "workDir/"+os.path.basename(file))
            pfile = os.path.abspath("workDir/"+os.path.basename(file))
    
            
            #Upload to destination
            o = urlparse(destinationPath, allow_fragments=False)
            bucket = o.netloc
            key = o.path.lstrip('/')+'/'

            count+=1
            s3.Bucket(bucket).upload_file(pfile, key+name+"-|-"+str(count)+".snappy.parquet")
    
    
    
    
    
    
    
    
def parquetFileConnector(accountID,job,connectionId,auth):
    headers = {
    'Authorization': auth,
    'Content-Type': 'application/json'
        }
    appUrl = os.environ.get("APP_URL")
    filecontextsLink = f"{appUrl}accounts/{accountID}/inputcontexts"
    signalList = job['spec']['signals']
    s3 = boto3.resource('s3')
    logging.info("Create Event")
    for i in range(len(signalList)):
        o = urlparse(signalList[i]['dataPath'], allow_fragments=False)
        bucket = o.netloc
        key = o.path.lstrip('/') 
        my_bucket = s3.Bucket(bucket)
        #Iterate through all files in given datapath
        for obj in my_bucket.objects.filter(Prefix=key):
            
            file = obj.key
     

            new_event = {
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
    #urlObj = sqs_client.get_queue_url(QueueName=queueName)
    #queue_url = urlObj['QueueUrl']
    queue_url = os.environ.get('QUEUE_URL')
    logging.info("Send Event")

    response = sqs_client.send_message(
        QueueUrl=queue_url,
        DelaySeconds=0,
        MessageBody=payload
    )
    logging.info("Event Sent")
    

    
    
    
    
acccountID1 = "849393527846985728"
datastreamID1 = "849433632037003264"
auth1 = os.environ.get('AUTH')
appUrl = os.environ.get('APP_URL')
createOutputSignalConnection(acccountID1,datastreamID1, appUrl, auth1)
