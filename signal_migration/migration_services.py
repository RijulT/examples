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
    datastreamLink = urlPrefix+"accounts/"+acccountID+"/datastreams/"+datastreamID

    numberOfSignals = requests.get(datastreamLink+"/signals",headers=headers)
    numberOfSignalsJSON = numberOfSignals.json()
    numberOfSignals = numberOfSignalsJSON[0]['count']
    
    numberOfEntities = requests.get(datastreamLink+"/entities",headers=headers)
    numberOfEntitiesJSON = numberOfEntities.json()
    numberOfEntities = numberOfEntitiesJSON[0]['count']
    
    signalJSON = []
    for i in range(int(numberOfSignals/500)+1):
        count = str(i*500)
        signalList = requests.get(datastreamLink+"/signals?limit=500&offset="+count,headers=headers).json()
        signalJSON.extend(signalList)
                
    entityJSON = []
    for i in range(int(numberOfEntities/500+1)):
        count = str(i*500)
        entityList = requests.get(datastreamLink+"/entities?limit=500&offset="+count,headers=headers)
        entityJSON.extend(entityList.json())


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
        print(entityJSON[j])
        entityNameList.append(entityJSON[j]['name'])
        for i in range(len(signalJSON)):

            signalKey = signalJSON[i]['key']
            signalName= signalJSON[i]['name']
            signalID= signalJSON[i]['id']
            entityName= entityJSON[j]['name']
            signalValueType = signalJSON[i]['valueType']
            entityID= entityJSON[j]['id']
            
            
            if(j==0):
                template +="""
                    {% if '"""
                template += signalJSON[i]['key']
                template +="""' in contextMap.filePath %}
                """
                template += """    { "sourceName": "{{ dataMap.thing }}_"""
                template += signalJSON[i]['name']
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

        obj = {"name":signalJSON[i]['name'], "valueType": signalJSON[i]['valueType'], "dataPath":"s3://falkonry-dev-backend/tercel/data/"+acccountID+"/"+datastreamID+"/RAWDATA/"+datastreamID+"/signal="+signalJSON[i]['key']+"/"}
        y = json.dumps(obj)
        y = json.loads(y)
        job['spec']['signals'].append(y)
    logging.info("Job Created " + dest)

    parquetFileConnector(acccountID,job,connectionID,auth)
    shutil.rmtree('workDir')
    connectedSourcesList = []
    for i in range(int(connectedSources[0]['count']/500+1)):
        url = urlPrefix+"accounts/"+str(acccountID)+"/connectedsources?limit=500&offset="+str(i*500)+"&connection=" + str(connectionID)
        connectedSource = requests.get(url, headers=headers).json()
        connectedSourcesList.extend(connectedSource)
    for i in range(connectedSources[0]['count']):
        connSourceID = connectedSourcesList[i]['id']
        connContext = connectedSourcesList[i]['context']
        connSourceTenant = connectedSourcesList[i]['tenant']
        sourceName = connectedSourcesList[i]['sourceName']
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
    #Get all entities in datastream
    for i in range(int(numberOfEntities/500+1)):
        count = str(i*500)
        entityListJSON = requests.get(datastreamLink+"/entities?limit=500&offset="+count,headers=headers)
        allEntityList.extend(entityListJSON.json())
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
        for a in range (int(numberOfModels/500+1)):
            url = f"{assessmentLink}/{str(assessmentIdList[i])}/models?limit=500&offset={str(a*500)}"
            modelList.extend(requests.get(url,headers=headers).json())
        #Iterate through all models in assesment
        for a in range(numberOfModels):
            logging.info("Model Number "+ str(a)+" in Assesment-" +str(i))
            #Get job ID from model and get Job object from API
            jobID = modelList[a]['job']
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
            flowName = f"{datastreamName}/{modelList[a]['name']}:{datastreamID}/{modelList[a]['id']}"
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
                "signals": []
                }
            }  
            jobN = json.dumps(jobJSON)
            jobN = json.loads(jobN)
            for j in range(len(allEntityList)):
                entityName = allEntityList[j]['name']
                
                obj = {"name": "Prediction_"+entityName, "valueType": "Categorical", "description": "Prediction"+entityName}
                y = json.dumps(obj)
                y = json.loads(y)
                print(allEntityList, type(allEntityList))
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
                    
            obj = {"name":"Prediction", "valueType": "categorical", "dataPath":"s3://falkonry-dev-backend/tercel/data/"+acccountID+"/"+datastreamID+"/"+assessmentIdList[i]+"/OUTPUTDATA/"+modelList[a]['id']+'/'}
            y = json.dumps(obj)
            y = json.loads(y)
            jobN['spec']['signals'].append(y)
            obj = {"name": "Confidence", "valueType": "Numeric", "dataPath":"s3://falkonry-dev-backend/tercel/data/"+acccountID+"/"+datastreamID+"/"+assessmentIdList[i]+"/CONFIDENCEDATA/"+modelList[a]['id']+'/'}
            y = json.dumps(obj)
            y = json.loads(y)
            jobN['spec']['signals'].append(y)
            obj = {"name":"Explanation", "valueType": signalValueType, "dataPath":"s3://falkonry-dev-backend/tercel/data/"+acccountID+"/"+datastreamID+"/"+assessmentIdList[i]+"/EXPLANATIONDATA/"+modelList[a]['id']+'/'}
            y = json.dumps(obj)
            y = json.loads(y)
            jobN['spec']['signals'].append(y)
                   
                    
            #Post Flow 
            dataJSON = str(dataJSON)
            postURL = urlPrefix+"accounts/"+acccountID+"/flows"
            logging.info("Flow Posting "+flowName)

            resp = requests.post(postURL, headers=headers, data=dataJSON)

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
            logging.info("MODELID "+ modelList[a]['id'])
            #Transform and Upload Parquet files
            #parquetTransfromUploadOutput(jobN)
            parquetFileConnector(acccountID, jobN,connectionID,auth)
            
            
            url = urlPrefix+"accounts/"+acccountID+"/connectedsources?connection=" + str(connectionID)
            resp = requests.get(url, headers=headers)
            connectedSources = resp.json()
            connectedSourcesList = []
            for b in range(int(connectedSources[0]['count']/500+1)):
                url = urlPrefix+"accounts/"+str(acccountID)+"/connectedsources?limit=500&offset="+str(i*500)+"&connection=" + str(connectionID)
                connectedSource = requests.get(url, headers=headers).json()
                connectedSourcesList.extend(connectedSource)
            for b in range(connectedSources[0]['count']):
                connSourceID = connectedSourcesList[b]['id']
                connContext = connectedSourcesList[b]['context']
                connSourceTenant = connectedSourcesList[b]['tenant']
                sourceName = connectedSourcesList[b]['sourceName']
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

                create_resp = requests.post(urlPrefix+"accounts/"+acccountID+"/signalcontexts", json = payload, headers=headers)

                logging.info("Created Signal Context")
    shutil.rmtree('workDir')
  
        
        
        
    
            
def parquetFileConnector(accountID,job,connectionId,auth):
    headers = {'Authorization': auth}
    signalList = job['spec']['signals']
    s3 = boto3.resource('s3')
    logging.info("Making Event")
    url = f"{appUrl}accounts/{accountID}/inputcontexts/"
    for i in range(len(signalList)):
        o = urlparse(signalList[i]['dataPath'], allow_fragments=False)
        bucket = o.netloc
        key = o.path.lstrip('/') 
        my_bucket = s3.Bucket(bucket)
        #Iterate through all files in given datapath
        for obj in my_bucket.objects.filter(Prefix=key):
            
            file = obj.key  
            s3_object = s3.Object(bucket_name=bucket, key=file)
            
            payload = {
                "tenant": accountID,
                "connection": connectionId,
                "inputContextType": "FILE",
                "fileDetails": {
                    "fileType": "PARQUET",
                    "filePath": f"/{bucket}/{file}",
                    "fileSize": s3_object.content_length
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
                 



