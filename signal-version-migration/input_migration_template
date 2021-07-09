import requests
import csv
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
    
def parquetTransfromUploadInput(job):
    #Create count to ensure unique file naming for upload
    logging.info("Begin transformation/upload of job "+job['spec']['destinationPath'])
    count = 0
    #Get list of usignals and entities from job object
    entityList = job['spec']["entities"]
    signalList = job['spec']['signals']
    
    #Traverse through each signal in job object
    for i in range(len(signalList)):
        #Get signal specific datapath from job jobject
        o = urlparse(signalList[i]['dataPath'], allow_fragments=False)
        bucket = o.netloc
        key = o.path.lstrip('/') 

       
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket(bucket)
        #Iterate through all files in given datapath
        for obj in my_bucket.objects.filter(Prefix=key):
            file = obj.key
            my_bucket.download_file(file, "workDir/"+os.path.basename(file))
            logging.info("Downloaded "+file )
            pfile = os.path.abspath("workDir/"+os.path.basename(file))
        

            destinationPath = job['spec']['destinationPath']
            #     #Upload to destination
            o = urlparse(destinationPath, allow_fragments=False)
            bucket = o.netloc
            key = o.path.lstrip('/')+'/'

            #     a = (filepath)
            #     logging.info("Uploaded "+os.path.basename(filepath))
            print(key)
            s3.Bucket(bucket).upload_file(pfile, key+ signalList[i]['name']+"-\-"+str(count)+".snappy.parquet")
            count+=1
    logging.info("Done transforming and uploading job "+ job['spec']['destinationPath'])
            
            


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
    print(numberOfSignalsJSON)
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
    entityNameList = []
    #Write to CSV
    flowName = f"{datastreamName}/{datastreamID}"
    logging.info("Begin Migrating model "+flowName)
    template = """ [
                {% for key, value in dataMap.items() %}
                    {% set list = contextMap.fileName.split('-\-') %} 
                    {% if (key =='value') %}
                    { "sourceName": "{{ dataMap.thing }}_{{list[0]}}", "time": "{{ dataMap.time }}",  "value": "{{ value }}"  }
                    {% if not loop.last %}
                    ,
                    {% endif %} 
                    
                    {% endif %} 
                {% endfor %}  
                ] """
            
    dataDict = {
            "name": flowName,
            "description": "string2",
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
            print(signalKey)
            entityID= entityJSON[j][0]['id']

    
            obj = {"name": entityName+"_"+signalName, "valueType": signalValueType, "description": entityName+"_"+signalName}
            y = json.dumps(obj)
            y = json.loads(y)
            dataJSON['spec']['sourceMappings'].append(y)
            inputDict[entityName+"_"+signalName] = {'eName':entityName,'eID':entityID,'sName':signalName,'sID':signalID}
            
            
            
                    
    dataJSON = str(dataJSON)
    postURL = urlPrefix+"accounts/"+acccountID+"/flows"
    logging.info("Flow Posted "+flowName)
    resp = requests.post(postURL, headers=headers, data=dataJSON)
    print(resp.json())
    flowJSON = resp.json()
    flowID = flowJSON['id']
    print(postURL+"/"+flowID)
    resp = requests.get(postURL+"/"+flowID, headers=headers)
    respJSON = resp.json()
    while (respJSON["status"]!="COMPLETED"):
        resp = requests.get(postURL+"/"+flowID, headers=headers)
        respJSON = resp.json()
    connectionID = (respJSON["outputs"][0]['connection'])  
    
    
    url = urlPrefix+"accounts/"+acccountID+"/connectedsources?connection=" + str(connectionID)
    
    resp = requests.get(url, headers=headers)
    connectedSources = resp.json()
    
    url = urlPrefix+"accounts/"+acccountID+"/connections/" + str(connectionID)
    print(url)
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


    parquetTransfromUploadInput(job)
    shutil.rmtree('workDir')
    for i in range(connectedSources[0]['count']):
        url = urlPrefix+"accounts/"+str(acccountID)+"/connectedsources?limit=1&offset="+str(i)+"&connection=" + str(connectionID)
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
        #print(payload)
        create_resp = requests.post(urlPrefix+"accounts/"+acccountID+"/signalcontexts", json = payload, headers=headers)
        print(create_resp.status_code)

        
                 
                
acccountID1 = "849393527846985728"
datastreamID1 = "849433632037003264"
auth1 = os.environ.get("AUTH")
appUrl = "https://dev.falkonry.ai/api/1.2/"
createInputSignalConnection(acccountID1,datastreamID1, appUrl, auth1)



