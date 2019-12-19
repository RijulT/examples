FALKONRY CSV FILE LOADER
========================

VERSIONS
--------
1.0 Initial Release - November 3, 2019  
1.0.1 Added Chunking - Dec 5, 2019  
1.0.2 Added tracking of files by jobid - Dec 18, 2019

DESCRIPTION
-----------
This utility application can load multiple CSV files into a Falkonry datastream starting from a root directory and navigating down to all the leave files. Files to be loaded can be filtered by specifying a file filter such as 'the\*files.csv'. The utility will create a datastream if one does not exist that matches the name passed to it.

REQUIREMENTS
------------
The utility is written in .NET core and hence supports Windows, Linux and MacOS operating systems. To run the utility one needs to first install .NET core runtime using one of the methods described in https://dotnet.microsoft.com/download. Currently the application supports version 2.2. Make sure to click on the "All .NET Core Downloads" button and select the latest 2.2.* runtime.

RUNNING UTILITY
---------------
Binary distribution can be downloaded from https://github.com/Falkonry/examples/raw/master/csvfileloader/binaries/FalkonryCSVLoader.zip.

To invoke, change to the directly containing the binary distribution files and type:
dotnet FalkonryCSVLoader.dll --help

This will produce an output similar to that shown here, explaining the various parameters that are supported:

FalkonryCSVLoader 0.5.0
Falkonry, Inc
```USAGE:
Load CSV files to a Falkonry datastream (must specify either a stream name 'snam' or id 'sid'.
Example invocation::
  dotnet FalkonryCSVLoader.dll --acct 1234567891011121314 --conf wide.json --snam CustomerX_ATypeMachines --root
  ~Documents/Project/CustomerX/Data --tok t00L0NgToShoW1NOn3L1n3 --uri https://app3.falkonry.ai/api/1.1

  -r, --root     Required. Path to Root directory where files are located. Loader will traverse this directory and all
                 descendant directories looking for files to load.  It will only load files that match the filter 'f'
                 parameter.

  -c, --conf     Required. Config File Path. Path to the stream configuration file (see examples: wide.json, narrow.json,
                 etc.

  -u, --uri      Required. Uri to Falkonry Service. Http endpoint used to invoke the root of the Falkonry API.  Example:
                 https://app3.falkonry.ai/api/1.1

  -a, --acct     Required. Falkonry account id. Account id obtained from Url of Falkorny LRS UI.

  -t, --tok      Required. Token id for accessing the API.

  -j, --jsize    Number of files per job.  This is a throttling parameter. A new job will be created every 'j' files.
                 Negative or Zero values will be ignored.  Default: 100.

  -s, --sleep    Seconds to sleep after each job.  This is a throttling parameter to allow Falkonry to complete previous
                 ingest jobs.  Negative values will be ignored. Default: 10

  -f, --filt     Files name filter.  Specify a filter in the form of a literal path plus the ? and * parameters.  Example:
                 '*cleaned*.csv'.  Default='*.csv'

  -n, --snam     Datastream Name. Specify datastream name.  If specified, it will be used to either create a new datastream
                 or access and existing one.  This parameter is preferred over using 's' parameter.

  -i, --sid      Datastream id. Specify id of an existing datastream id obtained from Url of Falkorny LRS UI.  If this
                 parameter is specified, the loader will not create a new stream and hence ignore parameter 'n'.

  -k, --chunk    Chunk size in MB.  This parameter allows breaking large files into chunks to be sent to Falkonry.
                 Options={8, 16, 24, 36, 48, 64, 128}.  Default=64
              
  --help         Display this help screen.

  --version      Display version information.
```
CONFIGURATION
--------------
The binary distributions includes examples of \*.json configuration files that need to be modifed according to your needs.  These files define the charateristics of the data in the CSV files and hence how they will the be imported into the datastream.  The basic structure of these files defines the mappings of columns in the CSV file with the datastream parameters suchs as Entity, Signal, TimeZone, Batch, etc.

```
Example (narrow_batch.json):
{
  "batchIdentifier":"batch",
  "entityIdentifier": "entity",
  "timeIdentifier": "time",
  "timeFormat": "M/D/YYYY H:m:ss",
  "timeZone": "Europe/Paris"
}
```
USAGE EXAMPLES
---------------
### 1. WIDE FORMAT    
  a. Root directory containing files to be sent is **C:\temp**.     
  b. Files are in wide format and include an entity column. Data was collected in South Korea.  File fragment:    
    `time,entity,signal1,signal2,signal3`   
    `11.12.2019 23:34:11.234150,pump1,2.1,3.42,stopped,0`  
    `11.12.2019 23:34:21.241501,pump2,56.3,234.4,running,1`  
    `11.14.2019 03:04:11.000000,valve1,100.3,0.5,open,1`  
    ...   
  c. Configuration file **wide_example1.json** is located in **C:\user\example1**. Configuration file contents:``
  ```
  {
  "entityIdentifier":"entity",
  "timeIdentifier":"time",
  "timeFormat":"DD.MM.YYYY HH:mm:ss.SSSSSS", 
  "timeZone":"Asia/Seoul"
  }
  ```
  d. Directories have many '.csv' files and only wish to load the ones that have been cleaned (end in '-cleaned.csv').  
  e. Files are large and bandwidth is slow.  Wish to send in small chunks of 16MB.  
  f. Token is **AVERYLONGTOKEN**  
  g. Account id is **1343546**  
  h. DataStream name is **PLANT1**  
  i. Usage:  
    ```
    dotnet FalkonryCSVLoader.dll -u https://myfalkonrylrs/api/1.1 -a 1343546 -n PLANT1 -c C:\user\example1\wide_example1.json 
      -r C:\temp -f "\*-cleaned.csv" -k 16 -t AVERYLONGTOKEN
    ```    
### 2. NARROW FORMAT WITH BATCH  
  a. Root directory containing files to be sent is **C:\batches**.     
  b. Files are in narrow format. There is a single entity (Unit2) and is not specified in the files. 
     Data was collected in France (note: timezone does not have to be specified if format is ISO).  File fragment:  
    `time,batch,signal,value`  
    `2019-12-11T23:34:11.234150+01:00,BATCH1,Status,running`  
    `2019-12-11T09:12:21.241501+01:00,BATCH1,Flow,456.234`  
    `2019-12-14T13:04:11.000000+01:00,BATCH2,Position,50.4`  
    ...    
  c. Configuration file **narrow_batch1.json** is located in **C:\user\example2**. Configuration file contents:  
  ```
  {
  "entityKey":"Unit2",
  "batchIdentifier":"batch",
  "timeIdentifier":"time",
  "timeFormat":"iso_8061", 
  "signalIdentifier":"signal",
  "valueIdentifier":"value"
  }
  ```  
  d. Token is **AVERYLONGANDBATCHYTOKEN**  
  e. Account id is **1343546**  
  d. DataStream id is **999666333**  
  g. Usage:  
    ```
    dotnet FalkonryCSVLoader.dll -u https://myfalkonrylrs/api/1.1 -a 1343546 -i 999666333 -c C:\user\example2\narrow_batch1.json 
      -r C:\batches -t AVERYLONGANDBATCHYTOKEN**
    ```


TROUBLESHOOTING AND LOGGING
---------------------------
The utility produces log files in a Log subdirectory.  It creates one file per day.  The files contain all responses from the API as well as any exception messages.  Note that the utility will continue to send files even if previous files reported failures.  Hence is a good idea to look in the log files to determine which files did not get accepted by the API.

In addition, the utility creates a Responses subfolder where it stores a "Responses.txt" file containing the responses from the last program execution.

The utility does not wait for the INGEST or DIGEST jobs to complete.  Errors experienced in INGEST and DIGEST jobs are only seen using LRS's UI and log monitors.

SOURCE
------
Source for this utility can be found in https://github.com/mariofalkonry/com.falkonry.api.client

LIMITATIONS
-----------
The utility only handles ingestion of historical files for model training.

ROADMAP
-------
Currently the applications does not support Mappings for signals.  This will be in a future relase.
Additionally it does not support ";" separators or "," for decimal points.  This will have to wait for Falkonry's web API to support these.
