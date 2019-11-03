DESCRIPTION
-----------
This utility application can load multiple CSV files into a Falknroy datastream starting from a root directory and navigating down to all the leave files. Files to be loaded can be filtered by specifying a file filter such as 'the\*files.csv'. The utility will create a datastream if one does not exist that matches the name passed to it.

REQUIREMENTS
------------
The utility is written in .NET core and hence supports Windows, Linux and MacOS operating systems. To run the utility one needs to first install .NET core runtime using one of the methods described in https://dotnet.microsoft.com/download. Currently the application supports version 2.2. Make sure to click on the "All .NET Core Downloads" button and select the latest 2.2.* runtime.

INVOKATION
----------
Binary distribution can be found here.

To invoke, change to the directly conataing the binary distribution files and type:
dotnet FalkonryCSVLoader.dll --help

This will explain what parameters are expected similarly as shown here:

FalkonryCSVLoader 0.5.0
Falkonry, Inc
USAGE:
Load CSV files to a Falkonry datastream (must specify either a stream name 'snam' or id 'sid'. Example invokation::
  dotnet FalkonryCSVLoader.dll --acct 1234567891011121314 --conf wide.json --snam CustomerX_ATypeMachines --root
  ~Documents/Project/CustomerX/Data --tok t00L0NgToShoW1NOn3L1n3 --uri https://app3.falkonry.ai/api/1.1

  -r, --root     Required. Path to Root directory where files are located. Loader will traverse this directory and all
                 descendant directories looking for files to load.  It will only load files that match the filter 'f'
                 parameter.

  -c, --conf     Required. Config File Path. Path to the stream configuration file (see examples: wide.json,
                 narrow.json, etc.

  -u, --uri      Required. Uri to Falkonry Service. Http endpoint used to invoke the root of the Falkonry API.  Example:
                 https://app3.falkonry.ai/api/1.1

  -a, --acct     Required. Falkonry account id. Account id obtained from Url of Falkorny LRS UI.

  -t, --tok      Required. Token id for accessing the API.

  -j, --jsize    Number of files per job.  This is a throttling parameter. A new job will be created every 'j' files.
                 Negative or Zero values will be ignored.  Default: 100.

  -s, --sleep    Seconds to sleep after each job.  This is a throttling parameter to allow Falkonry to complete previous
                 ingest jobs.  Negative values will be ignored. Default: 10

  -f, --filt     Files name filter.  Specify a filter in the form of a literal path plus the ? and * parameters.
                 Example:  '*cleaned*.csv'.  Default='*.csv'

  -n, --snam     Datastream Name. Specify datastream name.  If specified, it will be used to either create a new
                 datastream or access and existing one.  This parameter is preferred over using 's' parameter.

  -i, --sid      Datastream id. Specify id of an existing datastream id obtained from Url of Falkorny LRS UI.  If this
                 parameter is specified, the loader will not create a new stream and hence ignore parameter 'n'.

  --help         Display this help screen.

  --version      Display version information.



SOURCE
------
Source for this utility can be found in https://github.com/mariofalkonry/com.falkonry.api.client

ROADMAP
-------
Currently the applications does not support Mappings for signals.  This will be in a future relase.
Additionally it does not support ";" separators or "," for decimal points.  This will have to wait for Falkonry's web api support.  
 