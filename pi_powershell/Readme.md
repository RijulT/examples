OVERVIEW
--------
This script uses OSISoft provided cmdlets for PowerShell to extract tags in the narrow format needed to load signals into Falkonry.   
It allows for mapping of signals and entities to tags as well as deals with some of the common issues associated with values of numeric tags 
which can sometimes contain status messages from PI.

PRE-REQUISITES
--------------
This script require PI SMT Version 2015 (3.5.1.7) or later installed in the host where the script will be executed.  The script is not signed and will not run in systems
using a restricting policy for PowerShell scripts. 

USAGE
-----
Run as administrator.

Invocation:
  .\TagDataExtractor.ps1 -h hostname -f tagsfile -s starttime [-e endtime] [-b blocksize] -o outputdir

Where:
hostname - The PI Host or Collective name or address.

tagsfile - A path to the csv file containing the list of tags.

starttime - The start time for the data.  The Local timezone will be assumed.

endtime - The end time for the data. The local timezone will be assumed.  Default: Current Time.

blockSize - A string specifying the number of hours or days to include in each file.  Examples: 2d, 4h, 1d, 24h, etc.  Default: 1d

outputdir - Directory in which to place exported csv files.

NOTES
-----
The csv file containing tags is expected to be in the following format:
tag[,signal][,entity] 

A header is expected with any of the above columns in any particular order.

The output csv files with be in the format:
[Entity,][Signal,]Tag,Value,Timestamp(UTC),IsGood,IsAnnotated,IsSubstituted,IsQuestionable,IsServerError[,signal][,entity]

When the signal and entity are specified in the csv file containing the tags to extract, the narrow column mappings for importing the files are:
timestamp => Timestamp(UTC) 
entity => Entity
signal => Signal
value => value

When the signal is not specified, the tag will be assumed to be the signal name.  The narrow format column mappings for importing the files are:
timestamp => Timestamp(UTC) 
entity => Entity
signal => tag
value => value

If the entity is not specified, the narrow format column mappings for importing the files are:
timestamp => Timestamp(UTC) 
signal => tag or Signal depending on csv file containing tags list
value => value

