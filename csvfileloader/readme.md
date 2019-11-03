DESCRIPTION
-----------
This utility application can load multiple CSV files into a Falknroy datastream starting from a root directory and navigating down to all the leave files. Files to be loaded can be filtered by specifying a file filter such as 'the\*files.csv'. The utility will create a datastream if one does not exist that matches the name passed to it.

REQUIREMENTS
------------
The utility is written in .NET core and hence supports Windows, Linux and MacOS operating systems. To run the utility one needs to first install .NET core runtime using one of the methods described in https://dotnet.microsoft.com/download. Currently the application supports version 2.2. Make sure to click on the "All .NET Core Downloads" button and select the latest 2.2.* runtime.

INVOKATION
----------
Binary distribution can be found here.

To invoke, change to the directly conataing the g:
dotnet Falkonry

SOURCE
------
Source for this utility can be found in https://github.com/mariofalkonry/com.falkonry.api.client

ROADMAP
-------
Currently the applications does not support Mappings for signals.  This will be in a future relase.
Additionally it does not support ";" separators or "," for decimal points.  This will have to wait for Falkonry's web api support.  
 
