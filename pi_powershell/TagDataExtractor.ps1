<#
The MIT License
Copyright © 2010-2019 Falkonry.com
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#>

#PARAMETERS
param(
    [string] $h,
    [string] $f,
    [string] $s,
    [string] $e = $(Get-Date -Format "yyyy-MM-dd HH:mm:ss"),
    [string] $o,
    [string] $b = '1d' 
)

function Usage()
{
    Write-Output "Usage:"
    Write-Output "`t.\TagDataExtractor.ps1 -h hostname -f tagsfile -s starttime [-e endtime] [-b blocksize] -o outputdir"
    Write-Output "Where:"
	Write-Output "`t`thostname - The PI Host or Collective name or address."
	Write-Output "`t`ttagsfile - A path to the csv file containing the list of tags."
	Write-Output "`t`tstarttime - The start time for the data.  The local timezone of the script host will be assumed."
	Write-Output "`t`tendtime - OPTIONAL - The end time for the data. The local timezone of the script host will be assumed.  Default: Current Time."
	Write-Output "`t`tblocksize - OPTIONAL - A string specifying the number of hours or days to include in each file.  Examples: 2d, 4h, 1d, 24h, etc.  Default: 1d."
	Write-Output "`t`toutputdir - Directory in which to place exported csv files."
	Write-Output ""
	Write-Output "Times must be in the yyyy-MM-dd HH:mm:ss military time format"
	exit
}

function PrintParams()
{
	Write-Output $"hostname=$h"
	Write-Output $"tagsfile=$f"
	Write-Output $"startime=$s"
	Write-Output $"endtime=$e"
	Write-Output $"blocksize=$b"
	Write-Output $"outputdir=$o"
}

Add-Type -Assembly Microsoft.VisualBasic
function CoerceValue([string] $pttype,[string] $value)
{
	if($pttype.ToLower().StartsWith('flo') -or $pttype.ToLower().StartsWith('int'))
	{
		if(-not [Microsoft.VisualBasic.Information]::IsNumeric($value))
		{
			return ""
		}
	}
	return $value
}

# PATTERNs
$datePattern = '([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])[ ](?:[01]\d|2[0123]):(?:[012345]\d):(?:[012345]\d))'

# Validate Inputs
if([string]::IsNullOrEmpty($h))
{
    Write-Output("PI host not specified")
    Usage
}
if([string]::IsNullOrEmpty($f))
{
	Write-Output "Tags csv file path not specified"
    Usage
}
elseif(-not(Test-Path("$f")))
{
	Write-Output "Invalid tags csv file - file does not exist"
    exit
}

if([string]::IsNullOrEmpty($s))
{
	Write-Output "Start time not specified"
    Usage
}
elseif($s -notmatch $datePattern)
{
	Write-Output "Start time not in the correct format yyyy-MM-dd HH:mm:ss"
	exit
}
$startTime=Get-Date($s)

if($e -notmatch $datePattern)
{
	Write-Output "End time not in the correct format yyyy-MM-dd HH:mm:ss"
	exit
}
$endTime=Get-Date($e)
if($startTime -ge $endTime)
{
	Write-Output "End time must be greater than start time"
	exit
}

if([string]::IsNullOrEmpty($o))
{
	Write-Output "Output directory not specified"
    Usage
}
else
{
	# Delete csv Files in output folder if existing
	if(Test-Path("$o"))
	{
		Remove-Item -Path "$o\*" -Include *.csv
	}
	# otherwise create it
	else
	{
		Write-Output "Creating output directory $o"
		New-Item $o -ItemType Directory
	}
}

if($b.EndsWith('d') -or $b.EndsWith('D'))
{
	try
	{
	    $blockDays = [int] $b.Substring(0,$b.Length-1)
		$blockHours = $blockDays*24
	}
	catch
	{
		Write-Output "Blocksize not in the correct format"
		Usage
	}
}
elseif($b.EndsWith('h') -or $b.EndsWith('H'))
{
	try
	{
		$blockHours = [int] $b.Substring(0,$b.Length-1)
	}
	catch
	{
		Write-Ouput "Blocksize not in the correct format"
		Usage
	}
}
else
{
	Write-Ouput "Blocksize not in the correct format"
	Usage
}
if($blockHours -le 0)
{
	Write-Output "Blocksize must be a positive number"
	Usage
}

#DEBUG PrintParams
             
Write-Output $"Creating output files starting at $s"

# Get connection to PI
Write-Output "Getting connection to pi server $h"
$con = Connect-PIDataArchive -PIDataArchiveMachineName $h 

# Get header
$lines=Get-Content $f
$header=$lines[0].Split(",")
$tagIndex=-1
$signalIndex=-1
$entityIndex=-1
# get column indexes
$idx=0
foreach($part in $header)
{
	if($part.ToLower() -eq 'tag')
	{
		$tagIndex=$idx
	}
	elseif($part.ToLower() -eq 'entity')
	{
		$entityIndex=$idx
	}
	elseif($part.ToLower() -eq 'signal')
	{
		$signalIndex=$idx
	}
	$idx++
}
# Validate at least a tag is provided
if($tagIndex -eq -1)
{
	Write-Ouput "File $f does not contain a tag column"
	exit
}

if($lines.Count -lt 2)
{
	Write-Ouput "File $f does not contain any tags"
	exit
}
$i=1
do
{
	$parts=$lines[$i].Split(",")
	$tagName=$parts[$tagIndex]
	$bn=0
	$blockStartTime=$startTime
	$pttype=(Get-PIPoint -Name $tagName -Attributes pointtype -Connection $con).Attributes.pointtype
	do
	{
		# Get end time for this block of data
		$blockEndTime = (Get-Date $blockStartTime).AddHours($blockHours)
		if($blockEndTime -gt $endTime)
		{
			$blockEndTime=$endTime		
		}
		# Get values from PI
		$values = Get-PIValue  -PointName $tagName -StartTime $blockStartTime -EndTime $blockEndTime -Connection $con
		Write-Output "The total number of PI Values in BLOCK $i retrieved are" $values.Count
		# Write to file
		if($entityIndex -gt -1 -and $signalIndex -gt -1)
		{
			# Create file for block
			$fileName = ($parts[$entityIndex] -replace '[\W]','_')+'_'+($parts[$signalIndex] -replace '[\W]','_')+'_Block_'+$bn
			New-Item -ItemType File -Path $o -Name "$fileName.csv"
			Write-Output "created file $o\$fileName.csv"
			$values | Select-Object -Property @{Name='Entity';Expression={$parts[$entityIndex]}},@{Name='Signal';Expression={$parts[$signalIndex]}},@{Name='Tag';Expression={$tagName}},@{Name='Value';Expression={CoerceValue $pttype.ToString() $_.value.ToString()}},@{Name='Timestamp(UTC)';Expression={$_.timestamp.ToString("o")}},IsGood,IsAnnotated,IsSubstituted,IsQuestionable,IsServerError | Export-Csv -Path "$o\$fileName.csv" -NoTypeInformation 
		}
		elseif($entityIndex -gt -1)
		{
			# Create file for block
			$fileName = ($parts[$entityIndex] -replace '[\W]','_')+'_'+($tagName -replace '[\W]','_')+'_Block_'+$bn
			New-Item -ItemType File -Path $o -Name "$fileName.csv"
			Write-Output "created file $o\$fileName.csv"
			$values | Select-Object -Property @{Name='Entity';Expression={$parts[$entityIndex]}},@{Name='Tag';Expression={$tagName}},@{Name='Value';Expression={CoerceValue $pttype.ToString() $_.value.ToString()}},@{Name='Timestamp(UTC)';Expression={$_.timestamp.ToString("o")}},IsGood,IsAnnotated,IsSubstituted,IsQuestionable,IsServerError | Export-Csv -Path "$o\$fileName.csv" -NoTypeInformation 
		}
		elseif($signalIndex -gt -1)
		{
			# Create file for block
			$fileName = ($parts[$signalIndex] -replace '[\W]','_') +'_Block_'+$bn
			New-Item -ItemType File -Path $o -Name "$fileName.csv"
			Write-Output "created file $o\$fileName.csv"
			$values | Select-Object -Property @{Name='Signal';Expression={$parts[$signalIndex]}},@{Name='Tag';Expression={$tagName}},@{Name='Value';Expression={CoerceValue $pttype.ToString() $_.value.ToString()}},@{Name='Timestamp(UTC)';Expression={$_.timestamp.ToString("o")}},IsGood,IsAnnotated,IsSubstituted,IsQuestionable,IsServerError | Export-Csv -Path "$o\$fileName.csv" -NoTypeInformation 
		}
		else
		{
			# Create file for block
			$fileName = ($tagName -replace '[\W]','_')+'_Block_'+$bn
			New-Item -ItemType File -Path $o -Name "$fileName.csv"
			Write-Output "created file $o\$fileName.csv"
			$values | Select-Object -Property @{Name='Tag';Expression={$tagName}},@{Name='Value';Expression={CoerceValue $pttype.ToString() $_.value.ToString()}},@{Name='Timestamp(UTC)';Expression={$_.timestamp.ToString("o")}},IsGood,IsAnnotated,IsSubstituted,IsQuestionable,IsServerError | Export-Csv -Path "$o\$fileName.csv" -NoTypeInformation 
		}

		# increment block times
		$blockStartTime = $blockEndTime
		$bn++
	} until($blockStartTime -ge $endTime)
} until(++$i -ge $lines.Count)
  
  
  
  
