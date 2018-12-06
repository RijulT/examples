#
# Loads the wide & narrow format CSV files to Falkonry datastream
# Requires the Falkonry CLI to be installed See (https://github.com/Falkonry/falkonry-cli)
#
# Supply the ENVIRONMENT variable values
#

$FALKONRY_HOST=""			#REQUIRED *NO* trailing slash '/'
$SECURITY_TOKEN=""			#REQUIRED
$DATASTREAM_ID=""			#REQUIRED
$TIMESTAMP_COLUMN_NAME=""	#REQUIRED
$TIMESTAMP_FORMAT=""		#REQUIRED
$TIME_ZONE=""				#REQUIRED
$ENTITY_IDENTIFIER=""		#REQUIRED if your data file has entity column
$BATCH_IDENTIFIER=""		#REQUIRED if your datastream is a batch datastream
$PAUSE_TIME=60				#REQUIRED Seconds
$SIGNAL_IDENTIFIER=""		#REQUIRED IF the CSV file is in *narrow* format
$VALUE_IDENTIFIER=""		#REQUIRED IF the CSV file is in *narrow* format

#
# Function to load an individual wide & narrow format CSV file.
#
function load_file() {
	param ($f_name)
	#
	# Escape the windows file names
	#
	$f_name = $f_name -replace "\\", "/"
	$dt = Get-Date
	echo "$dt -- Loading file $f_name"
	#
	# Take care of quoting spaces in names
	#
	$ENT_ID = ""
	$BATCH_ID = ""
	if ($ENTITY_IDENTIFIER -ne "") {
		$ENT_ID = "--entityIdentifier ""$ENTITY_IDENTIFIER"""
	}
	if ($BATCH_IDENTIFIER -ne "") {
		$BATCH_ID = "--batchIdentifier ""$BATCH_IDENTIFIER"""
	}
	#
	# ONLY required for NARROW format CSV files
	#
	if ($SIGNAL_IDENTIFIER -ne "") {
		$SIGNAL_ID="--signalIdentifier ""$SIGNAL_IDENTIFIER"""
	}

	#
	# ONLY required for NARROW format CSV files
	#
	if ($VALUE_IDENTIFIER -ne "") {
		$VALUE_ID="--valueIdentifier ""$VALUE_IDENTIFIER"""
	}

	if ($FALKONRY_HOST.endswith("/")) {
		$FALKONRY_HOST = $FALKONRY_HOST.substring(0,$FALKONRY_HOST.length - 1)
	}

#
# Execute the file load command (DO NOT INDENT THESE LINES)
#
@"
login --host=$FALKONRY_HOST --token=$SECURITY_TOKEN
datastream_default_set --id=$DATASTREAM_ID

datastream_add_historical_data --timeIdentifier "$TIMESTAMP_COLUMN_NAME" --timeFormat "$TIMESTAMP_FORMAT" --path "$f_name" --timeZone "$TIME_ZONE" $ENT_ID $BATCH_ID $SIGNAL_ID $VALUE_ID
"@ | falkonry

	$dt = Get-Date
	echo "$dt -- Loading file $f_name complete"
	echo ""
	#
	# Default Pause is 60 seconds
	#
	sleep $PAUSE_TIME #Give some time for data ingestion before starting next file
}

#
# Main function
#
function main() {
	param([String[]]$files)
	if ($files.Count -eq 0) {
		echo "Usage $0 <files to load>"
	} elseif ( (-not $SECURITY_TOKEN) -or (-not $DATASTREAM_ID) -or (-not $TIMESTAMP_COLUMN_NAME) -or (-not $TIMESTAMP_FORMAT) -or (-not $TIME_ZONE) ) {
		echo "ERROR: Values for one or more variables (SECURITY_TOKEN, DATASTREAM_ID, TIMESTAMP_COLUMN_NAME, TIMESTAMP_FORMAT, TIME_ZONE) are missing. Update this file with values"
	} else {
		#
		# Loop through the files to load
		#
		foreach ($file0 in $files) {
			foreach ($file00 in (get-item $file0)) {
				load_file "$file00"
			}
		}
	}
}

main @Args
