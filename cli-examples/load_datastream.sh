#!/bin/bash

#
# Loads the wide & narrow format CSV files to Falkonry datastream
# Requires the Falkonry CLI to be installed See (https://github.com/Falkonry/falkonry-cli)
#
# Supply the ENVIRONMENT variable values
#

FALKONRY_HOST=""			#REQUIRED *NO* trailing slash '/'
SECURITY_TOKEN=""			#REQUIRED
DATASTREAM_ID=""			#REQUIRED
TIMESTAMP_COLUMN_NAME=""	#REQUIRED
TIMESTAMP_FORMAT=""			#REQUIRED
TIME_ZONE=""				#REQUIRED
ENTITY_IDENTIFIER=""		#REQUIRED if your data file has entity column
BATCH_IDENTIFIER=""			#REQUIRED if your datastream is a batch datastream
PAUSE_TIME=60				#REQUIRED Seconds
SIGNAL_IDENTIFIER=""		#REQUIRED IF the CSV file is in *narrow* format
VALUE_IDENTIFIER=""			#REQUIRED IF the CSV file is in *narrow* format

#
# Function to load an individual wide & narrow format CSV file.
#
function load_file() {
	#
	# Take care of quoting spaces in names
	#
	ENT_ID=""
	BATCH_ID=""
	SIGNAL_ID=""
	VALUE_ID=""
	if [[ "$ENTITY_IDENTIFIER" -ne "" ]]
	then
		ENT_ID="--entityIdentifier \"$ENTITY_IDENTIFIER\""
	fi

	if [[ "$BATCH_IDENTIFIER" -ne "" ]]
	then
		BATCH_ID="--batchIdentifier \"$BATCH_IDENTIFIER\""
	fi

	#
	# ONLY required for NARROW format CSV files
	#
	if [[ "$SIGNAL_IDENTIFIER" -ne "" ]]
	then
		SIGNAL_ID="--signalIdentifier \"$SIGNAL_IDENTIFIER\""
	fi

	#
	# ONLY required for NARROW format CSV files
	#
	if [[ "$VALUE_IDENTIFIER" -ne "" ]]
	then
		VALUE_ID="--valueIdentifier \"$VALUE_IDENTIFIER\""
	fi

	if [[ "${FALKONRY_HOST: -1}" == "/" ]]
	then
		FALKONRY_HOST="${FALKONRY_HOST%/}"
	fi

	FNAME="$1"

	echo "$(date) -- Loading file $FNAME"
	falkonry << EOF
login --host=$FALKONRY_HOST --token=$SECURITY_TOKEN
datastream_default_set --id=$DATASTREAM_ID
datastream_add_historical_data --timeIdentifier="$TIMESTAMP_COLUMN_NAME" --timeFormat="$TIMESTAMP_FORMAT" --timeZone "$TIME_ZONE" --path "$FNAME" $ENT_ID $BATCH_ID $SIGNAL_ID $VALUE_ID
EOF
	echo "$(date) -- Loading file $FNAME complete"
	#
	# Default Pause is 60 seconds
	#
	PAUSE_T=${PAUSE_TIME:-60}
	sleep $PAUSE_T #Give some time for data ingestion before starting next file
}

#
# Main function
#
function main() {
	if [[ "${#@}" == "0" ]]
	then
		echo "Usage $0 <files to load>"
	elif [[ -z $SECURITY_TOKEN || -z $DATASTREAM_ID || -z $TIMESTAMP_COLUMN_NAME || -z $TIMESTAMP_FORMAT || -z $TIME_ZONE ]]
	then
		echo "ERROR: Values for one or more variables (SECURITY_TOKEN, DATASTREAM_ID, TIMESTAMP_COLUMN_NAME, TIMESTAMP_FORMAT, TIME_ZONE) are missing. Update this file with values"
	else
		#
		# Loop through the files to load
		#
		for F0 in "$@"
		do
			load_file "$F0"
		done
	fi
}

main "$@"
