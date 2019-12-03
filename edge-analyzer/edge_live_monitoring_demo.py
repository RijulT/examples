#!/usr/bin/env python3
"""
The MIT License
Copyright © 2010-2019 Falkonry.com
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

"""
README
This is a *simple example* of "Falkonry Edge Analyzer Live Monitoring" script. 
It reads a CSV file for input data, saves the condition/prediction, confidence and explanation scores 
in an output file.  This code assumes a wide format CSV file for input.  Although Falkonry supports other 
formats and data ingestion methods, this code does not illustrate those methods.

Falkonry LRS Live monitoring would be similar, however, it requires authentication and REST urls will have different prefixes.

Assumptions:
* You only have default entity in your datastream
* This script does not support historical data ingestion; just live monitoring
* This script can only monitor one assessment for live monitoring.
* You are supplying data to Falkonry in csv format.
* Output will be saved to csv file.
"""

import argparse
import datetime
import json
import os
import os.path
import re
import sys
import threading
import time
import traceback

import ndjson
import requests

#
# Treat these as constants
#
F_EDGE_URL = 'http://192.168.2.2:9004/'


class Utils:
    @staticmethod
    def info(msg):
        """
        Log method
        """
        ts = str(datetime.datetime.now())
        print("INFO :" + ts + " : " + str(msg), flush=True)

    @staticmethod
    def err_msg(msg):
        """
        Error log method
        """
        ts = str(datetime.datetime.now())
        print("ERROR:" + ts + " : " + str(msg))  # , flush=True


class ColumnsInfo:
    def __init__(self, headerLine, timeColumnIndex, entityColumnIndex=None, batchColumnIndex=None):
        self.timeColumnIndex = timeColumnIndex
        self.entityColumnIndex = entityColumnIndex
        self.batchColumnIndex = batchColumnIndex
        self.headerColumns = []
        self.signals = []
        self.xplanationColumns = []
        self.xpMap = {}
        self.headerLine = headerLine
        self.headerColumns = self.headerLine.split(',') if self.headerLine else []
        for nI in range(0, len(self.headerColumns)):
            if not (nI == self.timeColumnIndex or nI == self.entityColumnIndex or nI == self.batchColumnIndex):
                self.signals.append(self.headerColumns[nI])

    def signals(self):
        return self.signals

    '''
    The number of signals used in the model may be less than input signals supplied.
    '''

    def set_explanation_output_column_info(self, xp_map, xp_columns):
        self.xpMap = xp_map
        self.xplanationColumns = xp_columns

    def header_columns(self):
        return self.headerColumns

    def column(self, index):
        return None if index is None else self.headerColumns[index]

    def output_header(self):
        ent = "" if self.entityColumnIndex is None else "entity,"
        bch = "" if self.batchColumnIndex is None else "batch,"
        return "time," + ent + bch + "condition,confidence," + ",".join(self.xplanationColumns) + "\n"


"""
Class representing Falkonry output - condition/prediction, confidence, and explanation scores.
"""


class FalkonryOutput:
    def __init__(self, columnInfo):
        self.entity = None
        self.batch = None
        self.time = None
        self.condition = None
        self.confidence = None
        self.columnInfo = columnInfo
        self.explanations = [None] * len(self.columnInfo.xplanationColumns)

    def is_complete(self):
        return (self.condition != None and not (None in self.explanations) and self.confidence != None)

    def to_string(self):
        ent = "" if self.entity is None else self.entity + ","
        bch = "" if self.batch is None else self.batch + ","
        result = str(self.time) + "," + ent + bch + str(self.condition) + "," + str(self.confidence)
        for column in self.explanations:
            result += "," + str(column)
        result += "\n"
        return result

    def set_entity_batch(self, j_obj):
        #
        # Set these only if the entity and batch columns exist
        #
        if self.columnInfo.entityColumnIndex:
            self.entity = j_obj['entity']
        if self.columnInfo.batchColumnIndex:
            self.batch = j_obj['batch']


class FalkonryOutputProcessor(threading.Thread):
    def __init__(self, columnInfo, outputFile):
        super().__init__()
        self.columnInfo = columnInfo
        self.outputFile = outputFile
        self.dataSync = open(outputFile, 'w')
        self.outputMap = {}
        self.timeArrowList = []

    def populate_explanations(self, x_out, ignore_first=False):
        r_index = None
        out_x = None
        xp_map = self.columnInfo.xpMap
        if x_out.status_code == 200:
            out_x = ndjson.loads(x_out.content.decode())
        isFirst = True
        for item in out_x:
            if ignore_first and isFirst:
                isFirst = False
                continue
            oitem = self.outputMap.get(item['time'])
            if oitem is None:
                oput = FalkonryOutput(self.columnInfo)
                oput.set_entity_batch(item)
                oput.time = item['time']
                oput.explanations[xp_map[item['signal']]] = item['score']
                self.outputMap[oput.time] = oput

                self.timeArrowList.append({"ts": oput.time, "complete": 0})
            else:
                oitem.explanations[xp_map[item['signal']]] = item['score']

            r_index = item['index']
        return r_index

    def _populate_value(self, c_input, isCondition, ignore_first=False):
        """
        Common method to populate condition/prediction label, and confidence score.
        """
        r_index = None
        out_c = None
        if c_input.status_code == 200:
            out_c = ndjson.loads(c_input.content.decode())
        isFirst = True
        for item in out_c:
            if ignore_first and isFirst:
                isFirst = False
                continue
            oitem = self.outputMap.get(item['time'])
            if oitem is None:
                oitem = FalkonryOutput(self.columnInfo)
                oitem.set_entity_batch(item)
                oitem.time = item['time']
                self.outputMap[oitem.time] = oitem
                self.timeArrowList.append({"ts": oitem.time, "complete": 0})

            if (isCondition):
                oitem.condition = item['value']
            else:
                oitem.confidence = item['value']
            r_index = item['index']
        return r_index

    def populate_confidences(self, conf_out, ignore_first=False):
        return self._populate_value(conf_out, isCondition=False, ignore_first=ignore_first)

    def populate_assessments(self, cond_out, ignore_first=False):
        return self._populate_value(cond_out, isCondition=True, ignore_first=ignore_first)

    def write_output(self, isFirst=False):
        #
        # Output Completed data
        #
        o_count = 0
        if isFirst:
            self.dataSync.write(self.columnInfo.output_header())
        for nI in range(0, len(self.timeArrowList), 1):
            ts = self.timeArrowList[nI]
            if self.outputMap.get(ts['ts']).is_complete():
                ts['complete'] = 1
                self.dataSync.write(self.outputMap.get(ts['ts']).to_string())
                self.dataSync.flush()
            else:
                Utils.info("Incomplete !!: " + self.outputMap.get(ts['ts']).to_string())
                break

        for nI in range(len(self.timeArrowList) - 1, -1, -1):
            if self.timeArrowList[nI]['complete'] == 1:
                del self.timeArrowList[nI]
                o_count += 1

        return o_count

    def run(self):
        Utils.info("Output file " + self.outputFile)
        self.write_output(isFirst=True)
        """
        Model output polling method.
        """
        signal_count = len(self.columnInfo.xpMap)
        Utils.info('output_thread')
        isFirst = True
        x_counter = 0
        a_out = None
        x_out = None
        c_out = None
        a_index = None
        x_index = None
        c_index = None
        o_count = 0
        accept = {'Accept': 'application/x-ndjson'}

        while True:
            try:
                if isFirst:
                    #
                    # Get the very first assessment.  Call method without offset.
                    #
                    a_out = requests.get(F_EDGE_URL + 'api/1.1/outputs/assessments', {'limit': 1},
                                         headers=accept)
                    x_out = requests.get(F_EDGE_URL + 'api/1.1/outputs/explanations', {'limit': signal_count},
                                         headers=accept)
                    c_out = requests.get(F_EDGE_URL + 'api/1.1/outputs/confidences', {'limit': 1},
                                         headers=accept)
                    if a_out != None and x_out != None and c_out != None:
                        a_index = self.populate_assessments(a_out)
                        x_index = self.populate_explanations(x_out)
                        c_index = self.populate_confidences(c_out)
                        Utils.info(
                            "First a_index " + str(a_index) + " x_index " + str(x_index) + " c_index " + str(c_index))
                        if a_index is None or x_index is None or c_index is None:
                            isFirst = True
                            time.sleep(1)
                        else:
                            isFirst = False
                    else:
                        time.sleep(1)
                else:
                    a_index0 = self.populate_assessments(
                                    requests.get(F_EDGE_URL + 'api/1.1/outputs/assessments',
                                                 {'offsetType': 'index', 'offset': a_index},
                                                 headers=accept), True)
                    x_index0 = self.populate_explanations(
                                    requests.get(F_EDGE_URL + 'api/1.1/outputs/explanations',
                                                 {'offsetType': 'index', 'offset': x_index,
                                                 'limit': signal_count * 50},
                                                 headers=accept), True)
                    c_index0 = self.populate_confidences(
                                    requests.get(F_EDGE_URL + 'api/1.1/outputs/confidences',
                                                 {'offsetType': 'index', 'offset': c_index},
                                                 headers=accept), True)
                    if a_index0:
                        a_index = a_index0
                    if x_index0:
                        x_index = x_index0
                    if c_index0:
                        c_index = c_index0

                    Utils.info("Next a_index0 " + str(a_index0) + " x_index0 "
                                + str(x_index0) + " c_index0 " + str(c_index0))
                    #
                    # Output Completed data
                    #
                    o_count += self.write_output(isFirst=False)

                    Utils.info("Total output assessments = " + str(o_count) + "  Incomplete list size : "
                               + str(len(self.timeArrowList)))
                    time.sleep(1)

                x_counter = 0 # Reset the exception counter
            except Exception as e:
                Utils.err_msg("Exception during output processing !!! " + str(e))
                x_counter += 1
                traceback.print_stack()
                time.sleep(5)
                if x_counter >= 50:
                    Utils.err_msg("Too many exceptions.  Stopping output thread.")
                    break
                else:
                    pass


"""
Data sent to Falkonry MUST be sorted in increasing time order.
   Once data at time T is processed in Falkonry live monitoring, if you pass data with timestamp less than T,
   that data will be ignored and no assessment will be produced for that data.
   The Live Monitoring maintains the last time T it processed.
   If you want to process data before time T, you have to stop Live Monitoring process and restart.
   At this point you can restart sending data.
"""


class FalkonryInputProcessor(threading.Thread):
    CHUNK_SIZE = 10000
    def __init__(self, args):
        super().__init__()
        self.timeIx = args.time
        self.timeFormat = args.format
        self.timeZone = args.zone
        self.entityIx = args.entity
        self.batchIx = args.batch
        self.filename = args.input
        self.feedRate = args.rate
        self.datasource = open(self.filename, "r")
        firstLine = self.datasource.readline()
        firstLine.strip()
        self.columnsInfo = ColumnsInfo(firstLine, self.timeIx, self.entityIx, self.batchIx)
        self.inputJobId = self.__create_edge_input_job()
        self.__map_explanation_scores_to_signals()

    def print_model_info(self):
        """
        Prints the current Edge Model information in json format to console
        """
        http_headers = {'content-type': 'application/json'}
        inputResponse = requests.get(F_EDGE_URL + 'api/1.1/model', auth=None,
                                      verify=False, headers=http_headers)
        Utils.info("InputResponse : " + str(inputResponse))
        model = inputResponse.json()
        Utils.info("Model Info :\n" + json.dumps(model, indent=2))

    def __create_edge_input_job(self):
        """
        Start Edge container and visit http[s]://edge-url:port/swagger.app for Edge API.
        See 'api/1.1/ingestjobs' help.

        This method is to create a Edge Analyzer Job.  During job creation you can:
        1. map signal names - which could be different from model signal names,
        2. time column name, time format & time zone - different from what model used,
        3. [entity identifier column mapping]
        4. [batch identifier column mapping]
        5. [signal identifier column mapping (only applicable for narrow format)]
        6. [value identifier column mapping (only applicable for narrow format)]
        """

        http_headers = {'content-type': 'application/json'}
        data = {
            "type": "Ingest",
            "timeIdentifier": self.columnsInfo.column(self.timeIx),  # "timestamp",
            "timeFormat": self.timeFormat,  # "micros",
            "timeZone": self.timeZone  # example "Europe/Stockholm",
        }
        if self.entityIx is not None:
            data["entityIdentifier"] = self.columnsInfo.column(self.entityIx)
        if self.batchIx is not None:
            data["batchIdentifier"] = self.columnsInfo.column(self.batchIx)

        Utils.info("Create_Edge_input_job  :" + str(data))
        inputResponse = requests.post(F_EDGE_URL + 'api/1.1/ingestjobs', auth=None,
                                      data=json.dumps(data), verify=False, headers=http_headers)
        Utils.info("InputResponse : " + str(inputResponse))
        job = inputResponse.json()
        Utils.info("Input job : " + str(job))
        Utils.info("Input job id : " + job["id"])
        self.inputJobId = job["id"]

        return self.inputJobId

    def __map_explanation_scores_to_signals(self):
        columns = self.columnsInfo.signals
        Utils.info("Columns : " + str(columns))
        #
        # Signal IDs/Names would be the same no matter what job.
        # Using Edge default Job 1 to retrieve the map (Job 1 for wide format, Job 2 for narrow format)
        #
        jobs_out = None
        delay = 1
        while True:
            jobs_out = requests.get(F_EDGE_URL + 'api/1.1/ingestjobs/1', headers={'Accept': 'application/x-ndjson'})
            if not jobs_out or jobs_out.status_code != 200:
                time.sleep(delay)
                delay = (delay + 1) % 60
                Utils.info(
                    "Is the edge not ready?  '" + F_EDGE_URL + "api/1.1/ingestjobs/1' request did not return results")
                Utils.info("Response is :" + str(jobs_out))
                continue
            else:
                break

        job = jobs_out.json()
        Utils.info("Job : " + str(job))
        xp_map = {}
        xp_columns = []
        #
        # The signal names to ids are stored in the "links" object
        #
        count = 0
        for item in job["links"]:
            Utils.info("item : " + str(item))
            if item["rel"] == "signal":
                hr = item["href"]
                sid = hr[hr.rfind('/') + 1:]
                s_nm = item["signalIdentifier"]
                xp_columns.append(s_nm)
                xp_map[sid] = count
                count += 1

        Utils.info("XP_MAP :" + str(xp_map))
        self.columnsInfo.set_explanation_output_column_info(xp_map, xp_columns)

    def get_next_chunk_of_data(self):
        #
        # Data sent to Falkonry MUST be sorted in increasing time order.
        #   Once data at time T is processed in Falkonry live monitoring, if you pass data with timestamp less than T,
        #   that data will be ignored and no assessment will be produced for that data.
        #   The Live Monitoring maintains the last time T it processed.
        #   If you want to process data before time T, you have to stop Live Monitoring process and restart.
        #   At this point you can restart sending data.
        #
        # <Customer:TODO> Columns have to be in the same order as that of the header line.
        # <Customer:TODO> Each line has to be terminated by '\n' character
        # <Customer:TODO> Read a chunk of data from datasource.  It could be a few thousand lines from data source.
        #
        lines = []
        for nI in range(0, FalkonryInputProcessor.CHUNK_SIZE):
            line = self.datasource.readline()
            if len(line) == 0:
                break
            else:
                # Ignore empty lines...
                if (len(line) == 1 and line.endswith('\n')) or (len(line) == 2 and line.endswith('\r\n')):
                    continue
                lines.append(line)

        return lines

    def run(self):
        """
        Thread method for streaming data to Falkonry Live Monitoring or Edge Analyzer Process
        """
        Utils.info('input_thread')
        # README
        # Data sent to Falkonry MUST be sorted in increasing time order [for each entity].
        #   [For each entity] Once data at time T is processed in Falkonry live monitoring, if you pass data with timestamp less than T,
        #   that data will be ignored and no assessment will be produced for that data [entity].
        #   The Live Monitoring maintains the last time T it processed [for each entity].
        #   If you want to process data before time T, you have to stop Live Monitoring process and restart.
        #   At this point you can restart sending data.
        #
        #   Using REST API you can send data to Falkonry Edge Analyzer in multiple ways:
        #   1. One or more rows of timestamps + all model signals data in CSV format (Wide CSV format)
        #       This is the ONLY format this example demonstrates
        #   2. One or more rows of timestamps + each signal's data in CSV format (Narrow CSV format)
        #       You can use this format when each signal's data is available in real time at different sampling rates.
        #       This is ideal method when you are not sure of the sampling interval.
        #   3. One or more rows of timestamps + all model signals data in JSON format (Wide JSON format)
        #       This is JSON format you can use when all signals data is available for sending to Edge Analyzer
        #   4. One or more rows of timestamps + each signal's data in JSON format (Narrow JSON format)
        #       You can use this format when each signal's data is available in real time at different sampling rates.
        #       This is ideal method when you are not sure of the sampling interval.

        #
        # Example of data being sent to Falkonry
        # String data = "time, entity, signal1, signal2, signal3, signal4" + "\n"
        #    + "1467729675422, entity1, 41.11, 62.34, 77.63, 4.8" + "\n"
        #    + "1467729675445, entity1, 43.91, 82.64, 73.63, 3.8"
        http_headers = {'content-type': 'text/csv'}
        Utils.info('input_job_id is : ' + str(self.inputJobId))
        no_data_sleep_time = 1
        total_count = 0
        records_per_second = 100 if (self.feedRate <= 0 or self.feedRate is None) else self.feedRate
        pause_time = 1.0
        if records_per_second < 1.0 and records_per_second > 0.0:
            pause_time = 1.0 / records_per_second
            records_per_second = 1
        elif records_per_second - int(records_per_second) > 0.0:
            records_per_second = int(records_per_second)

        while True:
            #
            # Get next chunk of data from datasource
            #
            lines = self.get_next_chunk_of_data()
            size = len(lines)
            total_count += size
            if size == 0:
                Utils.info(
                    "No more data from get_next_chunk_of_data.  Sleeping " + str(no_data_sleep_time) + " seconds.")
                time.sleep(no_data_sleep_time)
                no_data_sleep_time += no_data_sleep_time
                no_data_sleep_time = 120 if no_data_sleep_time > 120 else no_data_sleep_time
                continue

            bucket = int(records_per_second)
            Utils.info("Bucket size is : " + str(bucket))
            for nI in range(0, size, bucket):
                data = self.columnsInfo.headerLine
                #
                # Send a bucket of data at a time
                # Concatenate a string.
                #
                first = None
                for nJ in range(nI, (nI + bucket), 1):
                    if nJ < size:
                        first = lines[nJ] if first is None else first
                        data += lines[nJ]

                # Utils.info(data)
                Utils.info("Sending " + str(nI) + " to " + str(nI + bucket) + " records to Edge.")
                trials = 1
                while True:
                    try:
                        inputResponse = requests.post(F_EDGE_URL + 'api/1.1/ingestjobs/' + self.inputJobId + '/inputs',
                                                      auth=None, data=data, verify=False, headers=http_headers)

                        Utils.info(str(inputResponse.json()))
                        break
                    except:
                        Utils.err_msg(sys.exc_info())
                        if trials > 1:
                            Utils.err_msg("Exception during sending data to Edge Analyzer. Iteration #:" + str(trials))
                            if trials > 100:
                                Utils.info("Giving up after 100 trials for this data. Moving on to next.")
                                break
                        time.sleep(1)
                        trials += 1
                        pass

                no_data_sleep_time = 1
                time.sleep(pause_time)
            Utils.info("Total lines sent for processing so far -- " + str(total_count))


def setup_parser():
    """
    Command line parser setup method.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--url", dest='url', required=True,
                        help="Falkonry Edge URL")
    parser.add_argument("-i", "--input_file", dest='input', required=True,
                        help="Input data file to feed into Falkonry Edge Analyzer")
    parser.add_argument("-o", "--output_file", dest='output', required=True,
                        help="File name to write Falkonry Edge Analyzer output")
    parser.add_argument("-t", "--time_column", dest='time', type=int, required=True,
                        help="Time column index starting with 0")
    parser.add_argument("-z", "--time_zone", dest='zone', required=True,
                        help="Time zone. Use the 'TZ database name OR UTC offset' from https://en.wikipedia.org/wiki/List_of_tz_database_time_zones. If its not supported use one from supported list that matches the time zone.")
    parser.add_argument("-f", "--time_format", dest='format', required=True,
                        help="Timestamp format. See https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html for format details.")
    parser.add_argument("-e", "--entity_column", dest='entity', type=int,
                        help="Entity column index starting with 0")
    parser.add_argument("-b", "--batch_column", dest='batch', type=int,
                        help="Batch column index starting with 0")
    parser.add_argument("-r", "--input_feed_rate", dest='rate', type=float, default=1000,
                        help="Number of records to send to edge per second.")

    return parser


def main():
    """
    Setup the Falkonry connection parameters.
    Launch a thread for sending signal data to Falkonry.
    Launch a thread to get the assessment output.
    """
    parser = setup_parser()
    args = parser.parse_args()

    global F_EDGE_URL
    F_EDGE_URL = args.url
    input_file = args.input
    output_file = args.output

    Utils.info("Input file " + input_file)
    Utils.info("Output file " + output_file)

    #
    # All 3 arguments are required
    #
    regex = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    if F_EDGE_URL is None or input_file is None or output_file is None:
        Utils.err_msg("One or more arguments missing!!")
        parser.print_help()
        return

    if not os.path.isfile(input_file):
        Utils.err_msg("File '" + input_file + "' does not exist!!")
        parser.print_help()
        return

    # if os.path.isfile(output_file):
    #    Utils.err_msg("File '" + output_file + "' already exists!!")
    #    parser.print_help()
    #    return

    if not re.match(regex, F_EDGE_URL):
        Utils.err_msg("Invalid URL : " + F_EDGE_URL)
        parser.print_help()
        return

    if args.time is None or args.time < 0:
        Utils.err_msg("Invalid time column index : " + args.time)
        parser.print_help()
        return

    if F_EDGE_URL[-1] != '/':
        F_EDGE_URL += '/'

    Utils.info("url:" + F_EDGE_URL + ", input_file:" + input_file + ", output_file:" + output_file)
    Utils.info(str(args))

    ot_count = threading.activeCount()
    Utils.info("Active thread count " + str(ot_count))
    #
    # Start a thread to stream data to Falkonry Edge Analyzer
    #
    input_processor = FalkonryInputProcessor(args)
    input_processor.name = "InputProcessor"
    input_processor.start()
    input_processor.print_model_info()
    #
    # Start a thread to get assessment output from Falkonry
    #
    Utils.info("Done waiting for headers to be prepared...")

    output_processor = FalkonryOutputProcessor(input_processor.columnsInfo, args.output)
    output_processor.name = "OutputProcessor"
    output_processor.start()

    #
    # Wait until both threads exit or Ctrl+C is pressed
    #
    while True:
        try:
            if input_processor.is_alive() and output_processor.is_alive():
                Utils.info("Processing...")
            else:
                if not input_processor.is_alive():
                    Utils.err_msg("Input Thread is NOT active.  Something wrong.")
                if not output_processor.is_alive():
                    Utils.err_msg("Output Thread is NOT active.  Something wrong.")
            time.sleep(10)
        except KeyboardInterrupt:
            #
            # Ctrl+C is pressed.
            #
            Utils.info("Caught Keyboard Interrupt")
            break


if __name__ == '__main__':
    if sys.version_info[0] < 3:
        print("Requires python 3 or later")
        sys.exit()
    main()

