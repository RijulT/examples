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
This is a *simple example* of "Falkonry Live Monitoring" script.
It reads a CSV file for input data. It saves the condition, confidence and explanation factors in an output file.
This code assumes a wide format CSV file for input.  Although Falkonry supports other formats and data ingestion methods,
this code does not illustrate those methods.

Assumptions:
* You only have default entity in your datastream
* This script does not support historical data ingestion; just live monitoring
* This script can only monitor one assessment for live monitoring.
* You are supplying data to Falkonry in csv format.
* Output will be saved to csv file.
"""

import sys
import threading
import time
import datetime
import argparse
import os
import os.path
import io
import requests
import ndjson
import re
import json

#
# Treat these as constants
#
EXCEPTION_LIMIT = 100
CHUNK_SIZE = 10000
F_EDGE_URL = 'http://192.168.2.2:9004/'

class Headers:
    header = None
    timeColumnIndex = None
    entityColumnIndex = None
    batchColumnIndex = None
    headerColumns = None
    xplanationColumns = None

    @staticmethod
    def file_header(ds_file, timeColumnIndex, entityColumnIndex=None, batchColumnIndex=None):
        if (Headers.header is None):
            Headers.header = ds_file.readline()
            Headers.header = Headers.header.rstrip()
            Headers.timeColumnIndex = timeColumnIndex
            Headers.entityColumnIndex = entityColumnIndex
            Headers.batchColumnIndex = batchColumnIndex
        return Headers.header

    @staticmethod
    def signals():
        result = []
        columns = Headers.header.split(',') if Headers.header else []
        for nI in range(0, len(columns)):
            if (not (nI == Headers.timeColumnIndex or nI == Headers.entityColumnIndex or nI == Headers.batchColumnIndex)):
                result.append(columns[nI])

        return result

    '''
    The signals used in the model may be less than input signals supplied.
    '''
    @staticmethod
    def setExplanationOutputColumns(xp_columns):
        Headers.xplanationColumns = xp_columns

    @staticmethod
    def file_columns():
        Headers.headerColumns = Headers.header.split(',')

    @staticmethod
    def column(index):
        if (Headers.headerColumns is None):
            Headers.file_columns()

        return None if index is None else Headers.headerColumns[index]

    @staticmethod
    def output_header():
        ent = "" if Headers.entityColumnIndex is None else "entity,"
        bch = "" if Headers.batchColumnIndex is None else "batch,"
        return "time," + ent + bch + "condition,confidence," + ",".join(Headers.xplanationColumns) + "\n"


class FalkonryOutput:
    def __init__(self):
        self.entity = None
        self.batch = None
        self.time = None
        self.condition = None
        self.confidence = None
        self.explanations = [None for nI in Headers.xplanationColumns]

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
        if (Headers.entityColumnIndex is not None):
            self.entity = j_obj['entity']
        if (Headers.batchColumnIndex is not None):
            self.batch = j_obj['batch']

def info(msg):
    """
    Log method
    """
    ts = str(datetime.datetime.now())
    print("INFO:" + ts + " : " + str(msg), flush=True)


def err_msg(msg):
    """
    Error log method
    """
    ts = str(datetime.datetime.now())
    print("ERROR:" + ts + " : " + str(msg)) #, flush=True)


def populate_explanations(x_out, xp_map, output_map, ts_list, ignore_first=False):
    r_index = None
    out_a = x_out.json(cls=ndjson.Decoder)
    isFirst = True
    for item in out_a:
        if (ignore_first and isFirst):
            isFirst = False
            continue
        oitem = output_map.get(item['time'])
        if (oitem == None):
            oput = FalkonryOutput()
            oput.set_entity_batch(item)
            oput.time = item['time']
            oput.explanations[xp_map[item['signal']]] = item['score']
            output_map[oput.time] = oput

            ts_list.append({"ts":oput.time, "complete":0})
        else:
            oitem.explanations[xp_map[item['signal']]] = item['score']

        r_index = item['index']
    return r_index


def populate_confidences(conf_out, xp_map, output_map, ts_list, ignore_first=False):
    r_index = None
    out_a = conf_out.json(cls=ndjson.Decoder)
    isFirst = True
    for item in out_a:
        #info("Confidencess  ==" + str(item))
        if (ignore_first and isFirst):
           isFirst = False
           continue
        oitem = output_map.get(item['time'])
        if (oitem == None):
            oput = FalkonryOutput()
            oput.set_entity_batch(item)
            oput.time = item['time']
            oput.confidence = item['value']
            output_map[oput.time] = oput
            ts_list.append({"ts":oput.time, "complete":0})
        else:
            oitem.confidence = item['value']
        r_index = item['index']
    return r_index


def populate_assessments(cond_out, xp_map, output_map, ts_list, ignore_first=False):
    r_index = None
    out_a = cond_out.json(cls=ndjson.Decoder)
    isFirst = True
    for item in out_a:
        if (ignore_first and isFirst):
            isFirst = False
            continue
        oitem = output_map.get(item['time'])
        if (oitem == None):
            oput = FalkonryOutput()
            oput.set_entity_batch(item)
            oput.time = item['time']
            oput.condition = item['value']
            output_map[oput.time] = oput
            ts_list.append({"ts":oput.time, "complete":0})
        else:
            oitem.condition = item['value']

        r_index = item['index']
    return r_index


def map_explanation_scores_to_signals():
    columns = Headers.signals()
    info("Columns : " + str(columns))
    #
    # Signal IDs/Names would be the same no matter what job.  Using Job 1 to retrieve the map
    #
    jobs_out = requests.get(F_EDGE_URL + 'api/1.0/ingestjobs/1', headers={'Accept': 'application/x-ndjson'})
    job = jobs_out.json()
    info("Job : " + str(job))
    xp_map = {}
    xp_columns = []
    #
    # The signal names to ids are stored in the "links" object
    #
    count = 0
    for item in job["links"]:
        info("item : " + str(item))
        if (item["rel"] == "signal"):
            hr = item["href"]
            sid = hr[hr.rfind('/')+1:]
            s_nm = item["signalIdentifier"]
            xp_columns.append(s_nm)
            xp_map[sid] = count
            count += 1

    info("XP_MAP :" + str(xp_map))
    return (xp_map, xp_columns)


def output_thread(output_file):
    info("Output file " + output_file)
    """
    Assessment output polling method.
    This method does not need to be modified unless you have entities.
    """
    (xp_map, xp_columns) = map_explanation_scores_to_signals()
    Headers.setExplanationOutputColumns(xp_columns)
    signal_count = len(xp_map)
    #
    # Timestamps in order
    #
    ts_list = []
    data_sync = open(output_file, 'w')
    data_sync.write(Headers.output_header())
    output_map = {}
    info('output_thread')
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
                a_out = requests.get(F_EDGE_URL + 'api/1.0/outputs/assessments', {'limit':1},
                                     headers=accept)
                x_out = requests.get(F_EDGE_URL + 'api/1.0/outputs/explanations', {'limit':signal_count},
                                     headers=accept)
                c_out = requests.get(F_EDGE_URL + 'api/1.0/outputs/confidences', {'limit':1},
                                     headers=accept)
                if (a_out != None and x_out != None and c_out != None):
                    a_index = populate_assessments(a_out,xp_map,output_map,ts_list)
                    x_index = populate_explanations(x_out,xp_map,output_map,ts_list)
                    c_index = populate_confidences(c_out,xp_map,output_map,ts_list)
                    info( "First a_index " + str(a_index) + " x_index " + str(x_index) + " c_index " + str(c_index))
                    if (a_index == None or x_index == None or c_index == None):
                        isFirst = True
                        time.sleep(1)
                    else:
                        isFirst = False
                else:
                    time.sleep(1)
            else:
                a_index0 = populate_assessments(requests.get(F_EDGE_URL + 'api/1.0/outputs/assessments',
                                     {'offsetType':'index', 'offset':a_index},
                                     headers=accept), xp_map, output_map,ts_list,True)
                x_index0 = populate_explanations(requests.get(F_EDGE_URL + 'api/1.0/outputs/explanations',
                                     {'offsetType':'index', 'offset':x_index, 'limit':signal_count*50},
                                     headers=accept), xp_map, output_map,ts_list,True)
                c_index0 = populate_confidences(requests.get(F_EDGE_URL + 'api/1.0/outputs/confidences',
                                     {'offsetType':'index', 'offset':c_index},
                                     headers=accept), xp_map, output_map,ts_list,True)
                if (a_index0 is not None):
                    a_index = a_index0
                if (x_index0 is not None):
                    x_index = x_index0
                if (c_index0 is not None):
                    c_index = c_index0

                info( "Next a_index0 " + str(a_index0) + " x_index0 " + str(x_index0) + " c_index0 " + str(c_index0))
                #
                #Output Completed data
                #
                for nI in range(0,len(ts_list),1):
                    ts = ts_list[nI]
                    if (output_map.get(ts['ts']).is_complete()):
                        ts['complete'] = 1
                        data_sync.write(output_map.get(ts['ts']).to_string())
                        data_sync.flush()
                    else:
                        info("Incomplete !!: " + output_map.get(ts['ts']).to_string())
                        break

                for nI in range(len(ts_list)-1, -1, -1):
                    if (ts_list[nI]['complete'] == 1):
                        del ts_list[nI]
                        o_count += 1

                info("Total output assessments = " + str(o_count) + "  Incomplete list size : " + str(len(ts_list)))
                time.sleep(1)
            #
            #  TODO Handle the output properly
            #
        except:
            x_counter += 1
            traceback.print_stack()
            time.sleep(5)
            #if (x_counter >= 10):
            #   break


def get_next_chunk_of_data(datasource):
    #
    # Data being sent to Falkonry MUST be sorted in increasing time order.
    #   Once data at time T is processed in Falkonry live monitoring, if you pass data with timestamp less than T,
    #   that data will be ignored and no assessment will be produced for that data.
    #   The Live Monitoring maintains the last time T it processed.
    #   If you want to process data before time T, you have to stop Live Monitoring process and restart.
    #   At this point you can restart sending data.
    #
    #<Customer:TODO> Columns have to be in the same order as that of the header line.
    #<Customer:TODO> Each line has to be terminated by '\n' character
    #<Customer:TODO> Read a chunk of data from datasource.  It could be a few thousand lines from data source.
    #
    lines = []
    for nI in range(0,CHUNK_SIZE):
        line = datasource.readline()
        if line == '':
            break
        else:
            lines.append(line)

    return lines


def create_edge_input_job(time_column, time_format, time_zone, entity_column=None, batch_column=None):
    #if (entity_column is None):
    #    return "1"
    #else:
    http_headers = {'content-type': 'application/json'}
    data = {
       "type": "Ingest",
       "timeIdentifier": time_column,    #"timestamp",
       "timeFormat": time_format,        #"micros",
       "timeZone": time_zone             #"Europe/Stockholm",
    }
    if (entity_column is not None):
        data["entityIdentifier"] = entity_column
    if (batch_column is not None):
        data["batchIdentifier"] = batch_column

    info("Create_Edge_input_job  :" + str(data))
    inputResponse = requests.post(F_EDGE_URL + 'api/1.0/ingestjobs', auth=None,
                                  data=json.dumps(data), verify=False, headers=http_headers)
    job = inputResponse.json()
    info("Input job : " + str(job))
    info("Input job id : " + job["id"])
    return job["id"]


def input_thread(args=None):
    info("Input file " + args.input)
    """
    Thread method for streaming data to Falkonry Live Monitoring Process
    """
    info('input_thread')
    datasource = open(args.input, "r")
    #
    # Replace the string with the SAME column headers used in Falkonry datastream
    #
    header = Headers.file_header(datasource, args.time, args.entity, args.batch)
    header += "\n"

    # README
    # Data being sent to Falkonry MUST be sorted in increasing time order [for each entity].
    #   [For each entity] Once data at time T is processed in Falkonry live monitoring, if you pass data with timestamp less than T,
    #   that data will be ignored and no assessment will be produced for that data [entity].
    #   The Live Monitoring maintains the last time T it processed [for each entity].
    #   If you want to process data before time T, you have to stop Live Monitoring process and restart.
    #   At this point you can restart sending data.
    #

    #
    # Example of data being sent to Falkonry
    # String data = "time, entity, signal1, signal2, signal3, signal4" + "\n"
    #    + "1467729675422, entity1, 41.11, 62.34, 77.63, 4.8" + "\n"
    #    + "1467729675445, entity1, 43.91, 82.64, 73.63, 3.8"
    http_headers = {'content-type': 'text/csv'}
    #
    # Create an input job if the default job is not suitable
    #
    input_job_id = create_edge_input_job(Headers.column(args.time), args.format,
                          args.zone, Headers.column(args.entity), Headers.column(args.batch)) # Get the input job id
    no_data_sleep_time = 1
    total_count = 0
    records_per_second = 1000 if (args.rate < 0 or args.rate is None) else args.rate
    while True:
        #
        # Get next chunk of data from datasource
        #
        lines = get_next_chunk_of_data(datasource)
        size = len(lines)
        total_count += size
        if (size == 0):
            info("No more data from get_next_chunk_of_data.  Sleeping " + str(no_data_sleep_time) + " seconds.")
            time.sleep(no_data_sleep_time)
            no_data_sleep_time += no_data_sleep_time
            no_data_sleep_time = 120 if no_data_sleep_time > 120 else no_data_sleep_time
            continue

        bucket = records_per_second
        info("Bucket size is : " + str(bucket))
        for nI in range(0,size,bucket):
            counter = 0
            data = header
            #
            # Send a bucket of data at a time
            # Concatenate a string.
            #
            first = None
            last = None
            for nJ in range(nI,(nI+bucket),1):
                if (nJ < size):
                    first = lines[nJ] if first is None else first
                    last = lines[nJ]
                    data += lines[nJ]

            #info(data)
            info("Sending " + str(nI) + " to " + str(nI+bucket) + " records to Edge.")
            try:
                inputResponse = requests.post(F_EDGE_URL + 'api/1.0/ingestjobs/' + input_job_id + '/inputs', auth=None,
                                              data=data, verify=False, headers=http_headers)

                info(str(inputResponse.json()))
            except:
                #
                # Put code to retry
                #
                err_msg(sys.exc_info())
                err_msg("Exception caught...")
                return

            no_data_sleep_time = 1
            time.sleep(1)
        info("Total lines sent for processing so far -- " + str(total_count))


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
                help="Time zone")
    parser.add_argument("-f", "--time_format", dest='format', required=True,
                help="Timestamp format")
    parser.add_argument("-e", "--entity_column", dest='entity', type=int,
                help="Entity column index starting with 0")
    parser.add_argument("-b", "--batch_column", dest='batch', type=int,
                help="Batch column index starting with 0")
    parser.add_argument("-r", "--input_feed_rate", dest='rate', type=int, default=1000,
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
    time_index = args.time
    time_zone = args.zone
    time_format = args.format

    info("Input file " + input_file)
    info("Output file " + output_file)

    #
    # All 3 arguments are required
    #
    regex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    if (F_EDGE_URL == None or input_file == None or output_file == None):
        err_msg("One or more arguments missing!!")
        parser.print_help()
        return

    if (not os.path.isfile(input_file)):
        err_msg("File '" + input_file + "' does not exist!!")
        parser.print_help()
        return

    #if (os.path.isfile(output_file)):
    #    err_msg("File '" + output_file + "' already exists!!")
    #    parser.print_help()
    #    return

    if (not re.match(regex, F_EDGE_URL)):
        err_msg("Invalid URL : " + F_EDGE_URL)
        parser.print_help()
        return

    if (args.time is None or args.time < 0):
        err_msg("Invalid time column index : " + args.time)
        parser.print_help()
        return

    if (F_EDGE_URL[-1] != '/'):
        F_EDGE_URL += '/'

    info("url:" + F_EDGE_URL + ", input_file:" + input_file + ", output_file:" + output_file)
    info(str(args))

    ot_count = threading.activeCount()
    info("Active thread count " + str(ot_count))
    #
    # Start a thread for streaming data to Falkonry
    #
    i_thread = threading.Thread(target=input_thread, name="InputThread", kwargs={"args" : args})
    i_thread.start()
    #
    # Start a thread for getting assessment output from Falkonry
    #
    info("Waiting for headers to be prepared...")
    while (Headers.header is None and len(Headers.signals()) == 0):
        time.sleep(1)
    info("Done waiting for headers to be prepared...")

    o_thread = threading.Thread(target=output_thread, name="OutputThread", args=[output_file])
    o_thread.start()

    #
    # Wait until both threads exit or Ctrl+C is pressed
    #
    while True:
        try:
            if (i_thread.is_alive() and o_thread.is_alive()):
                info("Processing...")
            else:
                if (not i_thread.is_alive()):
                    err_msg("Input Thread is NOT active.  Something wrong.")
                if (not o_thread.is_alive()):
                    err_msg("Ouput Thread is NOT active.  Something wrong.")
            time.sleep(10)
        except KeyboardInterrupt:
            #
            # Ctrl+C is pressed.
            #
            info("Caught Keyboard Interrupt")
            break


if __name__ == '__main__':
    if sys.version_info[0] < 3:
        print ("Requires python 3 or later")
        sys.exit()
    main()
