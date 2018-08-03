# Author: Habib Sabiu
# Date: March 31, 2017
# Purpose: Script to process Spark log files generated when running Spark applications on the
#          Mesos cluster manager. This script would loop through all the log files in the
#          current directory and get the timestamp for all events specified within 'events' list.
#          Basically, the events of interest here are the number of executors added or remove
#          from an application over time. The x-axis shows the elapsed time while the y-axis
#          shows the count of the executors used by the application at the given time stamp.
#          The script will only process and compare the log files of 5 applications at a time
# Copyright: Any person can adopt this script for their specific need as long as credit is given
#            to the initial author


from datetime import datetime
from itertools import groupby

from matplotlib import ticker
from matplotlib.font_manager import FontProperties

import tzlocal
import re

import matplotlib.pyplot as plt
import dateutil
import glob
import json
import os

eventsFileName = "events_log_record.txt"
events = ["SparkListenerApplicationStart", "SparkListenerExecutorAdded", "SparkListenerExecutorRemoved", "SparkListenerApplicationEnd"]

local_timezone = tzlocal.get_localzone()
intermediateLines = []

x_axis = []
y_axis = []

eventLine = ""
currentFileName = ""

# Get all file names in the current working directory
all_files = glob.glob('./*')

# Filter the files that have .txt, .py, .png, .jpg extension.
# This leaves us with only the log files which don't have any extension
log_files_name = [file for file in all_files if not file.endswith(('.txt', '.py', '.png', '.jpg', '.json'))]

for fileName in log_files_name:
    # A list to hold the parsed json
    parsedLogFile = []
    with open(fileName) as logFile:
        fileName = fileName.replace("./", "")
        for jsonLine in logFile:
            # Parse each json line by line
            parsedLogFile.append(json.loads(jsonLine))
            # Print the parsed json with indentation of 4
        jsonFormatted = json.dumps(parsedLogFile, indent=4, sort_keys=True)

        # Add .json format to the original log filename
        jsonFormatedLogFileName = fileName + ".json"

        # Delete the json file if it already exist in the directory
        try:
            os.remove(jsonFormatedLogFileName)
        except OSError:
            pass

        # Save the a copy of the original logfile as a formatted json file
        f = open(jsonFormatedLogFileName, 'w')
        print >> f, 'Filename:', jsonFormatted
        f.close()

json_file_names = glob.glob('./*.json')

for fileName in json_file_names:
    with open(fileName) as logFile:
        fileName = fileName.replace("./", "")
        fileName = fileName.replace(".json", "")
        for line in logFile:
            if any(s in line for s in events):
                eventLine = "\"App\":" + fileName + " " + line.replace(",", "").strip()
                currentFileName = fileName
            if "Timestamp" in line and eventLine != "" and currentFileName == fileName:
                intermediateLines.append(eventLine)
                intermediateLines.append("\"App\":" + fileName + " " + line.replace(",", "").strip())
                eventLine = ""

executorsCounter = 0
previousAppName = ""

listLength = len(intermediateLines)

with open(eventsFileName, 'w') as eventsFile:
    for index, line in enumerate(intermediateLines):
        if "Event" in line:
            if not index + 3 > listLength:
                currentEventTime = intermediateLines[index + 1]
                nextEvent = intermediateLines[index + 2]
                nextEventTime = intermediateLines[index + 3]

                currentEventTimeList = re.findall(r'\S+', currentEventTime)
                nextEventTimeList = re.findall(r'\S+', nextEventTime)

                currentEventTimeStampFloat = float(currentEventTimeList[2])
                nextEventTimeStampFloat = float(nextEventTimeList[2])

                current_event_local_time = datetime.fromtimestamp(currentEventTimeStampFloat / 1000.0, local_timezone)
                next_event_local_time = datetime.fromtimestamp(nextEventTimeStampFloat / 1000.0, local_timezone)

                currentEventTimestampToLocal = str(current_event_local_time.strftime("%Y-%b-%d %H:%M:%S"))
                nextEventTimestampToLocal = str(next_event_local_time.strftime("%Y-%b-%d %H:%M:%S"))

                intermediateTimeStamp = current_event_local_time
                intermediateTimeStamp = str(intermediateTimeStamp.strftime("%Y-%b-%d %H:%M:%S"))

                appName = currentEventTimeList[0][6:]
                event = line[58:len(line) - 1]

                if previousAppName == "":
                    previousAppName = appName

                if appName == previousAppName:
                    if event == "SparkListenerApplicationStart":
                        executorsCounter = 0
                    elif event == "SparkListenerExecutorAdded":
                        executorsCounter += 1
                    elif event == "SparkListenerExecutorRemoved":
                        executorsCounter -= 1
                    elif event == "SparkListenerApplicationEnd":
                        executorsCounter = 0
                else:
                    previousAppName = ""
                    executorsCounter = 0
                    x_axis.append("")
                    y_axis.append("")

                lineToWrite = (line.replace(",", "").strip() + " Timestamp: " + currentEventTimeList[2] + " Current Time: " + currentEventTimestampToLocal + " Next Time: " + nextEventTimestampToLocal + " NumExecutors: " + str(executorsCounter)).replace("\"", "")
                y_axis.append(executorsCounter)
                x_axis.append(currentEventTimestampToLocal)
                eventsFile.write(lineToWrite + "\n")

                if "SparkListenerApplicationEnd" in nextEvent:
                    lineToWrite = (line.replace(",", "").strip() + " Timestamp: " + currentEventTimeList[2] + " Current Time: " + currentEventTimestampToLocal + " Next Time: " + nextEventTimestampToLocal + " NumExecutors: " + str(executorsCounter)).replace("\"", "")
                    y_axis.append(executorsCounter)
                    x_axis.append(nextEventTimestampToLocal)
                    eventsFile.write(lineToWrite + "\n")

            else:
                lastEventTime = intermediateLines[index + 1]
                lastEventTimeList = re.findall(r'\S+', lastEventTime)
                lastEventTimeStampFloat = float(lastEventTimeList[2])
                last_event_local_time = datetime.fromtimestamp(lastEventTimeStampFloat / 1000.0, local_timezone)
                lastEventTimestampToLocal = str(last_event_local_time.strftime("%Y-%b-%d %H:%M:%S"))

                lineToWrite = (line.replace(",", "").strip() + " Timestamp: " + lastEventTimeList[2] + " Current Time: " + lastEventTimestampToLocal + " Next Time: " + lastEventTimestampToLocal + " NumExecutors: 0").replace("\"", "")
                y_axis.append(0)
                x_axis.append(lastEventTimestampToLocal)
                eventsFile.write(lineToWrite + "\n")


timeStampList = [list(group) for k, group in groupby(x_axis, lambda x: x == "") if not k]
executorsList = [list(group) for k, group in groupby(y_axis, lambda x: x == "") if not k]

maxExecutorsList = []
maxTimestampInNumbersList = []

firstPlotTimeStamp = [dateutil.parser.parse(s) for s in timeStampList[0]]
firstPlotExecutors = executorsList[0]

firstPlotTimeInNumbers = [0]
firstTimeStamp = firstPlotTimeStamp[0]

for index in range(1, len(firstPlotTimeStamp)):
    subsequentTimeStamp = firstPlotTimeStamp[index]
    sec_difference = (subsequentTimeStamp - firstTimeStamp).total_seconds()
    firstPlotTimeInNumbers.append(sec_difference)

secondPlotTimeStamp = [dateutil.parser.parse(s) for s in timeStampList[1]]
secondPlotExecutors = executorsList[1]

secondPlotTimeInNumbers = []

for index in range(0, len(secondPlotTimeStamp)):
    subsequentTimeStamp = secondPlotTimeStamp[index]
    sec_difference = (subsequentTimeStamp - firstTimeStamp).total_seconds()
    secondPlotTimeInNumbers.append(sec_difference)

thirdPlotTimeStamp = [dateutil.parser.parse(s) for s in timeStampList[2]]
thirdPlotExecutors = executorsList[2]

thirdPlotTimeInNumbers = []

for index in range(0, len(thirdPlotTimeStamp)):
    subsequentTimeStamp = thirdPlotTimeStamp[index]
    sec_difference = (subsequentTimeStamp - firstTimeStamp).total_seconds()
    thirdPlotTimeInNumbers.append(sec_difference)

fourthPlotTimeStamp = [dateutil.parser.parse(s) for s in timeStampList[3]]
fourthPlotExecutors = executorsList[3]

fourthPlotTimeInNumbers = []

for index in range(0, len(fourthPlotTimeStamp)):
    subsequentTimeStamp = fourthPlotTimeStamp[index]
    sec_difference = (subsequentTimeStamp - firstTimeStamp).total_seconds()
    fourthPlotTimeInNumbers.append(sec_difference)

fifthPlotTimeStamp = [dateutil.parser.parse(s) for s in timeStampList[4]]
fifthPlotExecutors = executorsList[4]

fifthPlotTimeInNumbers = []

for index in range(0, len(fifthPlotTimeStamp)):
    subsequentTimeStamp = fifthPlotTimeStamp[index]
    sec_difference = (subsequentTimeStamp - firstTimeStamp).total_seconds()
    fifthPlotTimeInNumbers.append(sec_difference)


maxExecutorsList.append(max(firstPlotExecutors))
maxExecutorsList.append(max(secondPlotExecutors))
maxExecutorsList.append(max(thirdPlotExecutors))
maxExecutorsList.append(max(fourthPlotExecutors))
maxExecutorsList.append(max(fifthPlotExecutors))

maxExecutors = max(maxExecutorsList)

maxTimestampInNumbersList.append(max(firstPlotTimeInNumbers))
maxTimestampInNumbersList.append(max(secondPlotTimeInNumbers))
maxTimestampInNumbersList.append(max(thirdPlotTimeInNumbers))
maxTimestampInNumbersList.append(max(fourthPlotTimeInNumbers))
maxTimestampInNumbersList.append(max(fifthPlotTimeInNumbers))

maxTimestamp = max(maxTimestampInNumbersList)


fig, ax = plt.subplots()
plt.title("Mesos - Static default with 1 core, 1GB per executors", fontsize=15)
plt.ylabel("Number of executors allocated", fontsize=15)
plt.xlabel("Elapsed Time (s)", fontsize=15)
#plt.xlim([0, maxTimestamp + 20])
plt.ylim([0, maxExecutors + 20])
# Set the x-ticks interval
#ax.xaxis.set_major_locator(ticker.MultipleLocator(60))

plt.plot(firstPlotTimeInNumbers, firstPlotExecutors, "r-*", label="job_1", )
plt.plot(secondPlotTimeInNumbers, secondPlotExecutors, "b-o", label="job_2")
plt.plot(thirdPlotTimeInNumbers, thirdPlotExecutors, "g-^", label="job_3")
plt.plot(fourthPlotTimeInNumbers, fourthPlotExecutors, "kx-", label="job_4")
plt.plot(fifthPlotTimeInNumbers, fifthPlotExecutors, "c-d", label="job_5")

# font of the legend
fontP = FontProperties()
fontP.set_size('small')

ax.legend(loc='upper right', shadow=False, ncol=5, prop=fontP)
ax.grid(False)

#fig.autofmt_xdate()

plt.show()

