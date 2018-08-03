# Author: Habib Sabiu
# Date: March 31, 2017
# Purpose: Script to process Spark log files generated when running Spark applications on Mesos
#          cluster manager. This script would loop through all the log files in the current directory and
#          get the timestamp for all events specified within 'events' list. Basically, the events of interest
#          here are the number of executors added or remove from an application over time. The x-axis shows
#          the time stamp of the event while the y-axis shows the count of the executors used by the application
#          at the given time stamp. The script will only process and compare the log files of 5 applications at a time
# Copyright: Any person can adopt this script for their specific need as long as credit is given
#            to the initial author


from datetime import datetime, timedelta
from itertools import groupby
from matplotlib.font_manager import FontProperties

import tzlocal
import re

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import dateutil
import glob

eventsFileName = "events_log_record.txt"
events = ["SparkListenerApplicationStart", "SparkListenerExecutorAdded", "SparkListenerExecutorRemoved", "SparkListenerApplicationEnd"]

local_timezone = tzlocal.get_localzone()
intermediateLines = []

x_axis = []
y_axis = []

list_of_files = glob.glob('./*.json')

eventLine = ""
currentFileName = ""

for fileName in list_of_files:
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

firstPlotTimeStamp = [dateutil.parser.parse(s) for s in timeStampList[0]]
firstPlotExecutors = executorsList[0]

secondPlotTimeStamp = [dateutil.parser.parse(s) for s in timeStampList[1]]
secondPlotExecutors = executorsList[1]

thirdPlotTimeStamp = [dateutil.parser.parse(s) for s in timeStampList[2]]
thirdPlotExecutors = executorsList[2]

fourthPlotTimeStamp = [dateutil.parser.parse(s) for s in timeStampList[3]]
fourthPlotExecutors = executorsList[3]

fifthPlotTimeStamp = [dateutil.parser.parse(s) for s in timeStampList[4]]
fifthPlotExecutors = executorsList[4]

fig, ax = plt.subplots()
plt.title("Mesos - DRA with 10 maximum executors", fontsize=15)
plt.ylabel("Number of executors allocated", fontsize=15)
plt.xlabel("Timestamp", fontsize=15)
plt.xlim([datetime(2017, 02, 16, 10, 41, 50), datetime(2017, 02, 16, 10, 53, 50)])
#plt.ylim([-5, 100])
plt.ylim([-0.3, 12])


ax.xaxis.set_major_locator(mdates.SecondLocator(interval=60))

ax.plot_date(firstPlotTimeStamp, firstPlotExecutors, fmt="r-*", label="job_1", )
ax.plot_date(secondPlotTimeStamp, secondPlotExecutors, fmt="b-o", label="job_2")
ax.plot_date(thirdPlotTimeStamp, thirdPlotExecutors, fmt="g-^", label="job_3")
ax.plot_date(fourthPlotTimeStamp, fourthPlotExecutors, fmt="kx-", label="job_4")
ax.plot_date(fifthPlotTimeStamp, fifthPlotExecutors, fmt="c-d", label="job_5")

# font of the legend
fontP = FontProperties()
fontP.set_size('small')

ax.legend(loc='upper right', shadow=False, ncol=5, prop=fontP)
ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
ax.grid(False)
fig.autofmt_xdate()
plt.show()
