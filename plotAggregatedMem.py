# Author: Habib Sabiu
# Date: March 31, 2017
# Purpose: Script to plot aggregated memory load logs generated by Ganglia
# Copyright: Any person can adopt this script to their specific need

from matplotlib.font_manager import FontProperties
from datetime import datetime, timedelta

import csv
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

with open('aggMemory.csv', 'rb') as f:
    reader = csv.reader(f)
    data_list = list(reader)

time_stamp_list = []
total_free_memory_list = []
total_memory_list = []
total_used_memory_list = []

for index, line in enumerate(data_list):
    if index > 0:
        time_stamp = line[0]
        total_free_memory = float(line[1]) + float(line[3]) + float(line[5]) + float(line[7]) + float(line[9]) + float(line[11]) + float(line[13]) + float(line[15]) + float(line[17]) + float(line[19]) + float(line[21])
        total_memory = float(line[2]) + float(line[4]) + float(line[6]) + float(line[8]) + float(line[10]) + float(line[12]) + float(line[14]) + float(line[16]) + float(line[18]) + float(line[20]) + float(line[22])
        total_used_memory = total_memory - total_free_memory

        time_stamp_list.append(datetime.strptime(time_stamp[0:19], "%Y-%m-%dT%H:%M:%S"))
        total_free_memory_list.append(int(total_free_memory / 1000000))
        total_memory_list.append(int(total_memory / 1000000))
        total_used_memory_list.append(int(total_used_memory / 1000000))

fig, ax = plt.subplots()
plt.title("Image format conversion - YARN", fontsize=20)
plt.ylabel("Memory (GB)", fontsize=15)
plt.xlabel("Timestamp", fontsize=15)
#plt.xlim([datetime(2017, 02, 01, 14, 22, 00), datetime(2017, 02, 01, 14, 35, 00)])
plt.ylim([-5, 250])

#ax.xaxis.set_major_locator(mdates.SecondLocator(interval=60))
ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:S'))

ax.plot_date(time_stamp_list, total_memory_list, fmt="r-", label="total memory", )
#ax.plot_date(time_stamp_list, total_free_memory_list, fmt="g-.", label="free memory", )
ax.plot_date(time_stamp_list, total_used_memory_list, fmt="b-*", label="used memory")

# font of the legend
fontP = FontProperties()
fontP.set_size('medium')

ax.legend(loc='upper right', shadow=False, ncol=3, prop=fontP)
ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
ax.grid(True)
fig.autofmt_xdate()
plt.show()
