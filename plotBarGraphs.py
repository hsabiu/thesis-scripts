# Author: Habib Sabiu
# Date: March 31, 2017
# Purpose: Script to plot nice graphs for comparing jobs makespan between running
#          Spark applications with static and dynamic resource allocation modes,
#          using different number of executors, and resource size per executor
# Copyright: Any person can adopt this script for their specific need

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import LinearLocator

# Averaged image registration dynamic resource allocation data
raw_data = {'job_name': ['Default', '88 tasks', '176 tasks'],
            'YARN - 1core, 1G': [4560, 3690, 3450],
            'Standalone - 1core, 1G': [3510, 3900, 3420],
            'Mesos - 1core, 1G': [4020, 3540, 3420]}

df = pd.DataFrame(raw_data, columns=['job_name', 'YARN - 1core, 1G', 'Standalone - 1core, 1G', 'Mesos - 1core, 1G'])

# Setting the positions and width for the bars
pos = list(range(len(df['YARN - 1core, 1G'])))
width = 0.25

# Plotting the bars
fig, ax = plt.subplots(figsize=(14, 8))

plt.bar(pos, df['YARN - 1core, 1G'], width, alpha=0.5, color='#332200', hatch="/")
plt.bar([p + width for p in pos], df['Standalone - 1core, 1G'], width, alpha=0.5, color='#140000', hatch="x")
plt.bar([p + width * 2 for p in pos], df['Mesos - 1core, 1G'], width, alpha=0.5, color='#000000', hatch=".")

ax.set_title('Image Registration - Dynamic resource allocation', fontsize=25)

ax.set_xticklabels(df['job_name'], fontsize=20)
ax.set_xticks([p + 1 * width for p in pos])
#plt.xlim(-0.1, 3)

ax.set_ylabel('Makespan (s)', fontsize=20)
# Set y-axis limit
plt.ylim([0, 6000])
plt.yticks(fontsize=15)
# Set the y-ticks frequency to 10 evenly spaced values
ax.get_yaxis().set_major_locator(LinearLocator(numticks=7))

fontP = FontProperties()
fontP.set_size('x-large')

plt.legend(['YARN - 88 executors with 1core, 1G per executor', 'Standalone - 88 executors with 1core, 1G per executor', 'Mesos - 88 executors with 1core, 1G per executor'], loc='upper left', shadow=False, prop=fontP)
#plt.grid()
plt.show()

"""
# Averaged image registration static resource allocation data
raw_data = {'job_name': ['Default', '88 tasks', '176 tasks'],
            'YARN - 1core, 1G': [7230, 7350, 7440],
            'Standalone - 1core, 1G': [6360, 4380, 3810],
            'Mesos - 1core, 1G': [4290, 3660, 3450]}
"""