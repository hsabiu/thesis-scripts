# Author: Habib Sabiu
# Date: March 31, 2017
# Purpose: Script to plot nice graphs for comparing jobs running time
#          between running spark applications with and without speculation
#          using different number of tasks. It also compare reading raw
#          HDFS file and creating Hadoop Archieve File (har) and reading
#          the created .har file for processing
# Copyright: Any person can adopt this script for their specific need

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

raw_data = {'job_name': ['48 tasks', '98 tasks', '294 tasks', '588 tasks'],
            'har no speculation': [128, 274, 216, 184],
            'har speculation': [108, 286, 250, 208],
            'dir no speculation': [192, 304, 296, 284],
            'dir speculation': [116, 354, 336, 314]}

df = pd.DataFrame(raw_data, columns=['job_name', 'har no speculation', 'har speculation', 'dir no speculation', 'dir speculation'])

# Setting the positions and width for the bars
pos = list(range(len(df['har no speculation'])))
width = 0.2

# Plotting the bars
fig, ax = plt.subplots(figsize=(10, 5))

plt.bar(pos, df['har no speculation'], width, alpha=0.5, color='#4C2C10', hatch="/")
plt.bar([p + width for p in pos], df['har speculation'], width, alpha=0.5, color='#140000', hatch="x")
plt.bar([p + width * 2 for p in pos], df['dir no speculation'], width, alpha=0.5, color='#000000', hatch=".")
plt.bar([p + width * 3 for p in pos], df['dir speculation'], width, alpha=0.5, color='#332200', hatch="o")

ax.set_ylabel('Job Completion Time (s)', fontsize=15)
#ax.set_title('Image Format Conversion', fontsize=20)
ax.set_xticks([p + 2 * width for p in pos])
#ax.set_yticks(100)
ax.set_xticklabels(df['job_name'], fontsize=15)

#plt.xlim(min(pos) - width, max(pos) + width * 4)
# plt.ylim([0, max(df['standalone'] + df['mesos'] + df['yarn'])] )
plt.xlim(-0.2, 4)
plt.ylim([0, 420])

fontP = FontProperties()
fontP.set_size('medium')

plt.legend(['har no speculation', 'har speculation', 'dir no speculation', 'dir speculation'], loc='upper left', shadow=False, prop=fontP)
plt.grid()
plt.show()

"""
raw_data = {'job_name': ['48 tasks', '98 tasks', '294 tasks', '588 tasks'],
            'har no speculation': [132, 192, 210, 246],
            'dir no speculation': [192, 276, 276, 288],
            'har speculation': [114, 240, 252, 282],
            'dir speculation': [144, 294, 312, 336]}

raw_data = {'job_name': ['48 tasks (no shuffle)', '98 tasks', '294 tasks', '588 tasks', '833 tasks'],
            'dir no speculation': [120, 1440, 1680, 468, 1320],
            'dir speculation': [96, 432, 552, 438, 432],
            'har no speculation': [132, 198, 216, 186, 234],
            'har speculation': [102, 246, 264, 348, 228]}

raw_data = {'job_name': ['48 tasks (no shuffle)', '98 tasks', '294 tasks', '588 tasks', '833 tasks'],
            'dir no speculation': [192, 276, 276, 288, 288],
            'dir speculation': [144, 294, 312, 336, 360],
            'har no speculation': [132, 192, 210, 246, 204],
            'har speculation': [114, 240, 252, 282, 237]}
"""