# Author: Habib Sabiu
# Date: March 31, 2017
# Purpose: Script to plot nice graphs for comparing jobs makespan between
#          running Spark applications on Standalone, Mesos or YARN
# Copyright: Any person can adopt this script for their specific need

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

raw_data = {'job_name': ['Default', '24 Max Executors', '48 Max Executors'],
            'Standalone - static': [584, 800, 512],
            'Standalone - dynamic': [512, 512, 440],
            'Mesos - static': [640, 602, 448],
            'Mesos - dynamic': [456, 470, 476],
            'YARN - static': [980, 446, 478],
            'YARN - dynamic': [506, 602, 560]}

df = pd.DataFrame(raw_data, columns=['job_name', 'Standalone - static', 'Standalone - dynamic', 'Mesos - static', 'Mesos - dynamic', 'YARN - static', 'YARN - dynamic'])

# Setting the positions and width for the bars
pos = list(range(len(df['Standalone - static'])))
width = 0.15

# Plotting the bars
fig, ax = plt.subplots(figsize=(13, 6))

plt.bar(pos, df['Standalone - static'], width, alpha=0.5, color='#4C2C10', hatch="/")
plt.bar([p + width for p in pos], df['Standalone - dynamic'], width, alpha=0.5, color='#140000', hatch="x")
plt.bar([p + width * 2 for p in pos], df['Mesos - static'], width, alpha=0.5, color='#000000', hatch=".")
plt.bar([p + width * 3 for p in pos], df['Mesos - dynamic'], width, alpha=0.5, color='#332200', hatch="o")
plt.bar([p + width * 4 for p in pos], df['YARN - static'], width, alpha=0.5, color='#1A1A1A', hatch="\\")
plt.bar([p + width * 5 for p in pos], df['YARN - dynamic'], width, alpha=0.5, color='#222211', hatch="*")


ax.set_ylabel('Makespan (s)', fontsize=15)
ax.set_title('Static Vs Dynamic allocation - 20 sec inter-arrival', fontsize=20)
ax.set_xticks([p + 3 * width for p in pos])
#ax.set_yticks(100)
ax.set_xticklabels(df['job_name'], fontsize=15)

#plt.xlim(min(pos) - width, max(pos) + width * 4)
# plt.ylim([0, max(df['standalone'] + df['mesos'] + df['yarn'])] )
plt.xlim(-0.1, 3)
#plt.ylim([0, 420])

fontP = FontProperties()
fontP.set_size('medium')

plt.legend(['Standalone - static', 'Standalone - dynamic', 'Mesos - static', 'Mesos - dynamic', 'YARN - static', 'YARN - dynamic'], loc='upper right', shadow=False, prop=fontP)
plt.grid()
plt.show()

"""
# Static Vs Dynamic allocation - 60 sec inter-arrival data
raw_data = {'job_name': ['Default', '24 Max Executors', '48 Max Executors'],
            'Standalone - static': [588, 456, 516],
            'Standalone - dynamic': [426, 390, 390],
            'Mesos - static': [522, 450, 504],
            'Mesos - dynamic': [504, 504, 492],
            'YARN - static': [1620, 408, 450],
            'YARN - dynamic': [474, 576, 504]}



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