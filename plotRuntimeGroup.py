# Author: Habib Sabiu
# Date: March 31, 2017
# Purpose: Script to plot nice graphs for comparing jobs running time
#          between running Spark applications on Standalone, Mesos or YARN
# Copyright: Any person can adopt this script for their specific need


import pandas as pd
import matplotlib.pyplot as plt

raw_data = {'job_name': ['job 1', 'job 2', 'job 3', 'job 4', 'job 5'],
            'standalone': [120, 222, 330, 474, 600],
            'mesos': [114, 444, 474, 462, 390],
            'yarn': [660, 900, 1200, 600, 1200]}
df = pd.DataFrame(raw_data, columns=['job_name', 'standalone', 'mesos', 'yarn'])

# Setting the positions and width for the bars
pos = list(range(len(df['standalone'])))
width = 0.25

# Plotting the bars
fig, ax = plt.subplots(figsize=(10, 5))

# Create a bar with standalone data,
# in position pos,
plt.bar(pos,
        # using df['standalone'] data,
        df['standalone'],
        # of width
        width,
        # with alpha 0.5
        alpha=0.5,
        # with color
        color='#4C2C10',
        # with hatch
        hatch="/",
        # with label the first value in job_name
        label=df['job_name'][0])

# Create a bar with mesos data,
# in position pos + some width buffer,
plt.bar([p + width for p in pos],
        # using df['meos'] data,
        df['mesos'],
        # of width
        width,
        # with alpha 0.5
        alpha=0.5,
        # with color
        color='#000000',
        # with hatch
        hatch=".",
        # with label the second value in job_name
        label=df['job_name'][1])

# Create a bar with yarn data,
# in position pos + some width buffer,
plt.bar([p + width * 2 for p in pos],
        # using df['yarn'] data,
        df['yarn'],
        # of width
        width,
        # with alpha 0.5
        alpha=0.5,
        # with color
        color='#140000',
        # with hatch
        hatch="x",
        # with label the third value in job_name
        label=df['job_name'][2])

# Set the y axis label
ax.set_ylabel('Running time (seconds)', fontsize=15)

# Set the chart's title
ax.set_title('Image Format Conversion', fontsize=20)

# Set the position of the x ticks
ax.set_xticks([p + 1.5 * width for p in pos])

# Set the labels for the x ticks
ax.set_xticklabels(df['job_name'])

# Setting the x-axis and y-axis limits
plt.xlim(min(pos) - width, max(pos) + width * 4)
# plt.ylim([0, max(df['standalone'] + df['mesos'] + df['yarn'])] )
plt.ylim([0, 1500])

# Adding the legend and showing the plot
plt.legend(['Standalone', 'Mesos', 'YARN'], loc='upper left')
plt.grid()
plt.show()
