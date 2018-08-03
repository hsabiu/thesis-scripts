# Author: Habib Sabiu
# Date: March 31, 2017
# Purpose: Script to plot nice graphs for comparing jobs makespan between
#          running Spark applications on Standalone, Mesos or YARN
# Copyright: Any person can adopt this script for their specific need


import pandas as pd
import matplotlib.pyplot as plt

from matplotlib.font_manager import FontProperties
from matplotlib.ticker import LinearLocator

raw_data = {'job_name': ['YARN', 'Standalone', 'Mesos'], #x-axis
            'Static - Default': [7230, 6360, 4290], #y-axis
            'Dynamic - Default': [4560, 3510, 4020],
            'Static - 88 tasks': [7350, 4380,	3660],
            'Dynamic - 88 tasks': [3690, 3900, 3540]}
df = pd.DataFrame(raw_data, columns=['job_name', 'Static - Default', 'Dynamic - Default', 'Static - 88 tasks', 'Dynamic - 88 tasks'])

# Setting the positions and width for the bars
pos = list(range(len(df['Static - Default'])))
width = 0.22

# Plotting the bars
fig, ax = plt.subplots(figsize=(14, 8))

plt.bar(pos,
        # using df['standalone'] data,
        df['Static - Default'],
        # of width
        width,
        # with alpha 0.5
        alpha=0.7,
        # with color
        color='#4C2C10',
        # with hatch
        hatch="/")

plt.bar([p + width for p in pos],
        # using df['meos'] data,
        df['Dynamic - Default'],
        # of width
        width,
        # with alpha 0.5
        alpha=0.7,
        # with color
        color='#140000',
        # with hatch
        hatch="x")


plt.bar([p + width * 2 for p in pos],
        # using df['yarn'] data,
        df['Static - 88 tasks'],
        # of width
        width,
        # with alpha 0.5
        alpha=0.7,
        # with color
        color='#000000',
        # with hatch
        hatch=".")

plt.bar([p + width * 3 for p in pos],
        # using df['yarn'] data,
        df['Dynamic - 88 tasks'],
        # of width
        width,
        # with alpha 0.5
        alpha=0.9,
        # with color
        color='#332200',
        # with hatch
        hatch="o")


# Set the y axis label
ax.set_ylabel('Makespan (seconds)', fontsize=20)

# Set the chart's title
ax.set_title('Image Registration (Static Vs Dynamic)', fontsize=25)

# Set the position of the x ticks
ax.set_xticks([p + 2 * width for p in pos])

# Set the labels for the x ticks
ax.set_xticklabels(df['job_name'], fontsize=20)

# Setting the x-axis and y-axis limits
#plt.xlim(min(pos) - width, max(pos) + width * 4)
# plt.ylim([0, max(df['standalone'] + df['mesos'] + df['yarn'])] )
plt.xlim(-0.2, 2.9)
plt.ylim([0, 10000])
plt.yticks(fontsize=15)
# Set the y-ticks frequency to 10 evenly spaced values
ax.get_yaxis().set_major_locator(LinearLocator(numticks=11))


fontP = FontProperties()
fontP.set_size('x-large')

# Adding the legend and showing the plot
plt.legend(['Static resource allocation with default number of tasks', 'Dynamic resource allocation with default number of tasks', 'Static resource allocation with 88 number of tasks', 'Dynamic resource allocation with 88 number of tasks'], loc='upper left', shadow=False, prop=fontP)
#plt.grid()
plt.show()