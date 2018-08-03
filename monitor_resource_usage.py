# Date: July 19, 2018
# Author: Habib Sabiu
# Purpose: Script to monitor percentage of CPU and memory utilization of a system
#          every 1 second and log the data to a text file. Each line in the output
#          file contain timestamp, percentage of CPU utilization, and percentage
#          of memory utilization.

import time
import psutil
import platform
import threading
import subprocess

class IntervalRunner():

    def __init__(self, seconds):
        self.seconds = seconds

    def run(self):

        with open('system_util.csv', 'w') as f:

            print('Timestamp,CPU Util (%),Mem Util (%)', file=f)

            while True:

                percentage_cpu_util = psutil.cpu_percent(interval=1, percpu=False)
                percentage_cpu_util = '{:.2f}'.format(round(percentage_cpu_util, 2))
                percentage_mem_util = ''

                if platform.system() == 'Darwin':
                    mem_stats = psutil.virtual_memory()
                    mem_total = mem_stats.total / 1073741824
                    mem_available = mem_stats.available / 1073741824
                    mem_used = mem_total - mem_available
                    percentage_mem_util = '{:.2f}'.format(round((mem_used * 100) / mem_total, 2))
                elif platform.system() == 'Linux':
                    p = subprocess.Popen("free | grep Mem | awk '{print $3/$2 * 100.0}'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    stdout, stderr = p.communicate()
                    percentage_mem_util = '{:.2f}'.format(round(float(stdout.decode()), 2))

                print('{},{},{}'.format(time.ctime(), percentage_cpu_util, percentage_mem_util), file=f)

                f.flush()

                time.sleep(self.seconds)


if __name__ == '__main__':

    runner = IntervalRunner(1)
    runner.run()
