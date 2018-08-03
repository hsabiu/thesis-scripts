# Date: March 09, 2018
# Author: Habib Sabiu
# Purpose:


import os
import time
import datetime
import numpy as np
import subprocess


spark_master = "spark://discus-p2irc-master:7077"

hdfs_input_base_dir = "hdfs://discus-p2irc-master:54310/user/hduser/cameradays/"
hdfs_output_base_dir = "hdfs://discus-p2irc-master:54310/user/hduser/outputs/"

command = ["hadoop", "fs", "-ls", hdfs_input_base_dir]

result = subprocess.check_output(command)

all_jobs = []

count = 0

for sub_dir in result.split("\n"):

    if sub_dir and "hdfs://" in sub_dir:
        
        job_name = sub_dir.split("/")[-1]

        all_jobs.append(["spark-submit", "--master", spark_master, "--py-files", "timelapse_image.py", "modified_flower_counter.py", hdfs_input_base_dir+job_name, hdfs_output_base_dir+"output_"+job_name, "job_"+job_name])


epoch_start_time = int(time.time())
local_start_time = datetime.datetime.fromtimestamp(epoch_start_time).strftime('%c')

logs_list = []
processes = []

logs_list.append("------------------------------------------------------------------")
logs_list.append("Experiment start time: " + str(epoch_start_time) + " => " + local_start_time)
logs_list.append("------------------------------------------------------------------")

for job in all_jobs[0:7]:

    p = subprocess.Popen(job)
    processes.append(p)

    epoch_current_time = int(time.time())
    local_current_time = datetime.datetime.fromtimestamp(epoch_current_time).strftime('%c')

    line = '{} {:13s} {} {} {} {}'.format("Submitted", job[-1], "on", str(epoch_current_time), "=>", local_current_time)
    logs_list.append(line)

    time.sleep(60)

exit_codes = [p.wait() for p in processes]

epoch_stop_time = int(time.time())
local_stop_time = datetime.datetime.fromtimestamp(epoch_stop_time).strftime('%c')

makespan = epoch_stop_time - epoch_start_time

logs_list.append("------------------------------------------------------------------")
logs_list.append("Experiment end time: " + str(epoch_stop_time) + " => " + local_stop_time)
logs_list.append("Experiment makespan: " + str(round(makespan, 3)) + " sec")
logs_list.append("------------------------------------------------------------------")


output_logs_file = "experiment_logs"
with open (output_logs_file, "w") as file_handle:
    for line in logs_list:
        file_handle.write("%s\n" % line)
    file_handle.close()

print("------------------------------------------------------------------")
print("SUCCESS: ALL JOBS ARE DONE...With exit codes: " + str(exit_codes))
print("------------------------------------------------------------------")

